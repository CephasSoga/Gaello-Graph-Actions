import time
import json
import functools
from datetime import datetime, timedelta
from typing import Callable, Any

import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

from trends.static import DEFAULT_KEYWORDS, DEFAULT_CAT_ID
from utils_ops.logs import Logger

class TrendEngine:

    def __init__(self, keywords: list[str] = DEFAULT_KEYWORDS, category_id: int = DEFAULT_CAT_ID, delta: int = 2, hl: str = 'en-US', geo: str = None, timeout: int = None, engine_timeout: tuple[int, int] = None,  proxies: str = None, retries: int = 3, backoff_factor: int = 1) -> None:
        """
        Initializes a TrendEngine object with the specified parameters.

        Args:
            keywords (list[str], optional): A list of keywords to search for. Defaults to DEFAULT_KEYWORDS.
            category_id (int, optional): The category ID to search within. Defaults to DEFAULT_CAT_ID.
            delta (int, optional): The time delta to search for. Defaults to 2.
            hl (str, optional): The language code for the search. Defaults to 'en-US'.
            geo (str, optional): The geographic location for the search. Defaults to None.
            timeout (int, optional): The timeout for the search. Defaults to None.
            engine_timeout (tuple[int, int], optional): The timeout for the engine. Defaults to (2, 5).
            proxies (str, optional): The proxies to use for the search. Defaults to None.
            retries (int, optional): The number of retries for the search. Defaults to 3.
            backoff_factor (int, optional): The backoff factor for retries. Defaults to 1.

        Returns:
            None
        """
        self.pytrends = TrendReq(
            hl=hl,
            geo=geo or '',
            timeout=engine_timeout or (2, 5),
            proxies=proxies or '',
        )
        self.keywords = keywords
        self.category_id = category_id
        self.delta = delta
        self.timeout = timeout
        self.retries = retries
        self.backoff_factor = backoff_factor

        self.logger = Logger("TrendEngine")

    @staticmethod
    def format_to_json(data: dict, format: str = 'records'):
        # Create DataFrames from the provided dictionaries
        futures_top_df = pd.DataFrame(data['top'])
        futures_rising_df = pd.DataFrame(data['rising'])
        
        # Convert DataFrames to JSON
        futures_top_json = futures_top_df.to_json(orient=format)
        futures_rising_json = futures_rising_df.to_json(orient=format)
        
        # Create a combined JSON structure
        formatted_json = {
            'top': json.loads(futures_top_json),
            'rising': json.loads(futures_rising_json)
        }
        
        return formatted_json

    def retry(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Decorator to retry a function call with exponential backoff.

        Args:
            func (Callable[..., Any]): The function to be retried.

        Returns:
            Callable[..., Any]: A wrapper function that retries the decorated function.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 0
            while attempt <= self.retries:
                try:
                    return func(*args, **kwargs)
                except TooManyRequestsError:
                    self.logger.log("warning", f"Attempt {attempt + 1} failed.")
                    if attempt == self.retries:
                        raise  # Raise the last exception after exhausting retries
                    attempt += 1
                    sleep_time = self.backoff_factor * (2 ** attempt)  # Exponential backoff
                    if self.timeout is not None:
                        start_time = time.time()
                        while time.time() - start_time < self.timeout:
                            time.sleep(min(sleep_time, self.timeout - (time.time() - start_time)))
                    else:
                        time.sleep(sleep_time)
                    continue
        return wrapper

    @property
    def related_queries_payload(self) -> Callable[..., Any]:
        """
        Returns a callable that retries the related_queries_payload method with retry logic.
        """
        return self.retry(self._related_queries_payload)

    def _related_queries_payload(self) -> dict:
        """
        Retrieves the related queries payload for the given keywords and category.

        This method calculates the start and end dates based on the current date and the delta value.
        It then builds the payload with the specified keywords, category, and timeframe.
        Finally, it calls the `related_queries` method of the `pytrends` object and returns the result.

        Returns:
            dict: The related queries payload as a dictionary.
        """
        end_date = datetime.today().strftime('%Y-%m-%d')
        start_date = (datetime.today() - timedelta(days=self.delta)).strftime('%Y-%m-%d')
        # Build the payload with the timeframe from 2 days ago to today
        self.pytrends.build_payload(
            kw_list=self.keywords,
            cat=self.category_id,
            timeframe=f'{start_date} {end_date}'
        )
        return self.pytrends.related_queries()

    def get_trends(self) -> dict:
        """
        Retrieves the trends related to the keywords and category specified in the object.

        This method attempts to retrieve the trends by calling the `related_queries_payload` method.
        If a `TooManyRequestsError` is raised, the method logs an error message and sets `self.result` to None.
        If a `ResponseError` is raised, the method logs an error message, sets `self.result` to None,
        and raises a `ValueError` with the message "Invalid request".

        If `self.result` is not None after the attempt to retrieve the trends, the method iterates over the
        items in `self.result` and formats each value using the `format_to_json` method. The resulting
        dictionary is stored in `parsed_result`.

        Returns:
            dict: A dictionary containing the parsed trends data.
        """
        self.result = None
        try:
            self.result = self.related_queries_payload()
        except TooManyRequestsError:
            self.logger.log("error", "Too many requests. Server rejected the request.")
            self.result = None
        except ResponseError:
            self.logger.log("error", "Invalid request. Server returned an error.")
            self.result = None
            raise ValueError("Invalid request")

        parsed_result = {}
        if self.result:
            for key, value in self.result.items():
                parsed_result[key] = self.format_to_json(value)

        return parsed_result
    

if __name__ == "__main__":
    engine = TrendEngine(retries=3, backoff_factor=2)
    r = engine.get_trends()
    print(r)
