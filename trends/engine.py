import time
import json
import functools
from datetime import datetime, timedelta
from typing import Callable, Any

import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

from trends.static import DEFAULT_KEYWORDS, DEFAULT_CAT_ID, KEYWORDS_MAP, reload_kwds, get_next_key
from utils_ops.logs import Logger

class TrendEngine:
    """
    A class to manage and retrieve trends data from Google Trends.

    Attributes:
        pytrends (TrendReq): An instance of TrendReq from the pytrends library.
        keywords (list[str]): A list of keywords to search for.
        category_id (int): The category ID to search within.
        delta (int): The time delta to search for.
        timeout (int): The timeout for the search.
        retries (int): The number of retries for the search.
        backoff_factor (int): The backoff factor for retries.
        logger (Logger): An instance of Logger for logging.

    Methods:
    -------
        format_to_json(data: dict, format: str = 'records') -> dict:
            Converts the provided dictionaries into JSON format.

        retry(func: Callable[..., Any]) -> Callable[..., Any]:
            Decorator to retry a function call with exponential backoff.

        related_queries_payload() -> dict:
            Retrieves the related queries payload for the given keywords and category.

        get_trends() -> dict:
            Retrieves and processes trends data from Google Trends.
    """

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
        Retrieves and processes trends data from Google Trends.

        This function attempts to fetch trends data using the related_queries_payload method.
        If the request is rejected due to too many requests, it shrinks the keyword list and retries.
        If the request is invalid, it attempts to rotate the keyword list. If the attempts are exhausted, it raises a ValueError.

        Returns:
            dict: A dictionary containing the parsed trends data in JSON format.
        """
        self.result = None
        try:
            self.result = self.related_queries_payload()
        except TooManyRequestsError:
            self.logger.log("warning", "Too many requests. Server rejected the request.")
            self.logger.log("info", "shrinking keyword list before retry")
            try:
                # reduce kwds list
                self.keywords = reload_kwds()
                # recall related_queries_payload property
                self.result = self.related_queries_payload()
            except TooManyRequestsError:
                self.logger.log("error", "Too many requests. Server rejected the request.")
                # if related_queries_payload fails again, sets self.result to None
                self.result = None
        except ResponseError:
            self.logger.log("warning", "Invalid request. Server returned an error.")
            self.logger.log("info", "rotating keyword list in attempt to recover")
            success = False
            _list = [] # store explored keys
            iteration = 0 # init iteration
            while not success:
                _key = get_next_key(KEYWORDS_MAP, iteration) # get next key from the KEYWORDS_MAP dict
                if not _key in _list:
                    _list.append(_key)
                self.keywords = KEYWORDS_MAP[_key] # get the value of the key and set it as the new keyword list of the class
                print(f"keyword list: {self.keywords}")
                try:
                    self.result = self.related_queries_payload() # call related_queries_payload poperty
                    success = True
                    self.logger.log("info", "Success! recovered from invalid request")
                    break
                except ResponseError:
                    # if it still fails
                    success = False
                    # check if all keys have been explored
                    if len(_list) == len(KEYWORDS_MAP) and not success:
                        self.logger.log("error", "Unable to recover from invalid request.", ValueError("Exhausted keyword list"))
                        break
                    # if not, continue
                    self.logger.log("warning", "Failed to recover from invalid request. Retrying...")
                    iteration += 1 # update iteration
                    continue
            
        
        except Exception as e: # catches all other exceptions
            self.logger.log("error", "Uncaught exception", e)
            # simply sets self.result to None
            self.result = None
            raise

        parsed_result = {} # empty dict to store parsed data
        if self.result:
            for key, value in self.result.items():
                parsed_result[key] = self.format_to_json(value)

        return parsed_result
    

if __name__ == "__main__":
    engine = TrendEngine(retries=3, backoff_factor=2, delta=1)
    print(engine.keywords, engine.category_id)
    r = engine.get_trends()
    print(r)
