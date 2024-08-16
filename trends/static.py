import time
import random
import functools
from typing import Callable, Any
from utils_ops.logs import Logger

# Categories
ALL: int  = 0
FINANCE: int = 7
DEFAULT_CAT_ID: int = 7

logger = Logger("Graph Ops-Static")

# Keywords

DEFAULT_KEYWORDS: list[str] = ['Apple', 'trending stocks', 'stock market', 'commodities tarding', 'forex trading', 'trending news', 'trending cryptos', 'futures']
KEYWORDS_MAP: dict[str, list[str]] = {
    'other_finance_1': ['Apple', 'market', 'stocks', 'options', 'performances'],
}

def retry(retries: int = 1, backoff_factor: int = 0, timeout: int = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to retry a function call with exponential backoff.

    Args:
        retries (int): Number of retries before giving up.
        backoff_factor (int): Factor by which the wait time increases (exponential backoff).
        timeout (int, optional): Timeout in seconds to limit the total retry duration. If None, no timeout is applied.

    Returns:
        Callable[..., Any]: A wrapper function that retries the decorated function.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 0
            while attempt <= retries:
                logger.log("info", f"Attempt {attempt} of {retries}")
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries:
                        logger.log("error", f"Attempt {attempt} of {retries} failed. Last error: {e}")
                        raise  # Raise the last exception after exhausting retries
                    attempt += 1
                    sleep_time = backoff_factor * (2 ** attempt)  # Exponential backoff
                    if timeout is not None:
                        start_time = time.time()
                        while time.time() - start_time < timeout:
                            logger.log("info", f"Retrying in {sleep_time} seconds...")
                            time.sleep(min(sleep_time, timeout - (time.time() - start_time)))
                    continue
        return wrapper
    return decorator

@retry(retries=3, backoff_factor=0, timeout=None)
def reload_kwds(kwds: list[str] = DEFAULT_KEYWORDS) -> None:
    """
    Reloads the keywords by randomly selecting and switching a sublist of them.

    Args:
        kwds (list[str], optional): The keywords to reload. Defaults to DEFAULT_KEYWORDS.

    Returns:
        list[str]: A list of reloaded keywords.

    Raises:
        ValueError: If there are less than two keywords to switch.
    """
    if len(kwds) < 2:
        raise ValueError("Need at least two keywords to perform this function")

    start = random.choice(kwds)
    end = random.choice(kwds)
    while start == end:
        end = random.choice(kwds)

    start_index = kwds.index(start)
    end_index = kwds.index(end)

    if start_index > end_index:
        start_index, end_index = end_index, start_index

    kwds = kwds[start_index:end_index + 1]

    if len(kwds) < 2:
        raise ValueError("Need at least two keywords to perform a switch")

    return kwds

@retry(retries=3, backoff_factor=0, timeout=None)
def update_category(category_id: int = DEFAULT_CAT_ID) -> None:
    pass

@retry(retries=3, backoff_factor=0, timeout=None)
def update_delta(delta: int = 2) -> None:
    pass

def get_next_key(map: dict[str, Any], iteration: int = 0) -> str:
    keys = list(map.keys())
    return keys[iteration]