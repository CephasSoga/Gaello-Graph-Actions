# Can be called once in a script to load environment variables
# Then, use os.getenv as usual in your code
# or continue using getenv function from within here.

import  os
import  dotenv
from typing import Optional

currentDir = os.path.dirname(__file__)
rootDir = os.path.dirname(currentDir)

dotenv.load_dotenv(dotenv_path=f'{rootDir}/.env')

def getenv(key: str, default=None) -> Optional[str]:
    """
    Get the value of an environment variable.

    Args:
        key (str): The name of the environment variable.
        default (Optional[str]): The default value to return if the environment variable is not found.

    Returns:
        Optional[str]: The value of the environment variable, or the default value if the environment variable is not found.
    """
    return os.getenv(key=key, default=default)

