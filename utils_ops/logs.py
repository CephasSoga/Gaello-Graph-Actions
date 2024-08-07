import logging
from typing import Any
from pathlib import Path

from utils_ops.paths import constructPath
from utils_ops.envHandler import getenv

DEFAULT_PATH = "C:\Temp\Gaello X"
LOGGING_PATH = getenv("LOGGING_PATH", DEFAULT_PATH)

class Logger(object):
    def __init__(self, name: str = None):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Ensure handlers are not duplicated
        if not self.logger.hasHandlers():
            self._add_console_handler()
            self._add_file_handler()

    def _add_console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)

    def _add_file_handler(self):
        log_dir = constructPath(Path(LOGGING_PATH), 'logs')
        log_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
        log_file_path = log_dir / f"{self.name}.log"
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger
    
    def log(self, level: str, message: str, error: Any = None, params: Any = None):
        if error:
            message = f"{message} | Error: {error}"
        if params:
            message = f"{message} | Params: {params}"

        log_method = getattr(self.logger, level.lower(), None)
        if callable(log_method):
            log_method(f"{message}\n\n")
        else:
            self.logger.error(f"Invalid log level: {level}. Message: {message}")



