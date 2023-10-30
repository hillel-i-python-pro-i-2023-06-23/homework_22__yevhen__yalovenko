import pathlib
from typing import Final


ROOT_DIR: Final[pathlib.Path] = pathlib.Path(__file__).parents[1]
DB_PATH: Final[pathlib.Path] = ROOT_DIR.joinpath("db", "crawler.db")
LOGS_PATH: Final[pathlib.Path] = ROOT_DIR.joinpath("logs", "logs.log")
URLS = [
    "https://example.com/",
    "https://www.djangoproject.com/",
    "https://pypi.org/",
]
