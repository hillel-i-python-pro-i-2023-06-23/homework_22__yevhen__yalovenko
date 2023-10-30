import logging

from crawler.config import LOGS_PATH


def init_logging(is_verbose: bool = False):
    file_handler = logging.FileHandler(LOGS_PATH)
    file_handler.setLevel(logging.DEBUG if is_verbose else logging.INFO)
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] "
        "[PROCESS %(process)d %(processName)s] "
        "[THREAD %(thread)d %(threadName)s] "
        "%(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().setLevel(logging.DEBUG if is_verbose else logging.INFO)
