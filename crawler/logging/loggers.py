import logging


def get_core_logger() -> logging.Logger:
    return logging.getLogger("core")


def get_custom_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
