import datetime
import logging
from pathlib import Path
import time


def root_path():
    return Path(__file__).parent.parent.parent


def logging_config(
        log_format: str = '%(asctime)s %(name)-20s %(levelname)-10s %(message)s',
        log_date_fmt: str = '%Y-%m-%dT%H:%M:%S%z',
        use_gmt_time: bool = True,
        **kwargs
):
    """Wrapper for logging basic config with a default format and verbose level

    :param log_format: logging format
    :param log_date_fmt: logging date format
    :param use_gmt_time: use gm time for time
    :param kwargs:
    :return:
    """
    """
    Wrapper for logging basic config with a default format
    :param log_file_path: path to log file. if not defined then use stderr
    :param logging_level: The logging levels
    """
    logging.basicConfig(format=log_format, datefmt=log_date_fmt, **kwargs)
    if use_gmt_time:
        logging.Formatter.converter = time.gmtime

    verbose_level = 5
    logging.addLevelName(verbose_level, "VERBOSE")
    logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(verbose_level, msg, *args, **kwargs)
    logging.verbose = lambda msg, *args, **kwargs: logging.log(verbose_level, msg, *args, **kwargs)
