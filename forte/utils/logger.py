import logging

# logging default formats
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_FMT = "%(asctime)s.%(msecs)03d [%(levelname)-8s] %(message)s [%(name)s]"

# logging levels
LOG_LEVELS = {
    50: 'CRITICAL',
    40: 'ERROR',
    30: 'WARNING',
    20: 'INFO',
    10: 'DEBUG',
     0: 'NOTSET',
}

def log_config(level=logging.DEBUG,
               filename=None, filename_level=None,
               std=True, std_level=None,
               log_fmt=LOG_FMT, log_datefmt=LOG_DATEFMT):
    """
    Setup root logger and handlers for log file and STDOUT

    :param level: logging level (from logging library)
    :type level: int
    :param filename: name of log file
    :type filename: str
    :param filename_level: logging level for file log (same as level if None)
    :type filename_level: int
    :param std: if True, sys.stderr will show log messages
    :type std: boolean
    :param std_level: logging level for std log (same as level if None)
    :type std_level: int
    :param log_fmt: log output formatting
    :type log_fmt: str
    :param log_datefmt: log date-time output formatting
    :type log_datefmt: str
    """

    # check and reset log levels
    reset_level = False
    if not isinstance(level, int):
        level = logging.DEBUG
        reset_level = True

    reset_filename_level = False
    if not isinstance(filename_level, int):
        if filename_level is not None:
            reset_filename_level = True
        filename_level = level

    reset_std_level = False
    if not isinstance(std_level, int):
        if std_level is not None:
            reset_std_level = True
        std_level = level

    # setup root logger
    logger_root = logging.getLogger()
    logger_root.setLevel(level)

    # setup formatter
    formatter = logging.Formatter(fmt=log_fmt, datefmt=log_datefmt)

    # setup file handler
    if filename is not None:
        handler_file = logging.FileHandler(filename)
        handler_file.setLevel(filename_level)
        handler_file.setFormatter(formatter)
        logger_root.addHandler(handler_file)

    # setup std handler
    if std:
        handler_console = logging.StreamHandler()
        handler_console.setLevel(std_level)
        handler_console.setFormatter(formatter)
        logger_root.addHandler(handler_console)

    # initialization message
    logger_root.info("Logging started")

    # registers results from housekeeping checks
    if reset_level:
        logger_root.warn("Invalid logging level, reset to DEBUG")
    if reset_filename_level:
        logger_root.warn("Invalid file logging level, reset to {l}".format(l=LOG_LEVELS.get(level, level)))
    if reset_std_level:
        logger_root.warn("Invalid std logging level, reset to {l}".format(l=LOG_LEVELS.get(level, level)))
    if filename is None:
        logger_root.info("No log file will be saved")
    if not std:
        logger_root.info("No log entries shown on console")