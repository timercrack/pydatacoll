import logging
import threading
import platform

initLock = threading.Lock()
rootLoggerInitialized = False

log_format = "%(asctime)s %(name)s [%(levelname)s] %(message)s"
level = logging.DEBUG
file_log = None  # File name
console_log = True
# sys_str = platform.system()
# if sys_str == "Windows":
#     file_log = u"d:\pydatacoll.log"
# elif sys_str == "Linux":
#     file_log = u"/var/log/pydatacoll.log"
#     console_log = False


def init_handler(handler):
    handler.setFormatter(Formatter(log_format))


def init_logger(logger):
    logger.setLevel(level)

    if file_log is not None:
        fileHandler = logging.FileHandler(file_log)
        init_handler(fileHandler)
        logger.addHandler(fileHandler)

    if console_log:
        consoleHandler = logging.StreamHandler()
        init_handler(consoleHandler)
        logger.addHandler(consoleHandler)


def initialize():
    global rootLoggerInitialized
    with initLock:
        if not rootLoggerInitialized:
            init_logger(logging.getLogger())
            rootLoggerInitialized = True


def getLogger(name=None):
    initialize()
    return logging.getLogger(name)


# This formatter provides a way to hook in formatTime.
class Formatter(logging.Formatter):
    DATETIME_HOOK = None

    def formatTime(self, record, datefmt=None):
        newDateTime = None

        if Formatter.DATETIME_HOOK is not None:
            newDateTime = Formatter.DATETIME_HOOK()

        if newDateTime is None:
            ret = logging.Formatter.formatTime(self, record, datefmt)
        else:
            ret = str(newDateTime)
        return ret
