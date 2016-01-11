import logging
from pydatacoll.utils.read_config import *


def get_logger(logger_name='main'):
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger
    log_file = os.path.join(app_dir.user_log_dir, '{}.log'.format(logger_name))
    if not os.path.exists(app_dir.user_log_dir):
        os.makedirs(app_dir.user_log_dir)
    formatter = logging.Formatter(config.get('LOG', 'format',
                                             fallback="%(asctime)s %(name)s [%(levelname)s] %(message)s"))
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.setLevel(config.get('LOG', 'level', fallback='ERROR'))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
