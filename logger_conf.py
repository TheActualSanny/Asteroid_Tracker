'''Module where the logger decorator is initialized.'''
import logging

formatter = logging.Formatter('%(asctime)s -  %(levelname)s - %(message)s')
streamhandler = logging.StreamHandler()
filehandler = logging.FileHandler('exceptions.log')
streamhandler.setFormatter(formatter)
filehandler.setFormatter(formatter)


def log_results(logger_name):
    def inner_wrapper(method):
        logger = logging.Logger(logger_name)
        logger.addHandler(streamhandler)
        logger.addHandler(filehandler)
        def wrapper(*args, **kwargs):
            try:
                return method(*args, **kwargs)
            except Exception as exc:
                logger.error(f'Caught an exception while calling the method {method.__name__}: {exc}')
        return wrapper
    return inner_wrapper
