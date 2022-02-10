import os
import logging
import logging.handlers


class singletonType(type):
    def __call__(cls, *args, **kwargs):
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super(singletonType, cls).__call__(*args, **kwargs)
            return cls.__instance


class basicLogger:
    logger_level = logging.DEBUG
    format = '[%(asctime)s][%(levelname)s][%(funcName)s] >> %(message)s'


class customLogger(basicLogger, metaclass=singletonType):
    __logger = None

    def __init__(self, name, logfile_path):
        self.__logger = logging.getLogger(name)
        self.__logger.propagate = True
        self.__logger.setLevel(self.logger_level)

        formatter = logging.Formatter(self.format, '%Y-%m-%d %H:%M:%S')
        os.makedirs(os.path.dirname(logfile_path), exist_ok=True)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=logfile_path,
            when='D',
            interval=7,
            encoding='utf-8',
            utc=False
        )
        file_handler.setFormatter(formatter)
        self.__logger.addHandler(file_handler)

    
    def getinstance(self):
        return self.__logger
