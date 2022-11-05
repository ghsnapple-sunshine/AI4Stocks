import logging


class Logger:
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3

    def __init__(self):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s.%(msecs)03d [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s',
                            datefmt='## %Y-%m-%d %H:%M:%S')

    @classmethod
    def debug(cls, msg: str):
        logging.info(msg)  # 避免打出引用包的方法

    @classmethod
    def info(cls, msg: str):
        logging.info(msg)

    @classmethod
    def warning(cls, msg: str):
        logging.warning(msg)

    @classmethod
    def error(cls, msg: str):
        logging.error(msg)
