import logging

from buffett.common.logger.type import LogType


class Logger:
    Level = LogType.INFO

    def __init__(self):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s.%(msecs)03d %(message)s',
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


