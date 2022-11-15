from buffett.adapter.logging import basicConfig, INFO, info, warning, error
from buffett.common.logger.type import LogType

basicConfig(level=INFO,
            format='%(asctime)s.%(msecs)03d %(message)s',
            datefmt='## %Y-%m-%d %H:%M:%S')


class Logger:
    Level = LogType.INFO

    @classmethod
    def debug(cls, msg: str):
        info(msg)  # 避免打出引用包的方法

    @classmethod
    def info(cls, msg: str):
        info(msg)

    @classmethod
    def warning(cls, msg: str):
        warning(msg)

    @classmethod
    def error(cls, msg: str):
        error(msg)
