from buffett.adapter.logging import basicConfig, INFO, info, warning, error
from buffett.adapter.os import os
from buffett.common.logger.type import LogType

basicConfig(
    level=INFO,
    format="%(asctime)s.%(msecs)03d %(message)s",
    datefmt="## %Y-%m-%d %H:%M:%S",
)


class Logger:
    Level = LogType.INFO

    @classmethod
    def debug(cls, msg: str):
        info(f"\033[1;36m{msg}\033[0m")  # 避免打出引用包的方法

    @classmethod
    def info(cls, msg: str):
        info(f"\033[1;32m{msg}\033[0m")

    @classmethod
    def warning(cls, msg: str):
        warning(f"\033[1;33m{msg}\033[0m")

    @classmethod
    def error(cls, msg: str):
        error(f"\033[1;31m{msg}\033[0m")


class ExLogger(Logger):
    def __init__(self):
        self._pid = os.getpid()

    def debug(self, msg: str):
        Logger.debug(self._format(msg))

    def info(self, msg: str):
        Logger.info(self._format(msg))

    def warning(self, msg: str):
        Logger.warning(self._format(msg))

    def error(self, msg: str):
        Logger.error(self._format(msg))

    def _format(self, msg: str):
        return f"[{self._pid}] {msg}"


class ProgressLogger(ExLogger):
    def __init__(self, total: int):
        super(ProgressLogger, self).__init__()
        self._total = total
        self._curr = 1

    def debug_with_progress(self, msg: str, update: bool = False):
        Logger.debug(self._format_with_progress(msg))
        self._update(update)

    def info_with_progress(self, msg: str, update: bool = False):
        Logger.info(self._format_with_progress(msg))
        self._update(update)

    def warning_with_progress(self, msg: str, update: bool = False):
        Logger.warning(self._format_with_progress(msg))
        self._update(update)

    def error_with_progress(self, msg: str, update: bool = False):
        Logger.error(self._format_with_progress(msg))
        self._update(update)

    def _format_with_progress(self, msg: str):
        return f"[{self._pid}] ({self._curr}/{self._total}) {msg}"

    def _update(self, update: bool):
        if update:
            self._curr += 1


if __name__ == "__main__":

    def print_log(logger: Logger):
        logger.debug("debug")
        logger.info("info")
        logger.warning("warning")
        logger.error("error")

    [print_log(x) for x in [Logger(), ExLogger(), ProgressLogger(1)]]
