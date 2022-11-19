from typing import Type

from buffett.common.error import ParamTypeError
from buffett.common.logger.logger import Logger
from buffett.common.logger.type import LogType
from buffett.common.magic import empty_method, get_name


class LoggerBuilder:
    _NO = 0

    @staticmethod
    def build(cls: type):
        if not issubclass(cls, Logger):
            raise ParamTypeError("cls", Type[Logger])
        overrides_func = {}
        level = Logger.Level
        for att_name in dir(cls):
            if not callable(getattr(cls, att_name)) or att_name.startswith("_"):
                continue
            if LoggerBuilder._recognize(att_name) < level:
                overrides_func[att_name] = empty_method
        dyn_cls = type(f"{get_name(cls)}No{LoggerBuilder._NO}", (cls,), overrides_func)
        LoggerBuilder._NO += 1
        return dyn_cls

    @staticmethod
    def _recognize(att_name: str) -> LogType:
        """
        根据方法名识别方法的打印类型

        :param att_name:        属性名
        :return:
        """
        if att_name.startswith(str(LogType.DEBUG)):
            return LogType.DEBUG
        if att_name.startswith(str(LogType.INFO)):
            return LogType.INFO
        if att_name.startswith(str(LogType.WARNING)):
            return LogType.WARNING
        # 始终需要执行的均返回LogType.ERROR
        return LogType.ERROR
