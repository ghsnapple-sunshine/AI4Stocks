from abc import abstractmethod
from typing import Type


class MyError(Exception):
    @abstractmethod
    def code(self) -> int:
        pass

    @abstractmethod
    def msg(self) -> str:
        pass

    __str__ = msg


class AttrTypeError(TypeError, MyError):
    def __init__(self, attr_name: str):
        self._name = attr_name

    def code(self) -> int:
        return 0

    def msg(self) -> str:
        return f'param {self._name} is not a callable item.'

    __str__ = msg


class ParamTypeError(TypeError, MyError):
    def __init__(self, param_name: str, param_type: Type):
        self._name = param_name
        self._param_type = param_type

    def code(self) -> int:
        return 1

    def msg(self) -> str:
        return f'param {self._name} is not a {self._param_type} item.'

    __str__ = msg


class DateSpanLogicError(MyError):
    def code(self) -> int:
        return 0

    def msg(self) -> str:
        return
