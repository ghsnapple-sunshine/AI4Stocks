from __future__ import annotations

from typing import Optional, Union

from buffett.adapter.enum import Enum


class CodeType(Enum):
    CODE6 = 1
    CODE8 = 2
    CODE9 = 3


class Code(str):
    def to_code6(self) -> str:
        return self

    def to_code9(self) -> str:
        if self[0] == "6":
            return "sh." + self
        elif self[0] == "0":
            return "sz." + self
        elif self[0] == "3":
            return "sz." + self
        raise NotImplemented

    def to_code8(self) -> str:
        if self[0] == "6":
            return "sh" + self
        elif self[0] == "0":
            return "sz" + self
        elif self[0] == "3":
            return "sz" + self
        raise NotImplemented


class Stock:
    def __new__(cls, code: Union[Code, str, None] = None, name: Optional[str] = None):
        if code is None and name is None:
            return None
        return super(Stock, cls).__new__(cls)

    def __init__(self, code: Union[Code, str, None] = None, name: Optional[str] = None):
        if type(code) == str:  # 使用isinstance则str, Code均为True
            code = Code(code)
        self._code = code
        self._name = name

    def with_code(self, code: Code) -> Stock:
        """
        条件设置code

        :param code:            股票代码
        :return:                Self
        """
        self._code = code
        return self

    def with_name(self, name: str) -> Stock:
        """
        条件设置name

        :param name:            股票名称
        :return:                Self
        """
        self._name = name
        return self

    def clone(self) -> Stock:
        """
        复制自身

        :return:                复制的对象
        """
        return Stock(code=self._code, name=self._name)

    @property
    def code(self) -> Optional[Code]:
        return self._code

    @property
    def name(self) -> Optional[str]:
        return self._name
