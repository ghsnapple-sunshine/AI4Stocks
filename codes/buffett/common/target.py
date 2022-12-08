from __future__ import annotations

from typing import Optional


class Target:
    """
    标的，可以是股票(Stock), 指数(Index), 概念(Concept), 行业(Industry)

    """

    def __new__(cls, code: Optional[str] = None, name: Optional[str] = None):
        if code is None and name is None:
            return None
        return super(Target, cls).__new__(cls)

    def __init__(self, code: Optional[str] = None, name: Optional[str] = None):
        self._code = code
        self._name = name

    def with_code(self, code: str) -> Target:
        """
        条件设置code

        :param code:            标的代码
        :return:                Self
        """
        self._code = code
        return self

    def with_name(self, name: str) -> Target:
        """
        条件设置name

        :param name:            标的名称
        :return:                Self
        """
        self._name = name
        return self

    def clone(self) -> Target:
        """
        复制自身

        :return:                复制的对象
        """
        return Target(code=self._code, name=self._name)

    @property
    def code(self) -> Optional[str]:
        return self._code

    @property
    def name(self) -> Optional[str]:
        return self._name

    def __str__(self):
        return "{0} {1}".format(
            "" if self._code is None else self._code,
            "" if self._name is None else self._name,
        ).strip(" ")
