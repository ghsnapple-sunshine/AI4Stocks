from abc import abstractmethod


class MyError(Exception):
    @abstractmethod
    def code(self) -> int:
        pass

    @abstractmethod
    def msg(self) -> str:
        pass

    __str__ = msg
