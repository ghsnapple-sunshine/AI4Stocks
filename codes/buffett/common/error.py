from abc import abstractmethod


class MyError(Exception):
    @abstractmethod
    def args(self) -> tuple[int, str]:
        pass


class InvalidAttrError(ValueError, MyError):
    def __init__(self, attr_name: str):
        self._name = attr_name

    def args(self) -> tuple[int, str]:
        return 0, 'param {0} is not a callable item.'.format(self._name)

