from buffett.common.error.my import MyError


class OutOfRangeError(MyError, ValueError):
    def __init__(self, code: int):
        self._code = code

    @property
    def code(self) -> int:
        return self._code

    @property
    def msg(self) -> str:
        return f"{self._code} is out of range."

    __str__ = msg
