from buffett.common.error.my import MyError


class OutOfRangeError(MyError, ValueError):
    def __init__(self, code: int):
        self._code = code

    def code(self) -> int:
        return self._code

    def msg(self) -> str:
        return f"{self._code} is out of range."
