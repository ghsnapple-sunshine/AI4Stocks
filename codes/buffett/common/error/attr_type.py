from buffett.common.error.my import MyError


class AttrTypeError(TypeError, MyError):
    def __init__(self, attr_name: str):
        self._name = attr_name

    def code(self) -> int:
        return 0

    def msg(self) -> str:
        return f"param {self._name} is not a callable item."

    __str__ = msg
