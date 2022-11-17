from buffett.common.error.my import MyError


class ClassParamError(MyError):
    def __init__(self, param_name: str, param_type: type):
        self._name = param_name
        self._param_type = param_type

    def code(self) -> int:
        return 1

    def msg(self) -> str:
        return f"param {self._name} is not a {self._param_type} item."

    __str__ = msg
