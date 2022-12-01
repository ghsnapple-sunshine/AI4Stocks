from buffett.common.error.my import MyError
from buffett.common.magic import get_name


class PreStepError(MyError):
    def __init__(self, curr_step: type, pre_step: type):
        self._curr_step = get_name(curr_step)
        self._pre_step = get_name(pre_step)

    def code(self) -> int:
        return 0

    def msg(self) -> str:
        return f"{self._pre_step} should run before {self._curr_step}."

    __str__ = msg
