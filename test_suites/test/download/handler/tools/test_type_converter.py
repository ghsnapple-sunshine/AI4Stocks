from buffett.adapter.numpy import ndarray
from buffett.common.tools import tuple_to_array
from test import Tester


class TestTypeConverter(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_tuple_to_array(self):
        tup = (1, 2, 3)
        arr = tuple_to_array(tup)
        assert type(arr) == ndarray
        assert arr[0, 0] == 1
        assert arr[0, 1] == 2
        assert arr[0, 2] == 3
