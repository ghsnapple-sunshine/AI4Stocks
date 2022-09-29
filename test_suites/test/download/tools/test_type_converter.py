import unittest

from ai4stocks.tools.tools import Tuple2Arr
from numpy import ndarray


class TestTypeConverter(unittest.TestCase):
    def test_tuple_to_array(self):
        tup = 1, 2, 3
        arr = Tuple2Arr(tup)
        assert type(arr) == ndarray
        assert arr[0, 0] == 1
        assert arr[0, 1] == 2
        assert arr[0, 2] == 3


if __name__ == '__main__':
    unittest.main()
