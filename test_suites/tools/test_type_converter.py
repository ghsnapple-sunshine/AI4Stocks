import unittest

from ai4stocks.tools.type_converter import TypeConverter
from numpy import ndarray


class TestTypeConverter(unittest.TestCase):
    def test_Tuple2Array(self):
        tup = 1, 2, 3
        arr = TypeConverter.Tuple2Arr(tup)
        assert type(arr) == ndarray
        assert arr[0, 0] == 1
        assert arr[0, 1] == 2
        assert arr[0, 2] == 3


if __name__ == '__main__':
    unittest.main()
