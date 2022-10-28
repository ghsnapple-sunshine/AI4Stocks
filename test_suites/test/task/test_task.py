import logging
import traceback
import unittest

from ai4stocks.task.task import Task


class InnerA:
    def func1(self,
              val1: str,
              val2: str):
        res = "print %s %s" % (val1, val2)
        return res

    def func2(self,
              var1: str,
              var2: str,
              var3: str = "",
              var4: str = ""):
        res = "print %s %s %s %s" % (var1, var2, var3, var4)
        return res


class InnerB:
    def run(self):
        raise ValueError('error')


class TestBaseTask(unittest.TestCase):
    def test_func1(self):
        task = Task(attr=InnerA().func1, args=('a', 'b'))
        var = task.run()[1]
        assert var == 'print a b'

    def test_func2_1(self):
        task = Task(attr=InnerA().func2, args=('a', 'b'))
        var = task.run()[1]
        assert var == 'print a b  '

    def test_func2_2(self):
        task = Task(attr=InnerA().func2, args=('a', 'b'), kwargs={'var3': 'c'})
        var = task.run()[1]
        assert var == 'print a b c '

    def test_func2_3(self):
        task = Task(attr=InnerA().func2, args=('a', 'b'), kwargs={'var4': 'd'})
        var = task.run()[1]
        assert var == 'print a b  d'

    def test_func2_4(self):
        task = Task(attr=InnerA().func2, args=('a', 'b'), kwargs={'var3': 'c', 'var4': 'd'})
        var = task.run()[1]
        assert var == 'print a b c d'

    def test_func2_5(self):
        task = Task(attr=InnerA().func2, args=('a', 'b'),
                    kwargs={'var3': 'c', 'var4': 'd', 'val5': 'e'})
        try:
            var = task.run()[1]
            assert False
        except TypeError as e:
            assert True

    def test_catch_error(self):
        try:
            task = Task(InnerB().run)
            task.run()
            assert False
        except ValueError as e:
            logging.error('\n' + traceback.format_exc())
            assert True


if __name__ == '__main__':
    unittest.main()
