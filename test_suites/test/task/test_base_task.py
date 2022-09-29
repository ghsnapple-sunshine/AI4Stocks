import logging
import traceback
import unittest

from ai4stocks.task.base_task import BaseTask


class InnerA:
    def Func1(self,
              val1: str,
              val2: str):
        res = "print %s %s" % (val1, val2)
        return res

    def Func2(self,
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
    def test_Func1(self):
        task = BaseTask(obj=InnerA(), method_name='Func1', args=('a', 'b'))
        var = task.Run()[1]
        assert var == 'print a b'

    def test_Func2_1(self):
        task = BaseTask(obj=InnerA(), method_name='Func2', args=('a', 'b'))
        var = task.Run()[1]
        assert var == 'print a b  '

    def test_Func2_2(self):
        task = BaseTask(obj=InnerA(), method_name='Func2', args=('a', 'b'), kwargs={'var3': 'c'})
        var = task.Run()[1]
        assert var == 'print a b c '

    def test_Func2_3(self):
        task = BaseTask(obj=InnerA(), method_name='Func2', args=('a', 'b'), kwargs={'var4': 'd'})
        var = task.Run()[1]
        assert var == 'print a b  d'

    def test_Func2_4(self):
        task = BaseTask(obj=InnerA(), method_name='Func2', args=('a', 'b'), kwargs={'var3': 'c', 'var4': 'd'})
        var = task.Run()[1]
        assert var == 'print a b c d'

    def test_Func2_5(self):
        task = BaseTask(obj=InnerA(), method_name='Func2', args=('a', 'b'),
                        kwargs={'var3': 'c', 'var4': 'd', 'val5': 'e'})
        try:
            var = task.Run()[1]
            assert False
        except TypeError as e:
            assert True

    def test_catchError(self):
        try:
            task = BaseTask(
                obj=InnerB(),
                method_name='run')
            task.Run()
            assert False
        except ValueError as e:
            logging.error('\n' + traceback.format_exc())
            assert True


if __name__ == '__main__':
    unittest.main()
