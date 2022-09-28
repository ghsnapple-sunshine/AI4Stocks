import unittest

from ai4stocks.task.base_task import BaseTask


class Inner:
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


class TestBaseTask(unittest.TestCase):
    def test_Func1(self):
        task = BaseTask(obj=Inner(), method_name='Func1', args=('a', 'b'))
        var = task.run()
        assert var == 'print a b'

    def test_Func2_1(self):
        task = BaseTask(obj=Inner(), method_name='Func2', args=('a', 'b'))
        var = task.run()
        assert var == 'print a b  '

    def test_Func2_2(self):
        task = BaseTask(obj=Inner(), method_name='Func2', args=('a', 'b'), kwargs={'var3': 'c'})
        var = task.run()
        assert var == 'print a b c '

    def test_Func2_3(self):
        task = BaseTask(obj=Inner(), method_name='Func2', args=('a', 'b'), kwargs={'var4': 'd'})
        var = task.run()
        assert var == 'print a b  d'

    def test_Func2_4(self):
        task = BaseTask(obj=Inner(), method_name='Func2', args=('a', 'b'), kwargs={'var3': 'c', 'var4': 'd'})
        var = task.run()
        assert var == 'print a b c d'

    def test_Func2_5(self):
        task = BaseTask(obj=Inner(), method_name='Func2', args=('a', 'b'),
                        kwargs={'var3': 'c', 'var4': 'd', 'val5': 'e'})
        try:
            var = task.run()
            assert False
        except TypeError as e:
            assert True



if __name__ == '__main__':
    unittest.main()
