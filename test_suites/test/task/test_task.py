from buffett.adapter import logging
from buffett.adapter.wellknown import format_exc
from buffett.common.wrapper import Wrapper
from buffett.task.task import Task
from test import Tester


class InnerA:
    def func1(self, val1: str, val2: str):
        res = "print %s %s" % (val1, val2)
        return res

    def func2(self, var1: str, var2: str, var3: str = "", var4: str = ""):
        res = "print %s %s %s %s" % (var1, var2, var3, var4)
        return res


class InnerB:
    def run(self):
        raise ValueError("error")


class TestTask(Tester):
    def test_func1(self):
        task = Task(wrapper=Wrapper(InnerA().func1), args=("a", "b"))
        var = task.run()[1]
        assert var == "print a b"

    def test_func2_1(self):
        task = Task(wrapper=Wrapper(InnerA().func2), args=("a", "b"))
        var = task.run()[1]
        assert var == "print a b  "

    def test_func2_2(self):
        task = Task(
            wrapper=Wrapper(InnerA().func2), args=("a", "b"), kwargs={"var3": "c"}
        )
        var = task.run()[1]
        assert var == "print a b c "

    def test_func2_3(self):
        task = Task(
            wrapper=Wrapper(InnerA().func2), args=("a", "b"), kwargs={"var4": "d"}
        )
        var = task.run()[1]
        assert var == "print a b  d"

    def test_func2_4(self):
        task = Task(
            wrapper=Wrapper(InnerA().func2),
            args=("a", "b"),
            kwargs={"var3": "c", "var4": "d"},
        )
        var = task.run()[1]
        assert var == "print a b c d"

    def test_func2_5(self):
        task = Task(
            wrapper=Wrapper(InnerA().func2),
            args=("a", "b"),
            kwargs={"var3": "c", "var4": "d", "val5": "e"},
        )
        success = task.run()[1]
        assert not success

    def test_catch_error(self):
        try:
            task = Task(wrapper=Wrapper(InnerB().run))
            success = task.run()[0]
            assert not success
        except ValueError as e:
            logging.error("\n" + format_exc())
            assert False
