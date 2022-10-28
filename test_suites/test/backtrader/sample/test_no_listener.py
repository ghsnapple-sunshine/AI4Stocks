import unittest


class A:
    def __init__(self, v: int):
        self.v = v


class B:
    def __init__(self, a: A):
        self.a = a


class C:
    def __init__(self, l: list[A]):
        self.l = l


class D:
    def change(self, a: A):
        a.v += 1


class NoListenerTest(unittest.TestCase):
    def test_no_listener(self):
        a1, a2 = A(1), A(3)
        b = B(a1)
        c = C([a1, a2])
        D().change(a1)
        assert b.a.v == 2
        assert c.l[0].v == 2
        assert c.l[1].v == 3
