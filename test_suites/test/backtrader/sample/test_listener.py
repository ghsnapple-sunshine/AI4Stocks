from test import Tester


class InnerA:
    def __init__(self):
        self.a = None
        self.b = None
        self.c = None
        self.d = None

    def notify(self):
        self.a = 1
        self.d.add_listener(self.b)
        self.d.add_listener(self.c)


class InnerB:
    def __init__(self):
        self.b = None

    def notify(self):
        self.b = 2


class InnerC:
    def __init__(self):
        self.c = None

    def notify(self):
        self.c = 3


class InnerD:
    def __init__(self):
        self.listeners = []

    def add_listener(self, obj):
        self.listeners.append(obj)

    def notify(self):
        for li in self.listeners:
            li.notify()


class ListenerTest(Tester):
    def test_add_listener(self):
        a, b, c, d = InnerA(), InnerB(), InnerC(), InnerD()
        a.b = b
        a.c = c
        a.d = d
        d.add_listener(a)
        d.notify()
        assert a.a == 1
        assert b.b == 2
        assert c.c == 3
