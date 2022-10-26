from typing import Type


class Wrapper:
    def __init__(self, attr=None):
        if callable(attr):
            self._attr = attr
        else:
            self._attr = None

    def run(self, *args, **kwargs):
        if callable(self._attr):
            return self._attr(*args, **kwargs)


class WrapperFactory:
    @staticmethod
    def from_method_name(obj: object, method_name: str):
        return Wrapper(attr=getattr(obj, method_name))

    @staticmethod
    def from_method_ref(attr):
        return Wrapper(attr=attr)
