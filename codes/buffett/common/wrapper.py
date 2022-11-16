from buffett.common.magic import get_name, get_self


class Wrapper:
    def __init__(self, attr=None):
        if callable(attr):
            self._attr = attr
        else:
            self._attr = None

    def run(self, *args, **kwargs):
        valid_args = isinstance(args, tuple)
        valid_kwargs = isinstance(kwargs, dict)

        if valid_args & valid_kwargs:
            res = self._attr(*args, **kwargs)
        elif valid_args:
            res = self._attr(*args)
        elif valid_kwargs:
            res = self._attr(**kwargs)
        else:
            res = self._attr()
        return res

    @property
    def caller(self):
        return get_self(self._attr)

    @property
    def func_name(self):
        return get_name(self._attr)


class WrapperFactory:
    @staticmethod
    def from_method_name(obj: object, method_name: str):
        return Wrapper(attr=getattr(obj, method_name))

    @staticmethod
    def from_method_ref(attr):
        return Wrapper(attr=attr)
