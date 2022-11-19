from typing import Callable

from buffett.common.error import AttrTypeError
from buffett.common.magic import get_self, get_name, get_class, get_module_name


class Accelerator:
    """
    Accelerator可以缓存func的运行结果，当(func, *args, **kwargs)检查结果一致时，

    """

    _cache = {}

    def __init__(self, func):
        self._func = None
        if not callable(func):
            raise AttrTypeError("func")
        caller = get_self(func)
        if isinstance(caller, Accelerator) and get_name(func) == "_mock":  # 避免套娃
            self._func = caller._func
        else:
            self._func = func

    def mock(self):
        return self._mock

    def _mock(self, *args, **kwargs):
        para = self._create_para(args, kwargs)
        if para in Accelerator._cache:
            return Accelerator._cache[para]
        result = self._func(*args, **kwargs)
        Accelerator._cache[para] = result
        return result

    def _create_para(self, args: tuple, kwargs: dict) -> tuple[Callable, ...]:
        para = [self._func]
        para.extend(args)
        kwargs_keys = list(kwargs.keys())
        kwargs_keys.sort()
        kwargs = [kwargs[k] for k in kwargs_keys]
        para.extend(kwargs)
        para = tuple(para)
        return para

    def __str__(self):
        cls_or_mdl = get_class(get_self(self._func))
        if cls_or_mdl is None:
            cls_or_mdl = get_module_name(get_self(self._func))
        return f"Accelerator of {cls_or_mdl}.{get_name(self._func)}"
