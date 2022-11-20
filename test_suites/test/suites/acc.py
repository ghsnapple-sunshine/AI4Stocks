from typing import Callable

from buffett.common.error import AttrTypeError
from buffett.common.magic import get_self, get_name, get_class, get_module_name
from buffett.common.magic.tools import get_func_params


class Accelerator:
    """
    Accelerator可以缓存func的运行结果，当(func, *args, **kwargs)检查结果一致时，

    """

    _cache = {}

    def __init__(self, func):
        self._func = None
        if not callable(func):
            raise AttrTypeError("func")
        restore = self.restore(func)
        self._func = func if restore is None else restore

    def mock(self):
        """
        得到一个Accelerator的mock方法

        :return:
        """
        return self._mock

    @staticmethod
    def restore(func):
        """
        从一个Accelerator的mock方法还原得到本来的方法
        如果还原失败，返回None

        :param func:
        :return:
        """
        caller = get_self(func)
        if isinstance(caller, Accelerator) and get_name(func) == "_mock":  # 避免套娃
            return caller._func
        return None

    def _mock(self, *args, **kwargs):
        si = self._get_signature(args, kwargs)
        if si in Accelerator._cache:
            return Accelerator._cache[si]
        result = self._func(*args, **kwargs)
        Accelerator._cache[si] = result
        return result

    """
    def _create_para(self, args: tuple, kwargs: dict) -> tuple[Callable, ...]:
        para = [self._func]
        para.extend(args)
        kwargs_keys = list(kwargs.keys())
        kwargs_keys.sort()
        kwargs = [kwargs[k] for k in kwargs_keys]
        para.extend(kwargs)
        para = tuple(para)
        return para
    """

    def _get_signature(self, args: tuple, kwargs: dict) -> tuple[Callable, ...]:
        params = get_func_params(self._func)
        for i in range(0, len(args)):
            params[i][1] = args[i]
        for i in range(len(args), len(params)):
            if params[i][0] in kwargs:
                params[i][1] = kwargs[params[i][0]]
        signature = [self._func]
        signature.extend([x[1] for x in params])
        signature = tuple(signature)
        return signature

    def __str__(self):
        cls_or_mdl = get_class(get_self(self._func))
        if cls_or_mdl is None:
            cls_or_mdl = get_module_name(get_self(self._func))
        return f"Accelerator of {cls_or_mdl}.{get_name(self._func)}"
