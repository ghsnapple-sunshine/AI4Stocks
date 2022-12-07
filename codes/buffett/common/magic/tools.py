#
from types import FunctionType
from typing import Type, Any, Optional

from buffett.adapter.importlib import import_module


# MODULE = '__module__'
# CLASS = '__class__'
# SELF = '__self__'
# NAME = '__name__'
# NEW = '__new__'
# INIT = '__init__'


#
def get_module_name(obj):
    return obj.__module__


def get_class(obj):
    return obj.__class__


def get_class_name(obj):
    return obj.__class__.__name__


def set_class(obj, value: Type):
    obj.__class__ = value


def get_name(obj):
    return obj.__name__ if hasattr(obj, "__name__") else "unknown name"


def get_self(obj):
    return obj.__self__ if hasattr(obj, "__self__") else None


def load_class(module: str, cls: str) -> Optional[Type]:
    try:
        return getattr(import_module(module), cls)
    except AttributeError:
        return None
    except ImportError:
        return None


def empty_init(self) -> None:
    return


def empty_method(*args, **kwargs) -> None:
    return


def get_attr_safe(obj, att_name) -> Any:
    return getattr(obj, att_name) if hasattr(obj, att_name) else None


def get_func_params(func) -> list[list[str, Any]]:
    """
    获取函数的参数及默认值

    :param func:
    :return:
    """
    var_names = func.__code__.co_varnames
    names_count = len(var_names)
    var_defaults = func.__defaults__
    defaults_count = 0 if var_defaults is None else len(var_defaults)
    results = [
        [
            var_names[i],
            None
            if i - (names_count - defaults_count) < 0
            else var_defaults[i - (names_count - defaults_count)],
        ]
        for i in range(0, names_count)
    ]
    if isinstance(func, FunctionType):
        return results
    return results[1 : len(results)]  # 如果不是functionType则会自动忽略第一个参数


def get_func_full_name(func):
    caller = get_self(func)
    cls_or_mdl = get_module_name(func) if caller is None else get_class_name(caller)
    return f"{cls_or_mdl}.{get_name(func)}"
