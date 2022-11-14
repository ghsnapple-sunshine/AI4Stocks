#
from typing import Type

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
    return obj.__name__ if hasattr(obj, '__name__') else 'unknown name'


def get_self(obj):
    return obj.__self__ if hasattr(obj, '__self__') else None


def load_class(module: str, cls: str) -> Type:
    return getattr(import_module(module), cls)


def empty_init(self) -> None:
    return


def empty_method(*args, **kwargs) -> None:
    return
