import logging
from typing import Type

from buffett.common.pendelum import DateTime


class Task:
    def __init__(self,
                 attr,
                 args: tuple = None,
                 kwargs: dict = None,
                 start_time: DateTime = None):
        if callable(attr):
            self._attr = attr
        else:
            raise ValueError("'attr' is not a callable item.")
        self._attr = attr
        self._args = args
        self._kwargs = kwargs
        self._start_time = start_time if isinstance(start_time, DateTime) else DateTime.now()

    def run(self) -> tuple:
        logging.info("---------------Start running {0}---------------".format(self._get_name()))

        valid_args = isinstance(self._args, tuple)
        valid_kwargs = isinstance(self._kwargs, dict)

        if valid_args & valid_kwargs:
            res = self._attr(*self._args, **self._kwargs)
        elif valid_args:
            res = self._attr(*self._args)
        elif valid_kwargs:
            res = self._attr(**self._kwargs)
        else:
            res = self._attr()
        logging.info("---------------End running {0}---------------".format(self._get_name()))
        return True, res, None

    @property
    def start_time(self) -> DateTime:
        return self._start_time

    def _get_name(self) -> str:
        ls = str(self._attr)[1:-1].split(' ')
        if ls[0] == 'function':
            return ls[1]
        if ls[0] == 'bound':
            return ls[2]
        return 'unknown'

