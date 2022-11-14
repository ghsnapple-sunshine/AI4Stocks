from abc import abstractmethod
from types import MappingProxyType
from typing import Any, Optional

from buffett.adapter import logging
from buffett.adapter.wellknown import format_exc
from buffett.common.pendelum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.constants.col.task import CLASS, MODULE, START_TIME
from buffett.constants.magic import get_module_name, get_class_name


class Task:
    def __init__(self,
                 wrapper: Wrapper,
                 args: tuple = tuple(),
                 kwargs: dict = MappingProxyType({}),
                 start_time: DateTime = None):
        self._wrapper = wrapper
        self._args = args
        self._kwargs = kwargs
        self._start_time = start_time if isinstance(start_time, DateTime) else DateTime.now()
        self._task_id = None

    def run(self) -> tuple[bool, Any, None, Optional[str]]:
        success = True
        err_msg = None
        res = None
        try:
            logging.info(f"---------------Start running {self._wrapper.func_name}---------------")
            """
            valid_args = isinstance(self._args, tuple)
            valid_kwargs = isinstance(self._kwargs, dict)
            if valid_args & valid_kwargs:
                res = self._wrapper.run(*self._args, **self._kwargs)
            elif valid_args:
                res = self._wrapper.run(*self._args)
            elif valid_kwargs:
                res = self._wrapper.run(**self._kwargs)
            else:
                res = self._wrapper.run()
            """
            res = self._wrapper.run(*self._args, **self._kwargs)
            logging.info(f"---------------End running {self._wrapper.func_name}---------------")
            new_task = self.get_subsequent_task(success=True)
        except Exception as e:
            logging.error(f"---------------Error occurred when running {self._wrapper.func_name}---------------")
            err_msg = format_exc()
            logging.error(err_msg)
            success = False
            new_task = self.get_subsequent_task(success=False)
        return success, res, new_task, err_msg

    @property
    def start_time(self) -> DateTime:
        return self._start_time

    @property
    def info(self) -> dict:
        return {CLASS: get_class_name(self),
                MODULE: get_module_name(self),
                START_TIME: self._start_time.format_YMDHms()}

    @property
    def task_id(self) -> int:
        return self._task_id

    @task_id.setter
    def task_id(self, task_id: int):
        """
        taskid只允许指定一次

        :param task_id:
        :return:
        """
        if self._task_id is None:
            self._task_id = task_id

    @abstractmethod
    def get_subsequent_task(self, success: bool):
        pass
