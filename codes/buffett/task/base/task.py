from abc import abstractmethod
from types import MappingProxyType
from typing import Any, Optional

from buffett.adapter.wellknown import format_exc
from buffett.common.constants.col.task import CLASS, MODULE, START_TIME
from buffett.common.logger import ExLogger, LoggerBuilder
from buffett.common.magic import get_module_name, get_class_name
from buffett.common.pendulum import DateTime
from buffett.common.wrapper import Wrapper


class Task:
    def __init__(
        self,
        wrapper: Wrapper,
        args: tuple = tuple(),
        kwargs: dict = MappingProxyType({}),
        start_time: DateTime = None,
    ):
        self._wrapper = wrapper
        self._args = args
        self._kwargs = kwargs
        self._start_time = (
            start_time if isinstance(start_time, DateTime) else DateTime.now()
        )
        self._task_id = None
        self._logger = LoggerBuilder.build(TaskLogger)(wrapper)

    def run(self) -> tuple[bool, Any, None, Optional[str]]:
        success = True
        err_msg = None
        res = None
        try:
            self._logger.info_start()
            res = self._wrapper.run(*self._args, **self._kwargs)
            self._logger.info_end()
            new_task = self.get_subsequent_task(success=True)
        except Exception:
            self._logger.error_end(format_exc())
            success = False
            new_task = self.get_subsequent_task(success=False)
        return success, res, new_task, err_msg

    @property
    def start_time(self) -> DateTime:
        return self._start_time

    @property
    def info(self) -> dict:
        return {
            CLASS: get_class_name(self),
            MODULE: get_module_name(self),
            START_TIME: self._start_time.format_YMDHms(),
        }

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


class TaskLogger(ExLogger):
    _SEP = "-" * 10

    def __init__(self, wrapper):
        super(TaskLogger, self).__init__()
        self._name = wrapper.full_name

    def info_start(self):
        self.info(f"{self._SEP}Start running {self._name}{self._SEP}")

    def info_end(self):
        self.info(f"{self._SEP}End running {self._name}{self._SEP}")

    def error_end(self, msg: str):
        self.error(f"{self._SEP}Error occurred when running {self._name}{self._SEP}")
        self.error(msg)
