from typing import Optional, Any

from buffett.adapter.pendulum import DateTime
from buffett.common.logger import Logger
from buffett.common.magic import get_name
from buffett.common.wrapper import Wrapper
from buffett.download import Para
from buffett.download.mysql import Operator


def create_task_no_subsequent(TaskCls: type) -> type:
    """
    在原Task基础上，创建使其不会生成新任务，不会捕获异常的SubClass

    :param TaskCls:     原TaskClass
    :return:            使其不会生成新任务，不会捕获异常的SubClass
    """
    task_sub_cls_name = get_name(TaskCls) + "SubClass"
    TaskSubCls = type(task_sub_cls_name, (TaskCls,), {"run": _run})  # 确保Class可以被import
    return TaskSubCls


def create_task_no_subsequent_n_shorter_span(
    TaskCls: type, HandlerCls: type, para: Para, func: str = "obtain_data"
) -> type:
    """
    在原Task基础上，创建使其不会生成新任务，不会捕获异常的SubClass

    :param TaskCls:     原TaskClass
    :param HandlerCls:  原HandlerClass
    :param func:        启动方法名
    :param para:        start, end
    :return:            使其不会生成新任务，不会捕获异常的SubClass
    """

    def _init(self, operator: Operator, start_time: DateTime = None):
        self._wrapper = Wrapper(getattr(HandlerCls(operator=operator), func))
        self._args = (para,) if func == "obtain_data" else tuple()
        self._kwargs = {}
        self._start_time = (
            start_time if isinstance(start_time, DateTime) else DateTime.now()
        )
        self._task_id = None

    task_sub_cls_name = get_name(TaskCls) + "SubClass"
    TaskSubCls = type(task_sub_cls_name, (TaskCls,), {"__init__": _init, "run": _run})
    return TaskSubCls


def _run(self) -> tuple[bool, Any, None, Optional[str]]:
    """
    不会生成新任务，不会捕获异常的run方法

    :param self:
    :return:
    """
    success = True
    err_msg = None
    new_task = None
    Logger.info(
        f"---------------Start running {self._wrapper.full_name}---------------"
    )
    res = self._wrapper.run(*self._args, **self._kwargs)
    Logger.info(f"---------------End running {self._wrapper.full_name}---------------")
    return success, res, new_task, err_msg
