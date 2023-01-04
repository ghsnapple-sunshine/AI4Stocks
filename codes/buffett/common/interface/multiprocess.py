import multiprocessing
from typing import Any, Union, Callable

from buffett.adapter.numpy import ndarray
from buffett.adapter.pandas import DataFrame
from buffett.common.error import ParamTypeError


class MultiProcessTaskManager:
    def __init__(
        self,
        worker: Callable,
        args: list[Any],
    ):
        self._worker = worker
        self._args = args
        if not isinstance(args[0], (DataFrame, ndarray, list, tuple)):
            raise ParamTypeError("args[0]", Union[DataFrame, ndarray, list, tuple])

    def run(self):
        args0 = self._args[0]
        total_num = len(args0)
        chunk_num = min(max(total_num, 4) // 4, 8)
        paras = [
            tuple([i, args0[i::chunk_num]] + self._args[1:]) for i in range(chunk_num)
        ]
        with multiprocessing.Pool() as workers:
            results = workers.map(self._worker, paras)
        return results
