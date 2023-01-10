import multiprocessing
from typing import Any, Callable


class MultiProcessTaskManager:
    def __init__(self, worker: Callable, args: list[Any], iterable_args: int = 1):
        self._worker = worker
        self._args = args
        self._iterable_args = iterable_args

    def run(self):
        total_num = len(self._args[0])
        chunk_num = min(max(total_num, 4) // 4, 8)
        paras = [
            tuple(
                [
                    args[x::chunk_num] if y < self._iterable_args else args
                    for y, args in enumerate(self._args)
                ]
            )
            for x in range(chunk_num)
        ]
        with multiprocessing.Pool() as workers:
            results = workers.map(self._worker, paras)
        return results
