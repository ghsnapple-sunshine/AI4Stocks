from __future__ import annotations

from queue import Queue
from typing import Iterable, Any, Optional

from buffett.adapter.threading import Thread
from buffett.adapter.wellknown import format_exc
from buffett.common.wrapper import Wrapper


class Producer(Thread):
    """
    生产者
    """

    def __init__(
        self,
        cache: Queue,
        runtime: Runtime,
        wrapper: Wrapper,
        args_map: Iterable[Any],
        task_num: int,
    ):
        super(Producer, self).__init__()
        self._cache = cache
        self._runtime = runtime
        self._wrapper = wrapper
        self._args_map = args_map
        self._task_num = task_num
        self.run = self._run_without_para if args_map is None else self._run_with_para

    def _run_with_para(self):
        try:
            for args in self._args_map:
                # 如果生产前发现消费者抛出了一个异常，则停止生产
                if self._runtime.err is not None:
                    return
                data = self._wrapper.run(args)
                # 如果生产后缓存已满，则检测消费者是否有异常。如果没有，则继续等待；如果有，退出生产。
                while self._cache.full():
                    if self._runtime.err is not None:
                        return
                self._cache.put_nowait(data)
        except Exception as e:
            self._runtime.err = format_exc()
            raise e

    def _run_without_para(self):
        try:
            for i in range(0, self._task_num):
                # 如果生产前发现消费者抛出了一个异常，则停止生产
                if self._runtime.err is not None:
                    return
                data = self._wrapper.run()
                # 如果生产后缓存已满，则检测消费者是否有异常。如果没有，则继续等待；如果有，退出生产。
                while self._cache.full():
                    if self._runtime.err is not None:
                        return
                # 如果缓存内仍然有空间，则完成这次生产。
                self._cache.put_nowait(data)
        except Exception as e:
            self._runtime.err = format_exc()
            raise e


class Consumer(Thread):
    """
    消费者
    """

    def __init__(self, queue: Queue, runtime: Runtime, wrapper: Wrapper, task_num: int):
        super(Consumer, self).__init__()
        self._queue = queue
        self._runtime = runtime
        self._wrapper = wrapper
        self._task_num = task_num

    def run(self):
        try:
            for t in range(0, self._task_num):
                while self._queue.empty():
                    # 如果消费后缓存已空，则检测生产者是否有异常。如果没有，则继续等待；如果有，退出消费。
                    if self._runtime.err is not None:
                        return
                # 如果缓存内仍然有数据，则继续消费直至把缓存清空。
                data = self._queue.get_nowait()
                self._wrapper.run(data)
        except Exception as e:
            self._runtime.err = format_exc()
            raise e


class ProducerConsumer:
    def __init__(
        self,
        producer: Wrapper,
        consumer: Wrapper,
        args_map: Optional[Iterable[Any]],
        queue_size: int,
        task_num: int,
    ):
        # 初始化
        self._queue = Queue(maxsize=queue_size)
        self._runtime = Runtime()
        self._producer = Producer(
            cache=self._queue,
            runtime=self._runtime,
            wrapper=producer,
            args_map=args_map,
            task_num=task_num,
        )
        self._consumer = Consumer(
            queue=self._queue,
            runtime=self._runtime,
            wrapper=consumer,
            task_num=task_num,
        )

    def run(self):
        self._consumer.start()
        self._producer.start()
        self._consumer.join()
        self._producer.join()
        if self._runtime.err is not None:
            raise RuntimeError(self._runtime.err)


class Runtime:
    err = None
    producer_end = False
    consumer_end = False
