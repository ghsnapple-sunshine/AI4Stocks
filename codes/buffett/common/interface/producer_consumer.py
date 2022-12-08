from __future__ import annotations

from queue import Queue
from typing import Iterable, Any, Optional

from buffett.adapter.threading import Thread, Event
from buffett.adapter.wellknown import format_exc
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.wrapper import Wrapper

PRODUCER, CONSUMER = "Producer", "Consumer"


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
        if args_map is None:
            self._args_map = range(0, task_num)
            self._run = self._run_without_para
        else:
            self._args_map = args_map
            self._run = self._run_with_para
        self._logger = LoggerBuilder.build(ProducerConsumerLogger)(PRODUCER)
        self._logger.debug_tasknum(task_num)

    def _run_with_para(self, args):
        return self._wrapper.run(args)

    def _run_without_para(self, it):
        return self._wrapper.run()

    def run(self):
        """
        Producer线程的退出条件：
        1. 生产任务执行完
        2. Consumer出现异常

        :return:
        """
        # 打印开始
        self._logger.debug_start()
        try:
            for args in self._args_map:
                # 如果生产前发现消费者抛出了一个异常，则停止生产
                if self._runtime.err_msg is not None:
                    self._logger.debug_end()
                    return
                self._wait_myself()
                self._execute(args)
                self._release_consumer()
            self._logger.debug_end(False)
        except Exception as e:
            # 如果不是由于消费者异常引发的连带异常（如：队列已满 & 通知生产）
            if self._runtime.err is None:
                self._runtime.err = e
                self._runtime.err_msg = format_exc()
                self._release_consumer()
                self._logger.debug_end(True)
            else:
                self._logger.debug_end(False)

    def _wait_myself(self):
        # 如果队列为满，则阻塞自己，等待恢复。
        if self._cache.full():
            self._logger.debug_wait()
            self._runtime.event_not_full.clear()
            self._runtime.event_not_full.wait()

    def _execute(self, args):
        # 生产
        data = self._run(args)
        self._cache.put_nowait(data)
        # self._logger.debug_execute(data)

    def _release_consumer(self):
        # 如果有需要，恢复阻塞中的消费者
        if not self._runtime.event_not_empty.is_set():
            self._runtime.event_not_empty.set()
            self._logger.debug_release()


class Consumer(Thread):
    """
    消费者
    """

    def __init__(
        self,
        queue: Queue,
        runtime: Runtime,
        wrapper: Wrapper,
        task_num: int,
    ):
        super(Consumer, self).__init__()
        self._queue = queue
        self._runtime = runtime
        self._wrapper = wrapper
        self._task_num = task_num
        self._logger = LoggerBuilder.build(ProducerConsumerLogger)(CONSUMER)
        self._logger.debug_tasknum(task_num)

    def run(self):
        """
        Consumer线程的退出条件：
        1. 消费任务执行完
        2. Producer出现异常且已清空queue

        :return:
        """
        self._logger.debug_start()
        try:
            for t in range(0, self._task_num):
                """
                2022/12/8 修复：
                    当Producer运行过程中出现异常，且此时Consumer正常，因此Producer无需release Consumer。
                    而Consumer当检测到队列为空时，开始等待，导致线程无法退出。
                """
                if self._runtime.err_msg is not None:
                    self._logger.debug_end()
                    return
                self._wait_myself()
                self._execute()
                self._release_producer()
            # 打印结束
            self._logger.debug_end(False)
        except Exception as e:
            # 如果不是由于生产者异常引发的连带异常（如：队列已空 & 通知消费）
            if self._runtime.err is None:
                self._runtime.err = e
                self._runtime.err_msg = format_exc()
                # 如果有需要，恢复阻塞中的生产者
                self._release_producer()
                # 打印结束
                self._logger.debug_end(True)
            else:
                self._logger.debug_end(False)

    def _wait_myself(self):
        """
        如果队列为空，则阻塞自己；等待恢复。

        :return:
        """
        if self._queue.empty():
            self._logger.debug_wait()
            self._runtime.event_not_empty.clear()
            self._runtime.event_not_empty.wait()

    def _execute(self):
        """
        消费

        :return:
        """
        data = self._queue.get_nowait()
        self._wrapper.run(data)
        # self._logger.debug_execute(data)

    def _release_producer(self):
        """
        如果有需要，恢复阻塞中的生产者

        :return:
        """
        if not self._runtime.event_not_full.is_set():
            self._logger.debug_release()
            self._runtime.event_not_full.set()  # 有空


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
        self._producer.start()
        self._consumer.start()
        self._producer.join()
        self._consumer.join()
        if self._runtime.err is not None:
            raise self._runtime.err


class Runtime:
    def __init__(self):
        self.err = None
        self.err_msg = None
        self.event_not_empty = Event()  # is_set() 表示全满，否则表示未满
        self.event_not_full = Event()  # is_set() 表示全空，否则表示未空
        self.event_not_full.set()  # 未满
        self.event_not_empty.clear()  # 全空


class ProducerConsumerLogger(Logger):
    def __init__(self, role: int):
        if role == PRODUCER:
            self._self = PRODUCER
            self._other = CONSUMER
        else:
            self._self = CONSUMER
            self._other = PRODUCER

    def debug_tasknum(self, task_num: int):
        Logger.debug(f"{self._self} task num is {task_num}.")

    def debug_start(self):
        Logger.debug(f"{self._self} start.")

    def debug_end(self, with_err: bool):
        Logger.debug(f"{self._self} end{' with error' if with_err else ''}.")

    def debug_wait(self):
        Logger.debug(f"{self._self} make himself wait.")

    def debug_release(self):
        Logger.debug(f"{self._self} release {self._other}.")
