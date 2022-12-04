from buffett.adapter.wellknown import sleep
from buffett.common.interface import ProducerConsumer
from buffett.common.wrapper import Wrapper
from test import SimpleTester


class TestProducerConsumer(SimpleTester):
    @classmethod
    def _setup_once(cls):
        pass

    def _setup_always(self) -> None:
        self._results = []
        self._count = 0

    def _produce(self):
        self._count += 1
        return 1

    def _produce_with_input(self, i):
        self._count += 1
        return i

    def _produce_with_error(self, i):
        self._count += 1
        if self._count <= 3:
            return i
        raise ValueError("i is larger than 3.")

    def _produce_with_delay(self, i):
        self._count += 1
        sleep(0.05 * self._count)
        self._results.append(i)
        return i

    def _consume(self, t):
        self._results.append(t)

    def _consume_with_delay(self, t):
        sleep(0.05 * (5 - self._count))
        self._results.append(-t)

    def test_run_without_para_qsize1(self):
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._produce),
            consumer=Wrapper(self._consume),
            args_map=None,
            task_num=5,
            queue_size=1,
        )
        prod_cons.run()
        assert self._results == [1, 1, 1, 1, 1]

    def test_run_without_para_qsize2(self):
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._produce),
            consumer=Wrapper(self._consume),
            args_map=None,
            task_num=5,
            queue_size=2,
        )
        prod_cons.run()
        assert self._results == [1, 1, 1, 1, 1]

    def test_run_with_para_qsize2(self):
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._produce_with_input),
            consumer=Wrapper(self._consume),
            args_map=[2, 3, 4, 1, 5],
            task_num=5,
            queue_size=2,
        )
        prod_cons.run()
        assert self._results == [2, 3, 4, 1, 5]

    def test_run_with_error_qsize2(self):
        try:
            prod_cons = ProducerConsumer(
                producer=Wrapper(self._produce_with_error),
                consumer=Wrapper(self._consume),
                args_map=[2, 3, 4, 1, 5],
                task_num=5,
                queue_size=2,
            )
            prod_cons.run()
            assert False
        except ValueError:
            print(self._results)
            assert self._results == [2, 3, 4]

    def test_run_with_delay_qsize1(self):
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._produce_with_delay),
            consumer=Wrapper(self._consume_with_delay),
            args_map=[1, 2, 3, 4, 5],
            task_num=5,
            queue_size=2,
        )
        prod_cons.run()
        """
        producer                consumer
        0.05(+0.05)    1       
        0.15(+0.1 )    2
                                0.25(+0.25)    -1
        0.4 (+0.15)    3                                 --waiting for 0.1--        
                                0.45(+0.2 )    -2
        0.6 (+0.2 )    4        0.6 (+0.15)    -3        --no sure who is first--
                                0.7 (+0.1 )    -4
        0.85(+0.25)    5        
                                0.85(+0.05)    -5
        """
        assert self._results[2] == -1
        assert self._results[-2] == 5
        assert self._results[-1] == -5
