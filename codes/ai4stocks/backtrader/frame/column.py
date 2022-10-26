from ai4stocks.backtrader.frame.clock_manager import Clock
from ai4stocks.backtrader.interface import ITimeSequence as Sequence


class Column(Sequence):
    def __init__(self, data: list, clock: Clock):
        self._data: list = data
        self._clock: Clock = clock

    def run(self):
        self.__reload__()

    def __reload__(self):
        pass  # 预留，大内存情况下，分段加载数据。

    def __getitem__(self, offset: int) -> int:
        if offset > 0:
            raise ValueError("Detect future failed: offset is {0}".format(offset))
        want_tick = self.curr_tick + offset
        if want_tick < 0 or want_tick >= len(self._data):
            raise ValueError('Input a incorrect offset.')
        return self._data[want_tick]
