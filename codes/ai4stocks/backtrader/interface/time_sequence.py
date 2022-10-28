# 时钟滚动通知接口
from abc import abstractmethod

from ai4stocks.backtrader.frame.clock_manager import Clock


class ITimeSequence:
    def __init__(self, clock: Clock = None):
        self._clock = clock

    @property
    def curr_tick(self) -> int:
        return self._clock.tick

    @property
    def curr_time(self) -> str:
        return self._clock.time

    @property
    def is_end(self) -> bool:
        return self._clock.is_end

    @abstractmethod
    def run(self):
        pass
