from buffett.backtrader.frame.exchange import Exchange
from buffett.backtrader.interface.time_sequence import ITimeSequence as Sequence
from buffett.download.handler import Handler
from buffett.download.types import HeadType


class StrategyBase(Sequence):
    def __init__(self):
        self._exchange: Exchange = Exchange()

    @staticmethod
    def col_request() -> dict[HeadType, type[Handler]]:
        return dict()

    # 订单结算
    def run(self):
        pass


class StrategyBaseUnderBuilt(StrategyBase):
    def set_exchange(self, exchange: Exchange):
        self._exchange = exchange
