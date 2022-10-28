from typing import Type

from ai4stocks.backtrader.frame.exchange import Exchange
from ai4stocks.backtrader.interface.time_sequence import ITimeSequence as Sequence
from ai4stocks.download.handler import Handler
from ai4stocks.download.types import HeadType


class StrategyBase(Sequence):
    def __init__(self):
        self._exchange: Exchange = Exchange()

    @staticmethod
    def col_request() -> dict[HeadType, Type[Handler]]:
        return dict()

    # 订单结算
    def run(self):
        pass


class StrategyBaseUnderBuilt(StrategyBase):
    def set_exchange(self, exchange: Exchange):
        self._exchange = exchange
