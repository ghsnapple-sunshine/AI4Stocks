from typing import Type

from ai4stocks.common import ColType as CType
from ai4stocks.backtrader.frame.exchange import Exchange
from ai4stocks.backtrader.interface.time_sequence import ITimeSequence as Sequence
from ai4stocks.download.handler_base import HandlerBase as Handler

class StrategyBase(Sequence):
    def __init__(self):
        self._exchange: Exchange = Exchange()

    @staticmethod
    def col_request() -> dict[CType, Type[Handler]]:
        return dict()

    # 订单结算
    def run(self):
        pass


class StrategyBaseUnderBuilt(StrategyBase):
    def set_exchange(self, exchange: Exchange):
        self._exchange = exchange
