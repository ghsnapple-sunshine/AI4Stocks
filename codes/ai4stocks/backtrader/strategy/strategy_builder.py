from ai4stocks.backtrader.strategy.strategy import StrategyBase, StrategyBaseUnderBuilt

_BUILT_CLASS_DICT: dict[type, type] = {
    StrategyBase: StrategyBaseUnderBuilt
}


class StrategyBuilder:
    def __init__(self, origin_cls: type):
        self._origin_cls = origin_cls
        under_built_cls = _BUILT_CLASS_DICT[origin_cls]
        self.item = under_built_cls()

    """
    def with_add_order(self, add_order: Wrapper):
        self.item.set_add_order(add_order=add_order)
        return self
    """

    def with_exchange(self, exchange):
        self.item.set_exchange(exchange=exchange)
        return self

    def build(self):
        self.item.__class__ = self._origin_cls
        return self.item
