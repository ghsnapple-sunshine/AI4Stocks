# 交易成功通知接口
from ai4stocks.backtrader.frame.order import Order


class ITradeSuccessNotify:
    def trade_success(self, order: Order):
        pass
