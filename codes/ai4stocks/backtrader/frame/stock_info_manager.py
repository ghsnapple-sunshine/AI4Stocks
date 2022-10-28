from typing import Type

from pandas import DataFrame

from ai4stocks.backtrader.frame.clock import Clock
from ai4stocks.backtrader.frame.column import Column
from ai4stocks.backtrader.frame.stock_info import StockInfo
from ai4stocks.backtrader.interface import ITimeSequence
from ai4stocks.common import Code as Code
from ai4stocks.common.pendelum import DateSpan
from ai4stocks.download.handler import Handler, Para
from ai4stocks.download.mysql import Operator
from ai4stocks.download.slow import AkStockDailyHandler as DHandler
from ai4stocks.download.types import FuquanType, HeadType


def _create_stock_info(code: Code,
                       cols: list[HeadType],
                       sources: dict[Handler, list[HeadType]],
                       datespan: DateSpan,
                       calendar: DataFrame,
                       operator: Operator,
                       clock: Clock) -> StockInfo:
    """
    查询某支股票的行情数据，返回一个StockInfo

    :param code:        股票内部代码
    :param cols:        StockInfo需要涵盖的列（多个）
    :param sources:     数据来源
    :param datespan     数据时间周期
    :param calendar:    交易日历
    :param operator:    Mysql操作器
    :param clock:       世界时钟
    :return:            一个StockInfo
    """
    info: dict[HeadType, Column] = {}
    data = __fetch__(code=code,
                     sources=sources,
                     datespan=datespan,
                     calendar=calendar,
                     operator=operator)
    for ctype in cols:
        info[ctype] = Column(data=list(data[str(ctype)]),
                             clock=clock)
    return StockInfo(data=info)


def __fetch__(code: Code,
              sources: dict[Handler, list[HeadType]],
              datespan: DateSpan,
              calendar: DataFrame,
              operator: Operator) -> DataFrame:
    """
    查询某支股票的历史行情数据

    :param code:        股票内部代码
    :param sources:     数据来源（Handler）
    :param datespan     数据时间周期
    :param calendar:    交易日历
    :param operator:    Mysql操作器
    :return:            某支股票的历史行情数据
    """
    base = calendar.copy(deep=True)
    for Hdl, cols in sources.items():
        data = Hdl(operator=operator).get_data(
            para=Para().with_code(code).with_fuquan(FuquanType.BFQ).with_span(datespan))
        base = base.join(data)
    return base


class StockInfoManager(ITimeSequence):
    def __init__(self):
        """
        初始化StockInfoManager
        """
        self._clock = Clock()
        self._stock_list: list[Code] = []
        self._infos: list[StockInfo] = []

    def run(self):
        """
        获取股票行情
        (当前股票行情为离线获取，因此所有获取方法在Builder中完成了。）

        :return:        None
        """
        pass

    def get_stock_code(self, code: int) -> Code:
        """
        查询股票代码

        :param code:    股票内部标识
        :return:        股票代码
        """
        return self._stock_list[code]

    def get_stock_num(self) -> int:
        """
        查询股票总数

        :return:        股票总数
        """
        return len(self._stock_list)

    def __getitem__(self, code: int) -> StockInfo:
        """
        查询股票信息

        :param code:    股票内部代码
        :return:        股票信息
        """
        return self._infos[code]

    def get_curr_olhc(self, code: int) -> tuple[int, int, int, int]:
        """
        查询某支股票在当前时刻的开盘，最低，最高，成交量

        :param code:    股票内部代码
        :return:        某支股票在当前时刻的开盘，最低，最高，成交量
        """
        return (
            self[code][HeadType.OPEN][0],
            self[code][HeadType.LOW][0],
            self[code][HeadType.HIGH][0],
            self[code][HeadType.CJL][0]
        )

    def get_all_close(self) -> list[int]:
        """
        查询所有在股票当前时刻的收盘

        :return:        所有股票当前时刻的收盘
        """
        ls = list(range(self.get_stock_num()))
        ls = [self[c][HeadType.CLOSE][0] for c in ls]
        return ls


class StockInfoManagerBuilder:
    class StockInfoManagerUnderBuilt(StockInfoManager):
        def set_stock_list(self, stock_list: list[Code]):
            self._stock_list = stock_list

        def set_infos(self, infos: list[StockInfo]):
            self._infos = infos

        def get_stock_list(self):
            return self._stock_list

    def __init__(self):
        self.item = StockInfoManagerBuilder.StockInfoManagerUnderBuilt()

    def with_stock_list(self, stock_list: list[Code]):
        stock_list = list(set(stock_list))
        self.item.set_stock_list(stock_list=stock_list)
        return self

    def with_infos(
            self,
            operator: Operator,
            add_cols: dict[HeadType, Type[Handler]],
            calendar: DataFrame,
            datespan: DateSpan,
            clock: Clock
    ):
        cols, sources = StockInfoManagerBuilder.__group_handler__(add_cols=add_cols)
        infos = self.__create_stock_infos__(cols=cols,
                                            sources=sources,
                                            calendar=calendar,
                                            operator=operator,
                                            datespan=datespan,
                                            clock=clock)
        self.item.set_infos(infos=infos)
        return self

    def build(self):
        self.item.__class__ = StockInfoManager
        return self.item

    @staticmethod
    def __group_handler__(
            add_cols: dict[HeadType, Type[Handler]]
    ) -> tuple[list[HeadType], dict[Handler, list[HeadType]]]:
        """
        基于基础列和策略指定的额外列，对数据源进行分类

        :param add_cols:     策略类需要的额外类
        :return:            StockInfo需要涵盖的列（多个），数据来源
        """
        cols = {
            HeadType.OPEN: DHandler,
            HeadType.LOW: DHandler,
            HeadType.HIGH: DHandler,
            HeadType.CLOSE: DHandler,
            HeadType.CJL: DHandler
        }
        sources: dict[Handler, list[HeadType]] = {
            DHandler: [HeadType.OPEN, HeadType.CLOSE, HeadType.HIGH, HeadType.LOW, HeadType.CJL]
        }
        for ctype, Thdl in add_cols.items():
            cols[ctype] = Thdl
            if Thdl in cols.keys:
                sources[Thdl].append(ctype)
            else:
                sources[Thdl] = [ctype]
        return list(cols.keys()), sources

    def __create_stock_infos__(
            self,
            cols: list[HeadType],
            sources: dict[Handler, list[HeadType]],
            datespan: DateSpan,
            calendar: DataFrame,
            operator: Operator,
            clock: Clock
    ) -> list[StockInfo]:
        """
        查询所有股票的行情数据，返回多个StockInfo

        :param cols:        StockInfo需要涵盖的列（多个）
        :param sources:     数据来源
        :param datespan     数据时间周期1
        :param calendar:    交易日历
        :param operator:    Mysql操作器
        :param clock:       世界时钟
        :return:            多个StockInfo
        """
        infos = []
        stock_list = self.item.get_stock_list()
        for idx in range(len(stock_list)):
            code = stock_list[idx]
            info = _create_stock_info(code=code,
                                      cols=cols,
                                      sources=sources,
                                      datespan=datespan,
                                      calendar=calendar,
                                      operator=operator,
                                      clock=clock)
            infos.append(info)
        return infos
