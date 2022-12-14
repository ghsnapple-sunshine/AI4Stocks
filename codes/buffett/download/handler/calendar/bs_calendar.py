from typing import Optional

from buffett.adapter.baostock import bs
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import DATE
from buffett.common.constants.meta.handler import CAL_META
from buffett.common.constants.table import TRA_CAL
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator


#
class CalendarHandler(FastHandler):
    """
    交易日查询
    """

    def __init__(self, operator: Operator):
        super().__init__(operator)

    def _download(self) -> DataFrame:
        #### 获取交易日信息 ####
        calendar = bs.query_trade_dates(
            start_date="1990-01-01",
            end_date=Date.today().add(months=2).format("YYYY-MM-01"),
        )

        # 仅保留交易日
        calendar = calendar[calendar["is_trading_day"] == "1"]
        # 保留有用列
        calendar = DataFrame({DATE: calendar["calendar_date"]})

        return calendar

    def _save_to_database(self, df: DataFrame):
        self._operator.create_table(name=TRA_CAL, meta=CAL_META)
        self._operator.try_insert_data(name=TRA_CAL, df=df)  # 忽略重复Insert

    def select_data(self, para: Para = None) -> Optional[DataFrame]:
        """
        获取指定时间段内的交易日历

        :param para:
        :return:
        """
        span = None if para is None else para.span
        df = self._operator.select_data(name=TRA_CAL, meta=CAL_META, span=span)
        if dataframe_not_valid(df):
            return
        df = df.set_index(DATE)
        return df
