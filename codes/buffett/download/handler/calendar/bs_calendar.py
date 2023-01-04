from typing import Optional

from buffett.adapter.baostock import bs
from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame, pd, DateOffset
from buffett.common.constants.col import DATE, DATETIME
from buffett.common.constants.meta.handler import CAL_META
from buffett.common.constants.table import TRA_CAL
from buffett.common.pendulum import Date, convert_datetimes
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator

ADD = "add"


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

    def select_data(
        self, para: Para = None, index: bool = True, to_datetimes: bool = False
    ) -> Optional[DataFrame]:
        """
        获取指定时间段内的交易日历

        :param para:
        :param index:           使用索引
        :param to_datetimes:    转换成datetime
        :return:
        """
        span = None if para is None else para.span
        dates = self._operator.select_data(name=TRA_CAL, meta=CAL_META, span=span)
        if dataframe_not_valid(dates):
            return
        if to_datetimes:
            dates[DATETIME] = convert_datetimes(dates[DATE])
            add_times = DataFrame(
                {
                    ADD: [
                        DateOffset(hour=x // 60, minute=x % 60)
                        for x in np.concatenate(
                            [np.arange(5, 125, 5), np.arange(215, 335, 5)]
                        )
                        + 570
                    ]
                }
            )
            dates = pd.merge(dates, add_times, how="cross")
            datetimes = DataFrame({DATETIME: dates[DATETIME] + dates[ADD]})
            datetimes = datetimes.sort_values(by=[DATETIME])
            if index:
                datetimes = datetimes.set_index(DATETIME)
            return datetimes
        else:
            if index:
                dates = dates.set_index(DATE)
            return dates
