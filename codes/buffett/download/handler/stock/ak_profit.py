from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.pendulum import Date
from buffett.common import create_meta
from buffett.common.constants.col import DATE
from buffett.common.constants.col.date import (
    YEAR2021,
    YEAR2022,
    YEAR2023,
    YEAR2024,
    YEAR2025,
)
from buffett.common.constants.col.target import CODE
from buffett.common.constants.table import STK_PROFIT
from buffett.common.tools import dataframe_is_valid
from buffett.download.handler.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [DATE, ColType.DATE, AddReqType.KEY],
        [YEAR2021, ColType.FLOAT, AddReqType.NONE],
        [YEAR2022, ColType.FLOAT, AddReqType.NONE],
        [YEAR2023, ColType.FLOAT, AddReqType.NONE],
        [YEAR2024, ColType.FLOAT, AddReqType.NONE],
        [YEAR2025, ColType.FLOAT, AddReqType.NONE],
    ]
)

_RENAME = {
    "代码": CODE,
    "2021预测每股收益": YEAR2021,
    "2022预测每股收益": YEAR2022,
    "2023预测每股收益": YEAR2023,
    "2024预测每股收益": YEAR2024,
    "2025预测每股收益": YEAR2025,
}


class AkProfitHandler(FastHandler):
    def __init__(self, operator: Operator):
        super(AkProfitHandler, self).__init__(operator=operator)

    def _download(self) -> DataFrame:
        """
        采用增量save模式

        :return:
        """
        profit = ak.stock_profit_forecast()
        profit = profit.rename(columns=_RENAME)
        # cols = [YEAR2021, YEAR2022, YEAR2023, YEAR2024]
        code_n_cols = [CODE, YEAR2021, YEAR2022, YEAR2023, YEAR2024]
        # for col in cols:
        #     profit[col] = profit[col].apply(lambda x: round(x, 3))  # 保留3位小数
        profit = profit[code_n_cols]
        profit = profit.drop_duplicates()  # akshare下载完带有重复数据

        # 对比裁剪
        curr_profit = self.select_data()
        if dataframe_is_valid(curr_profit):
            curr_profit = curr_profit[code_n_cols]
            profit = pd.subtract(profit, curr_profit)

        today = Date.today()
        profit[DATE] = today
        return profit

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=STK_PROFIT, meta=_META)
        self._operator.try_insert_data(name=STK_PROFIT, df=df)

    def select_data(self) -> Optional[DataFrame]:
        """
        获取股票收益预测

        :return:
        """
        return self._operator.select_data(name=STK_PROFIT)
