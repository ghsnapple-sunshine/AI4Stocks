from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.pendulum import Date
from buffett.common import create_meta
from buffett.common.constants.col import DATE
from buffett.common.constants.col.target import CODE
from buffett.common.constants.table import STK_PROFIT
from buffett.common.tools import dataframe_is_valid
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType


class DcProfitHandler(FastHandler):
    def __init__(self, operator: Operator):
        super(DcProfitHandler, self).__init__(operator=operator)

    def _download(self) -> DataFrame:
        """
        采用增量save模式

        :return:
        """
        profit = ak.stock_profit_forecast()
        #
        rename = self._get_rename()
        profit = profit.rename(columns=rename)
        # [CODE, '2021', '2022', ...]
        code_n_cols = list(filter(lambda x: x in profit.columns, rename.values()))
        profit = profit[code_n_cols]
        profit = profit.drop_duplicates()  # akshare下载完带有重复数据
        # ['2021', '2022', ...]
        cols = code_n_cols.copy()
        cols.remove(CODE)
        for col in cols:
            profit[col] = profit[col].apply(
                lambda x: round(float(x), 4)
            )  # 保留4位小数，确保数据库get到结果不变

        # 对比裁剪
        curr_profit = self.select_data()
        if dataframe_is_valid(curr_profit):
            curr_profit = curr_profit[code_n_cols]
            profit = pd.subtract(profit, curr_profit)

        today = Date.today()
        profit[DATE] = today
        return profit

    def _get_rename(self):
        """
        根据时间生成常量RENAME

        :return:
        """
        rename = {"代码": CODE}
        for year in range(2021, Date.today().year + 4):
            key = f"{year}预测每股收益"
            value = f"{year}"
            rename[key] = value
        return rename

    def _get_meta(self):
        """
        根据时间生成常量meta

        :return:
        """
        meta_list = [
            [CODE, ColType.CODE, AddReqType.KEY],
            [DATE, ColType.DATE, AddReqType.KEY],
        ]
        for year in range(2021, Date.today().year + 4):
            col = [f"{year}", ColType.FLOAT, AddReqType.NONE]
            meta_list.append(col)
        meta = create_meta(meta_list=meta_list)
        return meta

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=STK_PROFIT, meta=self._get_meta(), update=True)
        self._operator.try_insert_data(name=STK_PROFIT, df=df)

    def select_data(self) -> Optional[DataFrame]:
        """
        获取股票收益预测

        :return:
        """
        return self._operator.select_data(name=STK_PROFIT)
