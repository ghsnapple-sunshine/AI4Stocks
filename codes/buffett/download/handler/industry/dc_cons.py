from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, pd
from buffett.common.constants.col.target import CODE, NAME, INDUSTRY_CODE, INDUSTRY_NAME
from buffett.common.constants.meta.handler import INDUS_CONS_META
from buffett.common.constants.table import INDUS_CONS_LS
from buffett.common.error import PreStepError
from buffett.common.logger import Logger
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.base import FastHandler
from buffett.download.handler.industry.dc_list import DcIndustryListHandler
from buffett.download.handler.list import SseStockListHandler

DM = "代码"
MC = "名称"


class DcIndustryConsHandler(FastHandler):
    def _download(self) -> Optional[DataFrame]:
        industries = DcIndustryListHandler(self._operator).select_data()
        if dataframe_not_valid(industries):
            raise PreStepError(
                curr_step=DcIndustryConsHandler, pre_step=DcIndustryListHandler
            )
        cons = pd.concat_safe(
            [self._download_industry(row) for row in industries.itertuples(index=False)]
        )
        Logger.info(f"Before filter, total record num is {len(cons)}.")

        stocks = SseStockListHandler(self._operator).select_data()
        if dataframe_not_valid(stocks):
            raise PreStepError(
                curr_step=DcIndustryConsHandler, pre_step=SseStockListHandler
            )
        cons = pd.merge(cons, stocks[[CODE]], how="inner", on=[CODE])
        Logger.info(f"After filter, total record num is {len(cons)}.")
        return cons

    @staticmethod
    def _download_industry(row: tuple):
        """
        分别下载每个行业板块的成分股

        :param row:
        :return:
        """
        cons = ak.stock_board_industry_cons_em(symbol=getattr(row, INDUSTRY_NAME))
        cons = DataFrame(
            {
                CODE: cons[DM],
                NAME: cons[MC],
                INDUSTRY_CODE: getattr(row, INDUSTRY_CODE),
                INDUSTRY_NAME: getattr(row, INDUSTRY_NAME),
            }
        )
        return cons

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=INDUS_CONS_LS, meta=INDUS_CONS_META)
        self._operator.try_insert_data(
            name=INDUS_CONS_LS, df=df, update=True, meta=INDUS_CONS_META
        )
        self._operator.disconnect()

    def select_data(self) -> Optional[DataFrame]:
        """
        获取行业板块成分股

        :return:
        """
        return self._operator.select_data(INDUS_CONS_LS)
