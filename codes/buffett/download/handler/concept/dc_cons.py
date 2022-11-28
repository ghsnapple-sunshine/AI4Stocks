from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, pd
from buffett.common.constants.col.target import CONCEPT_CODE, CONCEPT_NAME, CODE, NAME
from buffett.common.constants.meta.handler import CNCP_CONS_META
from buffett.common.constants.table import CNCP_CONS_LS
from buffett.common.error.pre_step import PreStepError
from buffett.common.logger import Logger
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.base import FastHandler
from buffett.download.handler.concept.dc_list import DcConceptListHandler
from buffett.download.handler.list.sse_list import SseStockListHandler

DM = "代码"
MC = "名称"


class DcConceptConsHandler(FastHandler):
    def _download(self) -> DataFrame:
        concepts = DcConceptListHandler(self._operator).select_data()
        if dataframe_not_valid(concepts):
            raise PreStepError(
                curr_step=DcConceptConsHandler, pre_step=DcConceptListHandler
            )
        cons = pd.concat(
            [self._download_concept(row) for row in concepts.itertuples(index=False)]
        )
        Logger.info(f"Before filter, total record num is {len(cons)}.")

        stocks = SseStockListHandler(self._operator).select_data()
        if dataframe_not_valid(stocks):
            raise PreStepError(
                curr_step=DcConceptConsHandler, pre_step=SseStockListHandler
            )
        cons = pd.merge(cons, stocks[[CODE]], how="inner", on=[CODE])
        Logger.info(f"After filter, total record num is {len(cons)}.")
        return cons

    @staticmethod
    def _download_concept(row: tuple):
        """
        分别下载每个概念板块的成分股

        :param row:
        :return:
        """
        cons = ak.stock_board_concept_cons_em(symbol=getattr(row, CONCEPT_NAME))
        cons = DataFrame(
            {
                CODE: cons[DM],
                NAME: cons[MC],
                CONCEPT_CODE: getattr(row, CONCEPT_CODE),
                CONCEPT_NAME: getattr(row, CONCEPT_NAME),
            }
        )
        return cons

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=CNCP_CONS_LS, meta=CNCP_CONS_META)
        self._operator.try_insert_data(
            name=CNCP_CONS_LS, df=df, update=True, meta=CNCP_CONS_META
        )

    def select_data(self) -> Optional[DataFrame]:
        """
        获取概念板块成分股

        :return:
        """
        return self._operator.select_data(CNCP_CONS_LS)
