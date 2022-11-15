from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, Series, pd
from buffett.common import create_meta
from buffett.common.error.pre_step import PreStepError
from buffett.common.logger import Logger
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col.stock import CONCEPT_CODE, CONCEPT_NAME, CODE, NAME
from buffett.constants.table import CNCP_CONS_LS
from buffett.download import Para
from buffett.download.fast.concept.ak_list import ConceptListHandler
from buffett.download.fast.handler import FastHandler
from buffett.download.fast.stock_list_handler import StockListHandler
from buffett.download.mysql.types import ColType, AddReqType

DM = '代码'
MC = '名称'

_META = create_meta(meta_list=[
    [CONCEPT_CODE, ColType.CONCEPT_CODE_NAME, AddReqType.KEY],
    [CONCEPT_NAME, ColType.CONCEPT_CODE_NAME, AddReqType.NONE],
    [CODE, ColType.STOCK_CODE_NAME, AddReqType.KEY],
    [NAME, ColType.STOCK_CODE_NAME, AddReqType.NONE]])


class ConceptConsHandler(FastHandler):
    def _download(self) -> DataFrame:
        concepts = ConceptListHandler(self._operator).select_data()
        if dataframe_not_valid(concepts):
            raise PreStepError(curr_step=ConceptConsHandler, pre_step=ConceptListHandler)
        cons = pd.concat([self._download_concept(row) for index, row in concepts.iterrows()])
        Logger.info(f'Before filter, total record num is {len(cons)}.')

        stocks = StockListHandler(self._operator).select_data()
        if dataframe_not_valid(stocks):
            raise PreStepError(curr_step=ConceptConsHandler, pre_step=StockListHandler)
        cons = pd.merge(cons, stocks[[CODE]], how='inner', on=[CODE])
        Logger.info(f'After filter, total record num is {len(cons)}.')
        return cons

    @staticmethod
    def _download_concept(row: Series):
        """
        分别下载每个概念板块的成分股

        :param row:
        :return:
        """
        cons = ak.stock_board_concept_cons_em(symbol=row[CONCEPT_NAME])
        cons = DataFrame({CODE: cons[DM],
                          NAME: cons[MC],
                          CONCEPT_CODE: row[CONCEPT_CODE],
                          CONCEPT_NAME: row[CONCEPT_NAME]})
        return cons

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=CNCP_CONS_LS, meta=_META)
        self._operator.try_insert_data(name=CNCP_CONS_LS, df=df, update=True, meta=_META)
        self._operator.disconnect()

    def select_data(self,
                    para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(CNCP_CONS_LS)
        return df
