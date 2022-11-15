from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, Series, pd
from buffett.common import create_meta
from buffett.common.error.pre_step import PreStepError
from buffett.common.logger import Logger
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col.stock import CODE, NAME, INDUSTRY_CODE, INDUSTRY_NAME
from buffett.constants.table import IND_CONS_LS
from buffett.download import Para
from buffett.download.fast.handler import FastHandler
from buffett.download.fast.industry.ak_list import IndustryListHandler
from buffett.download.fast.stock_list_handler import StockListHandler
from buffett.download.mysql.types import ColType, AddReqType

DM = '代码'
MC = '名称'

_META = create_meta(meta_list=[
    [INDUSTRY_CODE, ColType.CONCEPT_CODE_NAME, AddReqType.KEY],
    [INDUSTRY_NAME, ColType.CONCEPT_CODE_NAME, AddReqType.NONE],
    [CODE, ColType.STOCK_CODE_NAME, AddReqType.KEY],
    [NAME, ColType.STOCK_CODE_NAME, AddReqType.NONE]])


class IndustryConsHandler(FastHandler):
    def _download(self) -> Optional[DataFrame]:
        industries = IndustryListHandler(self._operator).select_data()
        if dataframe_not_valid(industries):
            raise PreStepError(curr_step=IndustryConsHandler, pre_step=IndustryListHandler)
        cons = pd.concat([self._download_industry(row) for index, row in industries.iterrows()])
        Logger.info(f'Before filter, total record num is {len(cons)}.')

        stocks = StockListHandler(self._operator).select_data()
        if dataframe_not_valid(stocks):
            raise PreStepError(curr_step=IndustryConsHandler, pre_step=StockListHandler)
        cons = pd.merge(cons, stocks[[CODE]], how='inner', on=[CODE])
        Logger.info(f'After filter, total record num is {len(cons)}.')
        return cons

    @staticmethod
    def _download_industry(row: Series):
        """
        分别下载每个行业板块的成分股

        :param row:
        :return:
        """
        cons = ak.stock_board_industry_cons_em(symbol=row[INDUSTRY_NAME])
        cons = DataFrame({CODE: cons[DM],
                          NAME: cons[MC],
                          INDUSTRY_CODE: row[INDUSTRY_CODE],
                          INDUSTRY_NAME: row[INDUSTRY_NAME]})
        return cons

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=IND_CONS_LS, meta=_META)
        self._operator.try_insert_data(name=IND_CONS_LS, df=df, update=True, meta=_META)
        self._operator.disconnect()

    def select_data(self,
                    para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(IND_CONS_LS)
        return df
