# 下载沪深股票列表
from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col.stock import INDUSTRY_CODE, INDUSTRY_NAME
from buffett.constants.table import CNCP_LS, IND_LS
from buffett.download import Para
from buffett.download.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

BKMC = '板块名称'
BKDM = '板块代码'

_META = create_meta(meta_list=[
    [INDUSTRY_CODE, ColType.CONCEPT_CODE_NAME, AddReqType.KEY],
    [INDUSTRY_NAME, ColType.CONCEPT_CODE_NAME, AddReqType.NONE]])


class IndustryListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        industries = ak.stock_board_industry_name_em()
        # rename
        industries = DataFrame({INDUSTRY_CODE: industries[BKDM],
                                INDUSTRY_NAME: industries[BKMC]})
        return industries

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=IND_LS, meta=_META)
        self._operator.try_insert_data(name=IND_LS, df=df, update=True, meta=_META)
        self._operator.disconnect()

    def select_data(self,
                    para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(IND_LS)
        return df
