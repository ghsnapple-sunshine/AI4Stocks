# 下载沪深股票列表
from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common import create_meta
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col.stock import CONCEPT_CODE, CONCEPT_NAME
from buffett.constants.table import CNCP_LS
from buffett.download import Para
from buffett.download.fast.handler import FastHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType

BKMC = '板块名称'
BKDM = '板块代码'

_META = create_meta(meta_list=[
    [CONCEPT_CODE, ColType.CONCEPT_CODE_NAME, AddReqType.KEY],
    [CONCEPT_NAME, ColType.CONCEPT_CODE_NAME, AddReqType.NONE]])


class ConceptListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        concepts = ak.stock_board_concept_name_em()
        # rename
        concepts = DataFrame({CONCEPT_CODE: concepts[BKDM],
                              CONCEPT_NAME: concepts[BKMC]})
        return concepts

    def _save_to_database(self, df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=CNCP_LS, meta=_META)
        self._operator.try_insert_data(name=CNCP_LS, df=df, update=True, meta=_META)
        self._operator.disconnect()

    def select_data(self,
                    para: Para = None) -> Optional[DataFrame]:
        df = self._operator.select_data(CNCP_LS)
        return df
