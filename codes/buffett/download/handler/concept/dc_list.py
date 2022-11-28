from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.target import CONCEPT_CODE, CONCEPT_NAME
from buffett.common.constants.meta.handler import CNCP_META
from buffett.common.constants.table import CNCP_LS
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator

BKMC = "板块名称"
BKDM = "板块代码"


class DcConceptListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        concepts = ak.stock_board_concept_name_em()
        # rename
        concepts = DataFrame(
            {CONCEPT_CODE: concepts[BKDM], CONCEPT_NAME: concepts[BKMC]}
        )
        return concepts

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=CNCP_LS, meta=CNCP_META)
        self._operator.try_insert_data(name=CNCP_LS, df=df, update=True, meta=CNCP_META)

    def select_data(self) -> Optional[DataFrame]:
        """
        获取概念板块清单

        :return:
        """
        return self._operator.select_data(CNCP_LS)
