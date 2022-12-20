# 下载沪深股票列表
from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.target import INDUSTRY_CODE, INDUSTRY_NAME
from buffett.common.constants.meta.handler import INDUS_META
from buffett.common.constants.table import INDUS_LS
from buffett.download.handler.base import FastHandler
from buffett.download.mysql import Operator

BKMC = "板块名称"
BKDM = "板块代码"


class DcIndustryListHandler(FastHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)

    def _download(self) -> DataFrame:
        industries = ak.stock_board_industry_name_em()
        # rename
        industries = DataFrame(
            {INDUSTRY_CODE: industries[BKDM], INDUSTRY_NAME: industries[BKMC]}
        )
        return industries

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=INDUS_LS, meta=INDUS_META)
        self._operator.try_insert_data(
            name=INDUS_LS, df=df, update=True, meta=INDUS_META
        )

    def select_data(self) -> Optional[DataFrame]:
        """
        获取行业板块清单

        :return:
        """
        return self._operator.select_data(name=INDUS_LS, meta=INDUS_META)
