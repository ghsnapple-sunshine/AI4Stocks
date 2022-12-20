from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.target import INDEX_CODE, INDEX_NAME
from buffett.common.constants.meta.handler import INDEX_META
from buffett.common.constants.table import INDEX_LS
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.base import FastHandler

DSPL_CODE = "index_code"
DSPL_NAME = "display_name"


class DcIndexListHandler(FastHandler):
    def _download(self) -> Optional[DataFrame]:
        indexs = ak.index_stock_info()
        if dataframe_not_valid(indexs):
            return
        # rename
        indexs = DataFrame(
            {INDEX_CODE: indexs[DSPL_CODE], INDEX_NAME: indexs[DSPL_NAME]}
        )
        return indexs

    def _save_to_database(self, df: DataFrame) -> None:
        self._operator.create_table(name=INDEX_LS, meta=INDEX_META)
        self._operator.try_insert_data(
            name=INDEX_LS, df=df, update=True, meta=INDEX_META
        )

    def select_data(self) -> Optional[DataFrame]:
        """
        获取指数清单

        :return:
        """
        return self._operator.select_data(name=INDEX_LS, meta=INDEX_META)
