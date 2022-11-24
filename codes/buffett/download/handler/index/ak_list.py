from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col.target import INDEX_CODE, INDEX_NAME
from buffett.common.constants.table import INDEX_LS
from buffett.common.tools import dataframe_not_valid, create_meta
from buffett.download.handler.fast import FastHandler
from buffett.download.mysql.types import ColType, AddReqType

DSPL_CODE = "index_code"
DSPL_NAME = "display_name"

_META = create_meta(
    meta_list=[
        [INDEX_CODE, ColType.CODE, AddReqType.KEY],
        [INDEX_NAME, ColType.INDEX_NAME, AddReqType.NONE],
    ]
)


class AkIndexListHandler(FastHandler):
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
        self._operator.create_table(name=INDEX_LS, meta=_META)
        self._operator.try_insert_data(name=INDEX_LS, df=df, update=True, meta=_META)

    def select_data(self) -> Optional[DataFrame]:
        """
        获取指数清单

        :return:
        """
        df = self._operator.select_data(name=INDEX_LS)
        return df
