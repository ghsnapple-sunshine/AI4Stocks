from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, Series
from buffett.adapter.wellknown import tqdm
from buffett.common import create_meta, Code
from buffett.common.constants.col import DATE, SYL, DSYL, SJL, SXL, DSXL, GXL, ZSZ, DGXL
from buffett.common.constants.col.stock import CODE
from buffett.common.constants.table import get_stock_pepb
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler import Handler
from buffett.download.handler.list import StockListHandler
from buffett.download.mysql.types import AddReqType, ColType

_RENAME = {
    "trade_date": DATE,
    "pe": SYL,
    "pe_ttm": DSYL,
    "pb": SJL,
    "ps": SXL,
    "ps_ttm": DSXL,
    "dv_ratio": GXL,
    "dv_ttm": DGXL,
    "total_mv": ZSZ,
}

_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [CODE, ColType.CODE, AddReqType.KEY],
        [SYL, ColType.FLOAT, AddReqType.NONE],
        [DSYL, ColType.FLOAT, AddReqType.NONE],
        [SJL, ColType.FLOAT, AddReqType.NONE],
        [SXL, ColType.FLOAT, AddReqType.NONE],
        [DSXL, ColType.FLOAT, AddReqType.NONE],
        [GXL, ColType.FLOAT, AddReqType.NONE],
        [DGXL, ColType.FLOAT, AddReqType.NONE],
        [ZSZ, ColType.FLOAT, AddReqType.NONE],
    ]
)


class AkStockPePbHandler(Handler):
    def obtain_data(self, para: Para = None):
        stocks = StockListHandler(self._operator).select_data()
        with tqdm(total=len(stocks)) as pbar:
            for index, row in stocks.iterrows():
                self._download_and_save(row)
                pbar.update(1)

    def _download_and_save(self, row: Series) -> DataFrame:
        pepb = ak.stock_a_lg_indicator(symbol=row[CODE])
        # rename
        pepb = pepb.rename(columns=_RENAME)
        pepb[CODE] = row[CODE]
        table_name = get_stock_pepb(code=row[CODE])
        self._operator.create_table(name=table_name, meta=_META)
        self._operator.try_insert_data(
            name=table_name, meta=_META, df=pepb, update=True
        )
        return pepb

    def select_data(self, para: Para = None) -> Optional[DataFrame]:
        table_name = get_stock_pepb(code=para.stock.code)
        pepb = self._operator.select_data(name=table_name)
        if dataframe_not_valid(pepb):
            return
        pepb[CODE] = pepb[CODE].apply(lambda x: Code(x))
        return pepb
