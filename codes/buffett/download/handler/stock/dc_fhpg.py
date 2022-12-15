from typing import Optional

import pandas as pd

from buffett.adapter.akshare import ak
from buffett.adapter.error.data_source import DataSourceError
from buffett.adapter.pandas import DataFrame
from buffett.adapter.wellknown import tqdm
from buffett.common.constants.col import (
    DATE,
    TYPE,
    PXBLc,
    SGBLc,
    PGBLc,
    PGJGc,
    ZFBLc,
    ZFGSc,
    ZFJGc,
)
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.handler import STK_FHPG_META
from buffett.common.constants.table import STK_FHPG
from buffett.common.error import PreStepError
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler import Handler
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler

RENAME = {
    "type": TYPE,
    "pxbl": PXBLc,
    "sgbl": SGBLc,
    "pgbl": PGBLc,
    "pgjg": PGJGc,
    "zfbl": ZFBLc,
    "zfgs": ZFGSc,
    "zfjg": ZFJGc,
}


class DcFhpgHandler(Handler):
    def obtain_data(self):
        stock_list = self._get_stock_list()
        datas = self._download(stock_list)
        self._save_to_database(datas)

    def _get_stock_list(self) -> list[str]:
        """
        获取股票清单

        :return:
        """
        sse_handler = SseStockListHandler(operator=self._operator)
        bs_handler = BsStockListHandler(operator=self._operator)
        sse_list = sse_handler.select_data()
        if dataframe_not_valid(sse_list):
            raise PreStepError(DcFhpgHandler, SseStockListHandler)
        bs_list = bs_handler.select_data()
        if dataframe_not_valid(bs_list):
            raise PreStepError(DcFhpgHandler, BsStockListHandler)
        sse_list = sse_list[[CODE]]
        bs_list = bs_list[[CODE]]
        stock_list = pd.concat([sse_list, bs_list]).drop_duplicates()
        stock_list = list(stock_list[CODE].values)
        return stock_list

    def _download(self, stock_list: list[str]) -> DataFrame:
        """
        下载数据

        :param stock_list:
        :return:
        """
        datas = []
        with tqdm(total=len(stock_list)) as pbar:
            for code in stock_list:
                symbol = self._convert_code(code)
                try:
                    data = ak.stock_zh_a_fhpg_em(symbol)
                    data[CODE] = code
                    datas.append(data)
                except DataSourceError:
                    pass
                pbar.update(1)
        data = pd.concat(datas)
        data = data.rename(columns=RENAME)
        data = data[[CODE, DATE, TYPE, PXBLc, SGBLc, PGBLc, PGJGc, ZFBLc, ZFGSc, ZFJGc]]
        return data

    def _save_to_database(self, df: DataFrame):
        """
        保存数据到数据库

        :param df:
        :return:
        """
        self._operator.create_table(name=STK_FHPG, meta=STK_FHPG_META)
        self._operator.insert_data(name=STK_FHPG, df=df)

    @staticmethod
    def _convert_code(code: str) -> str:
        if code[0] == "6":
            return "sh" + code
        elif code[0] == "0":
            return "sz" + code
        elif code[0] == "3":
            return "sz" + code
        raise NotImplemented

    def select_data(self, index: bool = True) -> Optional[DataFrame]:
        """
        获取分红配股数据

        :return:
        """
        df = self._operator.select_data(name=STK_FHPG, meta=STK_FHPG_META)
        if dataframe_not_valid(df):
            return
        if index:
            df = df.set_index([DATE])
        return df
