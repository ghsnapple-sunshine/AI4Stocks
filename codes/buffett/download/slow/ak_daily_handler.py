from typing import Optional

import akshare as ak
from pandas import DataFrame

from buffett.common import create_meta
from buffett.common.tools import dataframe_not_valid
from buffett.constants.col import DATE, OPEN, CLOSE, HIGH, LOW, CJL, CJE, ZF, ZDF, ZDE, HSL
from buffett.download import Para
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.download.slow.handler import SlowHandler
from buffett.download.types import FuquanType

_RENAME = {'日期': DATE,
           '开盘': OPEN,
           '收盘': CLOSE,
           '最高': HIGH,
           '最低': LOW,
           '成交量': CJL,
           '成交额': CJE,
           '振幅': ZF,
           '涨跌幅': ZDF,
           '涨跌额': ZDE,
           '换手率': HSL}

_META = create_meta(meta_list=[
    [DATE, ColType.DATE, AddReqType.KEY],
    [OPEN, ColType.FLOAT, AddReqType.NONE],
    [CLOSE, ColType.FLOAT, AddReqType.NONE],
    [HIGH, ColType.FLOAT, AddReqType.NONE],
    [LOW, ColType.FLOAT, AddReqType.NONE],
    [CJL, ColType.INT32, AddReqType.NONE],
    [CJE, ColType.FLOAT, AddReqType.NONE],
    [ZF, ColType.FLOAT, AddReqType.NONE],
    [ZDF, ColType.FLOAT, AddReqType.NONE],
    [ZDE, ColType.FLOAT, AddReqType.NONE],
    [HSL, ColType.FLOAT, AddReqType.NONE]])


class AkDailyHandler(SlowHandler):
    def __init__(self,
                 operator: Operator):
        super().__init__(operator=operator)
        self._fuquans = [FuquanType.BFQ, FuquanType.QFQ, FuquanType.HFQ]

    def _download(self, para: Para) -> DataFrame:
        # 使用接口（stock_zh_a_hist，源：东财）,code为Str6
        # 备用接口（stock_zh_a_daily，源：新浪，未实现）
        start_date = para.span.start.format('YYYYMMDD')
        end_date = para.span.end.subtract(days=1).format('YYYYMMDD')
        daily_info = ak.stock_zh_a_hist(symbol=para.stock.code.to_code6(),
                                        period='daily',
                                        start_date=start_date,
                                        end_date=end_date,
                                        adjust=para.comb.fuquan.ak_format())

        # 重命名
        daily_info.rename(columns=_RENAME, inplace=True)
        return daily_info

    def _save_to_database(self,
                          table_name: str,
                          df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return

        self._operator.create_table(name=table_name,
                                    meta=_META,
                                    if_not_exist=True)
        self._operator.insert_data(table_name, df)
        self._operator.disconnect()

    def select_data(self,
                    para: Para) -> Optional[DataFrame]:
        """
        查询某支股票以某种复权方式的全部数据

        :param para:        code, fuquan
        :return:
        """
        para = para.clone().with_freq(self._freq).with_source(self._source)
        table_name = AkDailyHandler._get_table_name_by_code(para=para)
        df = self._operator.select_data(table_name)
        if dataframe_not_valid(df):
            return
        df.index = df[DATE]
        del df[DATE]
        return df
