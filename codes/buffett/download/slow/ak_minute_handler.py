import akshare as ak
from pandas import DataFrame

from buffett.common import create_meta
from buffett.common.pendelum import to_my_datetime, date_to_datetime
from buffett.constants.col import DATETIME, OPEN, CLOSE, HIGH, LOW, CJL, CJE, ZF, ZDF, ZDE, HSL
from buffett.download import Para
from buffett.download.mysql import ColType, AddReqType, Operator
from buffett.download.slow.handler import SlowHandler
from buffett.download.types import SourceType, FuquanType, FreqType

_RENAME = {'时间': DATETIME,
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
    [DATETIME, ColType.DATETIME, AddReqType.KEY],
    [OPEN, ColType.FLOAT, AddReqType.NONE],
    [CLOSE, ColType.FLOAT, AddReqType.NONE],
    [HIGH, ColType.FLOAT, AddReqType.NONE],
    [LOW, ColType.FLOAT, AddReqType.NONE],
    [CJL, ColType.INT32, AddReqType.NONE],
    [CJE, ColType.FLOAT, AddReqType.NONE],
    [ZF, ColType.FLOAT, AddReqType.NONE],
    [ZDF, ColType.FLOAT, AddReqType.NONE],
    [ZDE, ColType.FLOAT, AddReqType.NONE],
    [HSL, ColType.FLOAT, AddReqType.NONE]
])


class AkMinuteHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator)
        self._source = SourceType.AKSHARE_DONGCAI
        self._fuquans = [FuquanType.BFQ]
        self._freq = FreqType.MIN5

    def _download(self, para: Para) -> DataFrame:
        # 使用接口（stock_zh_a_hist_min_em，源：东财）,code为Str6
        minute_info = ak.stock_zh_a_hist_min_em(
            symbol=para.stock.code.to_code6(),
            period='5',
            start_date=date_to_datetime(para.span.start).format('YYYY-MM-DD HH:mm:ss'),
            end_date=date_to_datetime(para.span.end).format('YYYY-MM-DD HH:mm:ss'),
            adjust=para.comb.fuquan.ak_format())

        minute_info.rename(columns=_RENAME, inplace=True)  # 重命名
        return minute_info

    def _save_to_database(
            self,
            name: str,
            data: DataFrame
    ):
        if (not isinstance(data, DataFrame)) or data.empty:
            return

        self._operator.create_table(name=name, meta=_META)
        self._operator.insert_data(name=name, data=data)

    def get_data(self, para: Para) -> DataFrame:
        table_name = AkMinuteHandler._get_table_name(para=para)
        df = self._operator.get_table(table_name)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[DATETIME] = df[DATETIME].apply(lambda x: to_my_datetime(x))
        df.index = df[DATETIME]
        del df[DATETIME]
        return df
