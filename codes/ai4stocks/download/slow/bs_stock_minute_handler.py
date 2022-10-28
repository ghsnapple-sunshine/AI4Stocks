import baostock as bs
from pandas import DataFrame

from ai4stocks.common import create_meta
from ai4stocks.common.pendelum import to_my_datetime, date_to_datetime
from ai4stocks.constants.col import DATETIME, OPEN, CLOSE, HIGH, LOW, CJL, CJE
from ai4stocks.download import Para
from ai4stocks.download.mysql import ColType, AddReqType, Operator
from ai4stocks.download.slow.handler import SlowHandler
from ai4stocks.download.tools.tools import bs_result_to_dataframe, bs_str_to_datetime, bs_check_float, bs_check_int
from ai4stocks.download.types import SourceType, FuquanType, FreqType

_RENAME = {'time': DATETIME,
           'volume': CJL,
           'amount': CJE}

_META = create_meta(meta_list=[
    [DATETIME, ColType.DATETIME, AddReqType.KEY],
    [OPEN, ColType.FLOAT, AddReqType.NONE],
    [CLOSE, ColType.FLOAT, AddReqType.NONE],
    [HIGH, ColType.FLOAT, AddReqType.NONE],
    [LOW, ColType.FLOAT, AddReqType.NONE],
    [CJL, ColType.INT32, AddReqType.NONE],
    [CJE, ColType.FLOAT, AddReqType.NONE]])


class BsStockMinuteHandler(SlowHandler):
    def __init__(self, operator: Operator):
        super().__init__(operator)
        self._source = SourceType.BAOSTOCK
        self._fuquans = [FuquanType.BFQ]
        self._freq = FreqType.MIN5

    def _download(self, para: Para) -> DataFrame:
        bs.login()

        fields = "time,open,high,low,close,volume,amount"
        rs = bs.query_history_k_data_plus(code=para.stock.code.to_code9(),
                                          fields=fields,
                                          frequency='5',
                                          start_date=para.span.start.format('YYYY-MM-DD'),
                                          end_date=para.span.end.format('YYYY-MM-DD'),
                                          adjustflag=para.comb.fuquan.bs_format())
        minute_info = bs_result_to_dataframe(rs)

        # 重命名
        minute_info.rename(columns=_RENAME, inplace=True)

        # 按照start_date和end_date过滤数据
        minute_info[DATETIME] = minute_info[DATETIME].apply(lambda x: bs_str_to_datetime(x))
        minute_info = minute_info[(minute_info[DATETIME] <= date_to_datetime(para.span.start)) &
                                  (minute_info[DATETIME] >= date_to_datetime(para.span.end))]

        # 更改类型
        minute_info[OPEN] = minute_info[OPEN].apply(lambda x: bs_check_float(x))
        minute_info[CLOSE] = minute_info[CLOSE].apply(lambda x: bs_check_float(x))
        minute_info[HIGH] = minute_info[HIGH].apply(lambda x: bs_check_float(x))
        minute_info[LOW] = minute_info[LOW].apply(lambda x: bs_check_float(x))
        minute_info[CJL] = minute_info[CJL].apply(lambda x: bs_check_int(x))
        minute_info[CJE] = minute_info[CJE].apply(lambda x: bs_check_float(x))

        bs.logout()
        return minute_info

    def _save_to_database(self,
                          name: str,
                          data: DataFrame) -> None:
        if (not isinstance(data, DataFrame)) or data.empty:
            return

        self._operator.create_table(name=name, meta=_META)
        self._operator.insert_data(name, data)

    def get_data(self, para: Para) -> DataFrame:
        table_name = BsStockMinuteHandler._get_table_name(para=para)
        df = self._operator.get_table(table_name)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[DATETIME] = df[DATETIME].apply(lambda x: to_my_datetime(x))
        df.index = df[DATETIME]
        del df[DATETIME]
        return df
