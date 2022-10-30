import akshare as ak
from pandas import DataFrame

from buffett.common import create_meta
from buffett.common.pendelum import to_my_date
from buffett.constants.col import DATE, OPEN, CLOSE, HIGH, LOW, CJL, CJE, ZF, ZDF, ZDE, HSL
from buffett.download import Para
from buffett.download.mysql import ColType, AddReqType, Operator
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
    def __init__(self, operator: Operator):
        super().__init__(operator=operator)
        self._fuquans = [FuquanType.BFQ, FuquanType.QFQ, FuquanType.HFQ]

    def _download(self, para: Para) -> DataFrame:
        # 使用接口（stock_zh_a_hist，源：东财）,code为Str6
        # 备用接口（stock_zh_a_daily，源：新浪，未实现）
        start_date = para.span.start.format('YYYYMMDD')
        end_date = para.span.end.format('YYYYMMDD')
        daily_info = ak.stock_zh_a_hist(symbol=para.stock.code.to_code6(),
                                        period='daily',
                                        start_date=start_date,
                                        end_date=end_date,
                                        adjust=para.comb.fuquan.ak_format())

        # 重命名
        daily_info.rename(columns=_RENAME, inplace=True)
        return daily_info

    def _save_to_database(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        if (not isinstance(data, DataFrame)) or data.empty:
            return

        self._operator.create_table(name=name,
                                    meta=_META,
                                    if_not_exist=True)
        self._operator.insert_data(name, data)
        self._operator.disconnect()

    def get_data(self, para: Para) -> DataFrame:
        """
        查询某支股票以某种复权方式的全部数据

        :param para:        下载参数
        :return:
        """
        spara = para.clone().with_freq(self._freq).with_source(self._source)
        table_name = AkDailyHandler._get_table_name(para=spara)
        df = self._operator.get_table(table_name)
        if (not isinstance(df, DataFrame)) or df.empty:
            return DataFrame()

        df[DATE] = df[DATE].apply(lambda x: to_my_date(x))
        df.index = df[DATE]
        del df[DATE]
        return df
