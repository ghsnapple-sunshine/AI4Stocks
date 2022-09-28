import baostock as bs
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.constants import META_COLS
from ai4stocks.common.types import FuquanType, DataSourceType, DataFreqType
from ai4stocks.common.stock_code import StockCodeType, StockCode
from ai4stocks.download.base_handler import BaseHandler
from ai4stocks.download.connect.mysql_common import MysqlColType, MysqlColAddReq
from ai4stocks.download.connect.mysql_operator import MysqlOperator
from ai4stocks.download.download_recorder import DownloadRecorder


def __Str2Datetime__(str_datetime: str) -> DateTime:
    year = int(str_datetime[0:4])
    month = int(str_datetime[4:6])
    day = int(str_datetime[6:8])
    hour = int(str_datetime[8:10])
    minute = int(str_datetime[10:12])
    return DateTime(year=year, month=month, day=day, hour=hour, minute=minute)

class StockMinuteHandler(BaseHandler):
    def __init__(self, op: MysqlOperator):
        self.op = op
        self.recorder = DownloadRecorder(op=op)
        self.source = DataSourceType.BAOSTOCK
        self.fuquans = [FuquanType.NONE]
        self.code_type = StockCodeType.CODE6
        self.freq = DataFreqType.MIN5

    def __Download__(self, code: StockCode, fuquan: FuquanType, start_time: DateTime, end_time: DateTime) -> DataFrame:
        bs.login()
        start_time = start_time.format('YYYY-MM-DD')
        end_time = end_time.format('YYYY-MM-DD')

        fields = "time,open,high,low,close,volume,amount"
        rs = bs.query_history_k_data_plus(
            code=code.toCode9(),
            fields=fields,
            frequency='5',
            start_date=start_time,
            end_date=end_time,
            adjustflag=str(fuquan.value))
        minute_info = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            minute_info.append(rs.get_row_data())
        minute_info = DataFrame(minute_info, columns=rs.fields)

        # 重命名
        MINUTE_NAME_DICT = {'volume': 'chengjiaoliang',
                            'amount': 'chengjiaoe'}
        minute_info.rename(
            columns=MINUTE_NAME_DICT,
            inplace=True)
        minute_info['datetime'] = minute_info.apply(
            lambda x: __Str2Datetime__(x['time']), axis=1)
        minute_info.drop(
            columns=['time'],
            inplace=True)

        bs.logout()
        return minute_info

    def __Save2Database__(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        cols = [
            ['datetime', MysqlColType.DATETIME, MysqlColAddReq.KEY],
            ['open', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['close', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['high', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['low', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['chengjiaoliang', MysqlColType.INT32, MysqlColAddReq.NONE],
            ['chengjiaoe', MysqlColType.FLOAT, MysqlColAddReq.NONE],
        ]
        table_meta = DataFrame(data=cols, columns=META_COLS)
        self.op.CreateTable(name, table_meta)
        self.op.InsertData(name, data)
