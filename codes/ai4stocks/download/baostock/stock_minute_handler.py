import baostock as bs
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common import META_COLS, INT_MAX_VALUE, INT_MIN_VALUE, FLOAT_MAX_VALUE, FLOAT_MIN_VALUE, FuquanType, \
    DataSourceType, DataFreqType, StockCode
from ai4stocks.download import BaseHandler, DownloadRecorder
from ai4stocks.download.connect import MysqlColType, MysqlColAddReq, MysqlOperator


def __str_to_datetime__(str_datetime: str) -> DateTime:
    year = int(str_datetime[0:4])
    month = int(str_datetime[4:6])
    day = int(str_datetime[6:8])
    hour = int(str_datetime[8:10])
    minute = int(str_datetime[10:12])
    return DateTime(year=year, month=month, day=day, hour=hour, minute=minute)


def __check_int__(str_int: str):
    _int = int(str_int)
    if (_int > INT_MAX_VALUE) | (_int < INT_MIN_VALUE):
        return None
    return str_int


def __check_float__(str_float: str):
    _float = float(str_float)
    if (_float > FLOAT_MAX_VALUE) | (_float < FLOAT_MIN_VALUE):
        return None
    return str_float


class StockMinuteHandler(BaseHandler):
    def __init__(
            self,
            op: MysqlOperator
    ):
        self.op = op
        self.recorder = DownloadRecorder(op=op)
        self.source = DataSourceType.BAOSTOCK
        self.fuquans = [FuquanType.NONE]
        self.freq = DataFreqType.MIN5

    def __download__(
            self,
            code: StockCode,
            fuquan: FuquanType,
            start_time: DateTime,
            end_time: DateTime
    ) -> DataFrame:
        bs.login()
        start_time = start_time.format('YYYY-MM-DD')
        end_time = end_time.format('YYYY-MM-DD')


        fields = "time,open,high,low,close,volume,amount"
        rs = bs.query_history_k_data_plus(
            code=code.to_code9(),
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

        # 更改类型
        minute_info['datetime'] = minute_info['time'].apply(
            lambda x: __str_to_datetime__(x)
        )
        minute_info['open'] = minute_info['open'].apply(
            lambda x: __check_float__(x)
        )
        minute_info['close'] = minute_info['close'].apply(
            lambda x: __check_float__(x)
        )
        minute_info['high'] = minute_info['high'].apply(
            lambda x: __check_float__(x)
        )
        minute_info['low'] = minute_info['low'].apply(
            lambda x: __check_float__(x)
        )
        minute_info['chengjiaoliang'] = minute_info['chengjiaoliang'].apply(
            lambda x: __check_int__(x)
        )
        minute_info['chengjiaoe'] = minute_info['chengjiaoe'].apply(
            lambda x: __check_float__(x)
        )
        # 删除time字段
        minute_info.drop(
            columns=['time'],
            inplace=True
        )
        # 按照start_date和end_date过滤数据
        minute_info = minute_info[
            (minute_info['datetime'] <= end_time) &
            (minute_info['datetime'] >= start_time)
        ]

        bs.logout()
        return minute_info

    def __save_to_database__(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        if (not isinstance(data, DataFrame)) or data.empty:
            return

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
        self.op.create_table(name, table_meta)
        self.op.insert_data(name, data)
