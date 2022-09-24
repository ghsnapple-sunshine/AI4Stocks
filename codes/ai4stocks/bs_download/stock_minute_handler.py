import baostock as bs
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.common import FuquanType
from ai4stocks.common.stock_code import StockCodeConverter
from ai4stocks.data_connect.mysql_common import MysqlColType, MysqlColAddReq, MysqlConstants
from ai4stocks.data_connect.mysql_operator import MysqlOperator


class StockMinuteHandler:
    @staticmethod
    def DownloadStockMinuteInfos(code: str, start_date: DateTime, end_date: DateTime, fuquan: FuquanType) -> DataFrame:
        start_date = start_date.format('YYYY-MM-DD')
        end_date = end_date.format('YYYY-MM-DD')

        fields = "time,open,high,low,close,volume,amount"
        rs = bs.query_history_k_data_plus(
            code=code, fields=fields, frequency='5', start_date=start_date, end_date=end_date,
            adjustflag=str(fuquan.value))
        minute_info = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            minute_info.append(rs.get_row_data())
        minute_info = DataFrame(minute_info, columns=rs.fields)

        # 重命名
        MINUTE_NAME_DICT = {'volume': 'chengjiaoliang',
                            'amount': 'chengjiaoe'}
        minute_info.rename(columns=MINUTE_NAME_DICT, inplace=True)
        minute_info['datetime'] = minute_info.apply(lambda x: StockMinuteHandler.str2datetime(x['time']), axis=1)
        minute_info.drop(columns=['time'], inplace=True)
        return minute_info

    @staticmethod
    def str2datetime(str_datetime: str) -> DateTime:
        year = int(str_datetime[0:4])
        month = int(str_datetime[4:6])
        day = int(str_datetime[6:8])
        hour = int(str_datetime[8:10])
        minute = int(str_datetime[10:12])
        return DateTime(year=year, month=month, day=day, hour=hour, minute=minute)

    @staticmethod
    def Save2Database(op: MysqlOperator, code: str, fuquan: FuquanType, data: DataFrame) -> str:
        cols = [
            ['datetime', MysqlColType.DATETIME, MysqlColAddReq.PRIMKEY],
            ['open', MysqlColType.Float, MysqlColAddReq.NONE],
            ['close', MysqlColType.Float, MysqlColAddReq.NONE],
            ['high', MysqlColType.Float, MysqlColAddReq.NONE],
            ['low', MysqlColType.Float, MysqlColAddReq.NONE],
            ['chengjiaoliang', MysqlColType.Int32, MysqlColAddReq.NONE],
            ['chengjiaoe', MysqlColType.Float, MysqlColAddReq.NONE],
        ]
        table_meta = DataFrame(data=cols, columns=MysqlConstants.META_COLS)
        table_name = MysqlConstants.MINUTE_INFO_TABLE.format(5, code, fuquan.toString())  # 目前仅下载5分钟数据
        op.CreateTable(table_name, table_meta)
        op.InsertData(table_name, data)
        op.Disconnect()
        return table_name

    @staticmethod
    def DownloadAndSave(op: MysqlOperator, start_date: DateTime, end_date: DateTime) -> list:
        stocks = op.GetTable(MysqlConstants.STOCK_LIST_TABLE)

        # 登录系统
        lg = bs.login()
        # 显示登陆返回信息
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)

        tbls = []
        FUQUANS = [FuquanType.NONE]
        for index, row in stocks.iterrows():
            for fuquan in FUQUANS:
                code = StockCodeConverter.Code62Code9(row['code'])
                name = row['name']
                minute_info = StockMinuteHandler.DownloadStockMinuteInfos(
                    code=code, start_date=start_date, end_date=end_date, fuquan=fuquan)
                table_name = StockMinuteHandler.Save2Database(
                    op=op, code=code, fuquan=fuquan, data=minute_info)
                print('Successfully Download Stock Minute{0} {1} {2} {3}'.format(5, code, name, fuquan.toString()))
                tbls.append(table_name)

        bs.logout()

        return tbls
