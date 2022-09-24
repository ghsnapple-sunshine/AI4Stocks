import akshare as ak
from pandas import DataFrame

from ai4stocks.common.common import FuquanType
from ai4stocks.download.data_connect.mysql_common import MysqlColType, MysqlColAddReq, MysqlConstants
from ai4stocks.download.data_connect.mysql_operator import MysqlOperator
from pendulum import DateTime


class StockMinuteHandler:
    @staticmethod
    def DownloadStockMinuteInfos(code: str, start_date: DateTime, end_date: DateTime, fuquan: FuquanType) -> DataFrame:
        # 使用接口（stock_zh_a_hist_min_em，源：东财）,code为Str6
        start_date = start_date.format('YYYY-MM-DD HH:mm:ss')
        end_date = end_date.format('YYYY-MM-DD HH:mm:ss')
        minute_info = ak.stock_zh_a_hist_min_em(symbol=code, period='5', start_date=start_date, end_date=end_date,
                                                adjust=fuquan.toString())

        # 重命名
        MINUTE_NAME_DICT = {'时间': 'datetime',
                           '开盘': 'open',
                           '收盘': 'close',
                           '最高': 'high',
                           '最低': 'low',
                           '成交量': 'chengjiaoliang',
                           '成交额': 'chengjiaoe',
                           '振幅': 'zhenfu',
                           '涨跌幅': 'zhangdiefu',
                           '涨跌额': 'zhangdiee',
                           '换手率': 'huanshoulv'}
        minute_info.rename(columns=MINUTE_NAME_DICT, inplace=True)
        return minute_info

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
            ['zhenfu', MysqlColType.Float, MysqlColAddReq.NONE],
            ['zhangdiefu', MysqlColType.Float, MysqlColAddReq.NONE],
            ['zhangdiee', MysqlColType.Float, MysqlColAddReq.NONE],
            ['huanshoulv', MysqlColType.Float, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=MysqlConstants.META_COLS)
        table_name = MysqlConstants.MINUTE_INFO_TABLE.format(5, code, fuquan.toString()) # 目前仅下载5分钟数据
        op.CreateTable(table_name, table_meta)
        op.InsertData(table_name, data)
        op.Disconnect()
        return table_name

    @staticmethod
    def DownloadAndSave(op: MysqlOperator, start_date: DateTime, end_date: DateTime) -> list:
        stocks = op.GetTable(MysqlConstants.STOCK_LIST_TABLE)

        tbls = []
        FUQUANS = [FuquanType.HOUFUQIAN, FuquanType.QIANFUQIAN]
        for index, row in stocks.iterrows():
            for fuquan in FUQUANS:
                code = row['code']
                name = row['name']
                minute_info = StockMinuteHandler.DownloadStockMinuteInfos(code=code, start_date=start_date,
                                                                          end_date=end_date, fuquan=fuquan)
                table_name = StockMinuteHandler.Save2Database(op=op, code=code, fuquan=fuquan, data=minute_info)
                print('Successfully Download Stock Minute{0} {1} {2} {3}'.format(5, code, name, fuquan.toString()))
                tbls.append(table_name)

        return tbls
