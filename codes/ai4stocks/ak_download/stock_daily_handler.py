import akshare as ak
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.ak_download.common import FuquanType
from ai4stocks.data_connect.mysql_common import MysqlColType, MysqlColAddReq, MysqlConstants
from ai4stocks.data_connect.mysql_operator import MysqlOperator


class StockDailyHandler:
    @staticmethod
    def DownloadStockDailyInfos(code: str, start_date: DateTime, end_date: DateTime, fuquan: FuquanType):
        # 使用接口（stock_zh_a_hist）,code为Str6
        # 备用接口（stock_zh_a_daily，源：新浪，未实现）
        start_date = start_date.format('YYYYMMDD')
        end_date = end_date.format('YYYYMMDD')
        daily_info = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=start_date, end_date=end_date,
                                        adjust=fuquan.toString())

        # 重命名
        DAILY_NAME_DICT = {'日期': 'date',
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
        daily_info.rename(columns=DAILY_NAME_DICT, inplace=True)
        # 新增一列code
        daily_info['code'] = code
        return daily_info

    @staticmethod
    def Save2Database(op: MysqlOperator, code: str, fuquan: FuquanType, data: DataFrame):
        cols = [
            ['date', MysqlColType.DATE, MysqlColAddReq.PRIMKEY],
            ['code', MysqlColType.STOCK_CODE, MysqlColAddReq.NONE],
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
        table_name = MysqlConstants.DAILY_INFO_TABLE.format(code, fuquan.toString())
        op.CreateTable(table_name, table_meta)
        op.InsertData(table_name, data)
        op.Disconnect()
        return table_name

    @staticmethod
    def DownAndSave(op: MysqlOperator):
        start_date = DateTime(2010, 1, 1)
        end_date = DateTime(2022, 6, 30)

        tbl = op.GetTable(MysqlConstants.STOCK_LIST_TABLE)

        tbls = []
        FUQUANS = [FuquanType.HOUFUQIAN, FuquanType.QIANFUQIAN]
        for index, row in tbl.iterrows():
            for fuquan in FUQUANS:
                code = row['code']
                name = row['name']
                daily_info = StockDailyHandler.DownloadStockDailyInfos(code=code, start_date=start_date,
                                                                       end_date=end_date, fuquan=fuquan)
                table_name = StockDailyHandler.Save2Database(op=op, code=code, fuquan=fuquan, data=daily_info)
                print('Successfully Download Stock {0} {1} {2}'.format(code, name, fuquan))
                tbls.append(table_name)

        return tbls