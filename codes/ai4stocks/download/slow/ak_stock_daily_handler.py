import akshare as ak
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common import META_COLS, COL_DATE as DATE, StockCode as Code, FuquanType as FType
from ai4stocks.common.pendelum import to_my_date
from ai4stocks.download.connect import MysqlColType, MysqlColAddReq, MysqlOperator
from ai4stocks.download.slow.slow_handler_base import SlowHandlerBase


class AkStockDailyHandler(SlowHandlerBase):
    def __init__(self, operator: MysqlOperator):
        super().__init__(operator=operator)
        self.fuquans = [FType.NONE, FType.QIANFUQIAN, FType.HOUFUQIAN]

    """
    def download_and_save(
            self,
            start_time: DateTime,
            end_time: DateTime
    ) -> list:
        return super().download_and_save(
            start_time=start_time,
            end_time=end_time
        )

    def get_table(
            self,
            code: StockCode,
            fuquan: FuquanType
    ) -> DataFrame:
        return super().get_table(
            code=code,
            fuquan=fuquan
        )
    """

    def __download__(
            self,
            code: Code,
            fuquan: FType,
            start_time: DateTime,
            end_time: DateTime
    ) -> DataFrame:
        # 使用接口（stock_zh_a_hist，源：东财）,code为Str6
        # 备用接口（stock_zh_a_daily，源：新浪，未实现）
        start_time = start_time.format('YYYYMMDD')
        end_time = end_time.format('YYYYMMDD')
        daily_info = ak.stock_zh_a_hist(
            symbol=code.to_code6(),
            period='daily',
            start_date=start_time,
            end_date=end_time,
            adjust=fuquan.to_req()
        )

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
        return daily_info

    def __save_to_database__(
            self,
            name: str,
            data: DataFrame
    ) -> None:
        if (not isinstance(data, DataFrame)) or data.empty:
            return

        cols = [
            ['date', MysqlColType.DATE, MysqlColAddReq.KEY],
            ['open', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['close', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['high', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['low', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['chengjiaoliang', MysqlColType.INT32, MysqlColAddReq.NONE],
            ['chengjiaoe', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['zhenfu', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['zhangdiefu', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['zhangdiee', MysqlColType.FLOAT, MysqlColAddReq.NONE],
            ['huanshoulv', MysqlColType.FLOAT, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(data=cols, columns=META_COLS)
        self.operator.create_table(name, table_meta, if_not_exist=True)
        self.operator.insert_data(name, data)

    def get_table(self, code: Code, fuquan: FType) -> DataFrame:
        """
        查询某支股票以某种复权方式的全部数据

        :param code:        股票代码
        :param fuquan:      股票复权方式
        :return:
        """
        tbl = super().get_table(code=code, fuquan=fuquan)
        tbl[DATE] = tbl[DATE].apply(lambda x: to_my_date(x))
        tbl.index = tbl[DATE]
        del tbl[DATE]
        return tbl
