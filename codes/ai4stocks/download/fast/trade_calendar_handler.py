import logging

import baostock as bs
from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common import COL_DATE as DATE, META_COLS, TRADE_CALENDAR_TABLE
from ai4stocks.common.pendelum import to_my_date
from ai4stocks.download.connect import MysqlColType, MysqlColAddReq, MysqlOperator as Operator
from ai4stocks.download.fast.fast_handler_base import FastHandlerBase


# 交易日查询
class TradeCalendarHandler(FastHandlerBase):
    def __init__(self, operator: Operator):
        super().__init__(operator)

    """
    def download_and_save(self) -> DataFrame:
        return super().download_and_save()
    """

    def __download__(self) -> DataFrame:
        #### 登陆系统 ####
        lg = bs.login()
        # 显示登陆返回信息
        logging.info('login respond error_code:' + lg.error_code)
        logging.info('login respond  error_msg:' + lg.error_msg)

        #### 获取交易日信息 ####
        rs = bs.query_trade_dates(
            start_date='2001-01-01',
            end_date=DateTime.now().format('YYYY-MM-DD'))
        # logging.info('query_trade_dates respond error_code:' + rs.error_code)
        # logging.info('query_trade_dates respond  error_msg:' + rs.error_msg)

        #### 打印结果集 ####
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        res = DataFrame(data_list, columns=rs.fields)

        # 仅保留交易日
        res = res[res['is_trading_day'] == '1']
        RENAME_DICT = {'calendar_date': DATE}
        res.rename(columns=RENAME_DICT, inplace=True)
        res = res[['date']]

        return res

    def __save_to_database__(self, df: DataFrame):
        cols = [
            [DATE, MysqlColType.DATE, MysqlColAddReq.NONE]
        ]
        table_meta = DataFrame(
            data=cols,
            columns=META_COLS)
        self.operator.create_table(TRADE_CALENDAR_TABLE, table_meta)
        self.operator.try_insert_data(TRADE_CALENDAR_TABLE, df)  # 忽略重复Insert
        self.operator.disconnect()

    def get_table(self) -> DataFrame:
        df = self.operator.get_table(TRADE_CALENDAR_TABLE)
        df[DATE] = df[DATE].apply(lambda x: to_my_date(x))
        df.index = df[DATE]
        return df
