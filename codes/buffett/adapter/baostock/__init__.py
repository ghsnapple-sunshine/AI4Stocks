from baostock import (
    login as bs_login,
    logout as bs_logout,
    query_history_k_data_plus as bs_query_history_k_data_plus,
    query_money_supply_data_month as bs_query_money_supply_data_month,
    query_stock_basic as bs_query_stock_basic,
    query_trade_dates as bs_query_trade_dates,
)
from baostock.data.resultset import ResultData as bs_ResultData

from buffett.adapter.pandas import DataFrame

# types
ResultData = bs_ResultData


class bs:
    @staticmethod
    def query_history_k_data_plus(
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ):
        bs_login()
        result = bs._resultdata_to_dataframe(
            bs_query_history_k_data_plus(
                code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag,
            )
        )
        bs_logout()
        return result

    @staticmethod
    def query_money_supply_data_month(start_date: str, end_date: str):
        bs_login()
        result = bs._resultdata_to_dataframe(
            bs_query_money_supply_data_month(start_date=start_date, end_date=end_date)
        )
        bs_logout()
        return result

    @staticmethod
    def query_trade_dates(start_date: str, end_date: str):
        bs_login()
        result = bs._resultdata_to_dataframe(
            bs_query_trade_dates(start_date=start_date, end_date=end_date)
        )
        bs_logout()
        return result

    @staticmethod
    def query_stock_basic():
        bs_login()
        result = bs._resultdata_to_dataframe(bs_query_stock_basic())
        bs_logout()
        return result

    @staticmethod
    def _resultdata_to_dataframe(self: ResultData) -> DataFrame:
        """
        将baostock返回的ResultData转换为DataFrame

        :param self:      ResultData
        :return:          DataFrame
        """
        data = []
        while (self.error_code == "0") & self.next():
            # 获取一条记录，将记录合并在一起
            data.append(self.get_row_data())
        return DataFrame(data, columns=self.fields)
