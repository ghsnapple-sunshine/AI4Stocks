from baostock import (
    login as bs_login,
    logout as bs_logout,
    query_history_k_data_plus as bs_query_history_k_data_plus,
    query_money_supply_data_month as bs_query_money_supply_data_month,
    query_trade_dates as bs_query_trade_dates,
)
from baostock.data.resultset import ResultData as bs_ResultData

# types
ResultData = bs_ResultData


class bs:
    login = bs_login
    logout = bs_logout
    query_history_k_data_plus = bs_query_history_k_data_plus
    query_money_supply_data_month = bs_query_money_supply_data_month
    query_trade_dates = bs_query_trade_dates
