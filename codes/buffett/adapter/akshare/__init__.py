from akshare import (
    index_stock_info as ak_index_stock_info,
    stock_board_concept_cons_em as ak_stock_board_concept_cons_em,
    stock_board_concept_name_em as ak_stock_board_concept_name_em,
    stock_board_industry_cons_em as ak_stock_board_industry_cons_em,
    stock_board_industry_name_em as ak_stock_board_industry_name_em,
    stock_fhps_em as ak_stock_fhps_em,
    stock_info_a_code_name as ak_stock_info_a_code_name,
    stock_profit_forecast as ak_stock_profit_forecast,
    stock_zh_a_hist_min_em as ak_stock_zh_a_hist_min_em,
    stock_zh_a_spot_em as ak_stock_zh_a_spot_em,
)

from buffett.adapter.akshare.concept_industry_em import (
    my_stock_board_concept_n_industry_hist_em,
)
from buffett.adapter.akshare.index_em import my_stock_zh_index_daily_em
from buffett.adapter.akshare.stock_em import my_stock_zh_a_hist
from buffett.adapter.akshare.stock_fhpg_em import stock_fhpg_em
from buffett.adapter.akshare.stock_list_lg import my_stock_a_lg_indicator
from buffett.adapter.akshare.stock_th import my_stock_zh_a_hist_ths


class ak:
    @staticmethod
    def index_stock_info():
        return ak_index_stock_info()

    @staticmethod
    def stock_board_concept_cons_em(symbol: str):
        return ak_stock_board_concept_cons_em(symbol)

    @staticmethod
    def stock_board_concept_name_em():
        return ak_stock_board_concept_name_em()

    @staticmethod
    def stock_board_industry_cons_em(symbol: str):
        return ak_stock_board_industry_cons_em(symbol)

    @staticmethod
    def stock_board_concept_n_industry_hist_em(
        symbol: str, period: str, start_date: str, end_date: str, adjust: str
    ):
        return my_stock_board_concept_n_industry_hist_em(
            symbol=symbol,
            period=period,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

    @staticmethod
    def stock_board_industry_name_em():
        return ak_stock_board_industry_name_em()

    @staticmethod
    def stock_fhps_em(date: str):
        return ak_stock_fhps_em(date)

    @staticmethod
    def stock_info_a_code_name():
        return ak_stock_info_a_code_name()

    @staticmethod
    def stock_profit_forecast():
        return ak_stock_profit_forecast()

    @staticmethod
    def stock_zh_a_hist(
        symbol: str, period: str, start_date: str, end_date: str, adjust: str
    ):
        return my_stock_zh_a_hist(
            symbol=symbol,
            period=period,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

    @staticmethod
    def stock_zh_a_hist_min_em(
        symbol: str, period: str, start_date: str, end_date: str, adjust: str
    ):
        return ak_stock_zh_a_hist_min_em(
            symbol=symbol,
            period=period,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

    @staticmethod
    def stock_zh_a_spot_em():
        return ak_stock_zh_a_spot_em()

    @staticmethod
    def stock_a_lg_indicator(symbol: str):
        return my_stock_a_lg_indicator(symbol=symbol)

    @staticmethod
    def stock_zh_index_daily_em(symbol: str, start_date: str, end_date: str):
        return my_stock_zh_index_daily_em(
            symbol=symbol, start_date=start_date, end_date=end_date
        )

    @staticmethod
    def stock_zh_a_fhpg_em(symbol: str):
        return stock_fhpg_em(symbol=symbol)

    @staticmethod
    def stock_zh_a_hist_ths(symbol: str, adjust: str):
        """
        ???????????????????????????

        :param symbol:      ????????????
        :param adjust:      choice of {"00", "01", "02"}
                            "00": ?????????,  "01": ???????????? ???02": ?????????
        :return:
        """
        return my_stock_zh_a_hist_ths(symbol=symbol, adjust=adjust)
