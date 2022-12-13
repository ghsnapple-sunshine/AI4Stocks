from buffett.adapter.akshare.lazy import Lazy
from buffett.adapter.pandas import pd, DataFrame
from buffett.adapter.requests import requests


def my_stock_board_concept_n_industry_hist_em(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> DataFrame:
    """
    东方财富网-沪深板块-行业板块-历史行情
    https://quote.eastmoney.com/bk/90.BK1027.html

    :param symbol:          板块代码
    :param period:          choice of {'daily', 'weekly', 'monthly'}
    :param start_date:      开始时间
    :param end_date:        结束时间
    :param adjust:          choice of {'': 不复权, "qfq": 前复权, "hfq": 后复权}
    :return:                历史行情
    """
    period_dict = Lazy.get_period_dict()
    adjust_dict = Lazy.get_adjust_dict()
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"90.{symbol}",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "beg": start_date,
        "end": end_date,
        "smplmt": "10000",
        "lmt": "1000000",
        "_": "1626079488673",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    if temp_df.empty:
        return temp_df
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
    ]
    temp_df = temp_df[
        [
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "振幅",
            "换手率",
        ]
    ]
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    return temp_df
