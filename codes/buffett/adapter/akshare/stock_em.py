from buffett.adapter.akshare.lazy import Lazy
from buffett.adapter.error.data_source import DataSourceError
from buffett.adapter.pandas import pd, DataFrame
from buffett.adapter.requests import Requests


def my_stock_zh_a_hist(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic

    :param symbol:          股票代码
    :param period:          choice of {'daily', 'weekly', 'monthly'}
    :param start_date:      开始日期
    :param end_date:        结束日期
    :param adjust:          choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :return:
    """
    code_id_dict = Lazy.get_code_id_dict()
    period_dict = Lazy.get_period_dict()
    adjust_dict = Lazy.get_adjust_dict()

    if symbol not in code_id_dict:
        raise DataSourceError(source="dc_stock", target=symbol)

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
    r = Requests.get(url, params=params)
    data_json = r.json()
    if not (data_json["data"] and data_json["data"]["klines"]):
        return DataFrame()
    temp_df = DataFrame([item.split(",") for item in data_json["data"]["klines"]])
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
    temp_df.index = pd.to_datetime(temp_df["日期"])
    temp_df.reset_index(inplace=True, drop=True)

    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
    temp_df["最高"] = pd.to_numeric(temp_df["最高"])
    temp_df["最低"] = pd.to_numeric(temp_df["最低"])
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])

    return temp_df
