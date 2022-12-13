from akshare.utils import demjson

from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.requests import requests


def my_stock_em_pg() -> DataFrame:
    """
    东方财富网-数据中心-新股数据-配股
    https://data.eastmoney.com/xg/pg/

    :return:    配股
    """
    temp_dfs = []
    n = 0
    while True:
        n += 1
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "sortColumns": "EQUITY_RECORD_DATE",
            "sortTypes": "-1",
            "pageSize": "500",
            "pageNumber": f"{n}",
            "reportName": "RPT_IPO_ALLOTMENT",
            "columns": "ALL",
            "quoteColumns": "f2~01~SECURITY_CODE~NEW_PRICE",
            "quoteType": "0",
            "source": "WEB",
            "client": "WEB",
        }
        r = requests.get(url, params=params)
        data_json = demjson.decode(r.text)
        if data_json["result"] is None:
            break
        temp_df = DataFrame(data_json["result"]["data"])
        temp_df.columns = [
            "_",
            "_",
            "股票代码",
            "_",
            "股票简称",
            "配售代码",
            "_",
            "配股比例",
            "配股价",
            "配股前总股本",
            "配股数量",
            "配股后总股本",
            "股权登记日",
            "缴款起始日期",
            "缴款截止日期",
            "上市日",
            "_",
            "_",
            "_",
            "_",
            "_",
            "最新价",
            "_",
        ]
        temp_df = temp_df[
            [
                "股票代码",
                "股票简称",
                "配售代码",
                "配股数量",
                "配股比例",
                "配股价",
                "最新价",
                "配股前总股本",
                "配股后总股本",
                "股权登记日",
                "缴款起始日期",
                "缴款截止日期",
                "上市日",
            ]
        ]
        temp_dfs.append(temp_df)

    temp_df = pd.concat(temp_dfs)
    temp_df["配股比例"] = "10配" + temp_df["配股比例"].apply(lambda x: str(int(x)))
    temp_df["配股数量"] = pd.to_numeric(temp_df["配股数量"])
    temp_df["配股价"] = pd.to_numeric(temp_df["配股价"])
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
    temp_df["配股前总股本"] = pd.to_numeric(temp_df["配股前总股本"])
    temp_df["配股后总股本"] = pd.to_numeric(temp_df["配股后总股本"])
    temp_df["股权登记日"] = pd.to_datetime(temp_df["股权登记日"]).dt.date
    temp_df["缴款起始日期"] = pd.to_datetime(temp_df["缴款起始日期"]).dt.date
    temp_df["缴款截止日期"] = pd.to_datetime(temp_df["缴款截止日期"]).dt.date
    temp_df["上市日"] = pd.to_datetime(temp_df["上市日"]).dt.date
    return temp_df


if __name__ == "__main__":
    stock_em_pg_df = my_stock_em_pg()
    print(stock_em_pg_df)
