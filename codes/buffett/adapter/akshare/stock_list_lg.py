from akshare.stock_feature.stock_a_indicator import get_token_lg
from bs4 import BeautifulSoup

from buffett.adapter.pandas import pd, DataFrame
from buffett.adapter.request import Requests


def my_stock_a_lg_indicator(symbol: str):
    """
    解决akshare中stock_a_lg_indicator的问题。

    :param symbol:
    :return:
    """
    if symbol == "all":
        url = "https://www.legulegu.com/stocklist"
        r = Requests.get(url, verify=False)
        soup = BeautifulSoup(r.text, "lxml")
        node_list = soup.find_all(attrs={"class": "col-xs-6"})
        href_list = [item.find("a")["href"] for item in node_list]
        title_list = [item.find("a")["title"] for item in node_list]
        temp_df = DataFrame([title_list, href_list]).T
        temp_df.columns = ["stock_name", "short_url"]
        temp_df["code"] = temp_df["short_url"].str.split("/", expand=True).iloc[:, -1]
        temp_df = temp_df[["code", "stock_name"]]
        return temp_df
    else:
        url = f"https://www.legulegu.com/api/s/base-info/"
        token = get_token_lg()
        params = {"id": symbol, "token": token}
        r = Requests.get(url, params=params, verify=False)
        temp_json = r.json()
        temp_df = DataFrame(
            temp_json["data"]["items"], columns=temp_json["data"]["fields"]
        )
        temp_df["trade_date"] = pd.to_datetime(temp_df["trade_date"]).dt.date
        temp_df[temp_df.columns[1:]] = temp_df[temp_df.columns[1:]].astype(float)
        temp_df.sort_values(["trade_date"], inplace=True, ignore_index=True)
        return temp_df


if __name__ == "__main__":
    my_stock_a_lg_indicator("all")
    my_stock_a_lg_indicator("000001")
