import json

from buffett.adapter.error.data_source import DataSourceError
from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame
from buffett.adapter.requests import ProxyRequests


def my_stock_zh_a_hist_ths(
    symbol: str,
    adjust: str,
) -> DataFrame:

    url = f"https://d.10jqka.com.cn/v6/line/hs_{symbol}/{adjust}/all.js"
    r = ProxyRequests.get(url=url, proxy="random", timeout=5)
    if r.status_code == 200:
        data_text = r.text
        data_json = json.loads(data_text[data_text.find("{") : data_text.find("}") + 1])
        price = json.loads("[" + data_json["price"] + "]")
        total = int(data_json["total"])
        years = np.concatenate(
            [np.array([str(x[0])] * x[1]) for x in data_json["sortYear"]]
        )
        dates = data_json["dates"].split(",")
        full_dates = np.concatenate([years, dates]).reshape((total, 2), order="F")
        full_dates = np.vectorize(str.__add__)(full_dates[:, 0], full_dates[:, 1])
        price = np.array(price).reshape((total, 4))
        price[:, 1] = price[:, 1] + price[:, 0]
        price[:, 2] = price[:, 2] + price[:, 0]
        price[:, 3] = price[:, 3] + price[:, 0]
        price = price / 100
        volume = json.loads("[" + data_json["volumn"] + "]")
        df = DataFrame(
            {
                "date": full_dates,
                "low": price[:, 0],
                "open": price[:, 1],
                "high": price[:, 2],
                "close": price[:, 3],
                "chengjiaoliang": volume,
            }
        )
        return df
    raise DataSourceError("ths", symbol)


if __name__ == "__main__":
    ProxyRequests.load(file_name="tonghuashun")
    df = my_stock_zh_a_hist_ths(symbol="000001", adjust="00")
    ProxyRequests.save(file_name="tonghuashun")
    print(df)
