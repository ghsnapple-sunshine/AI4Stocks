import json

from buffett.adapter.error.data_source import DataSourceError
from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame
from buffett.adapter.request import ProxyRequests
from buffett.common.logger import Logger

request = ProxyRequests("tonghuashun")


def my_stock_zh_a_hist_ths(symbol: str, adjust: str) -> DataFrame:

    url = f"https://d.10jqka.com.cn/v6/line/hs_{symbol}/{adjust}/all.js"
    response = request.get(url=url, proxy="random", timeout=5)
    if response.status_code == 200:
        data_text = response.text
        data_json = json.loads(data_text[data_text.find("{") : data_text.find("}") + 1])
        price = json.loads("[" + data_json["price"] + "]")
        total = int(data_json["total"])
        years = np.concatenate(
            [np.array([str(x[0])] * x[1]) for x in data_json["sortYear"]]
        )
        dates = data_json["dates"].split(",")
        if len(years) == len(dates) == total:
            full_dates = np.concatenate([years, dates]).reshape((total, 2), order="F")
        elif len(years) != total and len(dates) == total:
            Logger.warning(
                f"Unexpected result in download {symbol}, got years {len(years)} and dates {len(dates)}"
            )
            if len(years) < total:
                full_dates = np.concatenate(
                    [years, [years[-1]] * (len(dates) - len(years)), dates]
                ).reshape((total, 2), order="F")
            else:
                full_dates = np.concatenate([years[:total], dates]).reshape(
                    (total, 2), order="F"
                )
        else:
            raise NotImplemented
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
    raise DataSourceError("th", symbol)
