from akshare.utils import demjson

from buffett.adapter.error.data_source import DataSourceError
from buffett.adapter.pandas import DataFrame
from buffett.adapter.requests import requests


def stock_fhpg_em(symbol: str):
    """
    https://push2.eastmoney.com/api/qt/stock/cqcx/get?id=SH600702
    https://quote.eastmoney.com/concept/sh600702.html#fschart-r

    :param symbol:      股票代码
    :return:
    """
    url = "https://push2.eastmoney.com/api/qt/stock/cqcx/get"
    params = {"id": symbol}
    r = requests.get(url, params=params)
    data_text = r.text
    data_json = demjson.decode(data_text)
    if data_json["data"] is None:
        raise DataSourceError(source="dc_fhpg", target=symbol)

    df = DataFrame(data_json["data"]["records"])
    """
    字段说明：
    date:   日期
    type:   choices of {1,2,8,16}  1: 派息; 2: 送股; 8: 配股; 16: 增发
    pxbl:   派息比例
    sgbl:   送股比例
    cxbl:   ?
    pgbl:   配股比例
    pgjg:   配股价格
    pghg:   ?
    zfbl:   增发比例
    zfgs:   增发股数(万股)
    zfjg:   增发价格
    ggflag: ?
    zzbl:   ?
    
    """
    return df


if __name__ == "__main__":
    stock_fhpg_em("SH600702")
