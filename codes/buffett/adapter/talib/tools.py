from typing import Callable, Union

from buffett.adapter.numpy import ndarray, np
from buffett.adapter.pandas import DataFrame, Series
from buffett.common.constants.col import OPEN, HIGH, LOW, CLOSE


def dataframe_to_ndarraydict(df: DataFrame) -> dict[str, ndarray]:
    dic = df.to_dict(orient="list")
    dic = dict((k, np.array(v)) for k, v in dic.items())
    return dic


def call_talib_cdl(
    func: Callable, name: str, data: Union[DataFrame, dict[str, ndarray]], **kwargs
) -> Series:
    if isinstance(data, DataFrame):
        dic = dataframe_to_ndarraydict(data)
        result = func(dic[OPEN], dic[HIGH], dic[LOW], dic[CLOSE], **kwargs)
        return Series(result, name=name, index=data.index)
    else:
        result = func(data[OPEN], data[HIGH], data[LOW], data[CLOSE], **kwargs)
        return Series(result, name=name)
