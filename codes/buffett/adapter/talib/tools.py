from typing import Callable

from buffett.adapter.numpy import ndarray, np
from buffett.adapter.pandas import DataFrame, Series
from buffett.common.constants.col import OPEN, HIGH, LOW, CLOSE


class Tools:
    _cache_objid = None
    _cache_output = None

    @classmethod
    def dataframe_to_ndarraydict(cls, df: DataFrame) -> dict[str, ndarray]:
        dic = df.to_dict(orient="list")
        dic = dict((k, np.array(v)) for k, v in dic.items())
        return dic

    @classmethod
    def dataframe_to_ndarraydict_with_cache(cls, df: DataFrame) -> dict[str, ndarray]:
        if id(df) == cls._cache_objid:
            return cls._cache_output
        dic = cls.dataframe_to_ndarraydict(df)
        cls._cache_objid = id(df)
        cls._cache_output = dic
        return dic

    @classmethod
    def call_talib_cdl(
        cls, func: Callable, name: str, df: DataFrame, **kwargs
    ) -> Series:
        dic = cls.dataframe_to_ndarraydict_with_cache(df)
        result = func(dic[OPEN], dic[HIGH], dic[LOW], dic[CLOSE], **kwargs)
        result = Series(result, name=name, index=df.index)
        return result
