from typing import Optional

from pandas import (
    DataFrame as pd_DataFrame,
    Series as pd_Series,
    concat as pd_concat,
    isna as pd_isna,
    notna as pd_notna,
    NaT as pd_NAT,
    merge as pd_merge,
    to_datetime as pd_to_datetime,
    to_numeric as pd_to_numeric,
    read_csv as pd_read_csv,
    read_feather as pd_read_feather,
    Timestamp as pd_Timestamp,
    Timedelta as pd_Timedelta,
)

# constants

# types
DataFrame = pd_DataFrame
Timestamp = pd_Timestamp
Timedelta = pd_Timedelta
Series = pd_Series


class pd:
    # methods
    concat = pd_concat
    isna = pd_isna
    notna = pd_notna
    NAT = pd_NAT
    merge = pd_merge
    read_csv = pd_read_csv
    read_feather = pd_read_feather
    to_datetime = pd_to_datetime
    to_numeric = pd_to_numeric

    # new_add_methods
    @staticmethod
    def subtract(df1: DataFrame, df2: DataFrame) -> DataFrame:
        """
        df1 - df2

        :param df1:
        :param df2:
        :return:
        """
        return pd.concat([df1, df2, df2]).drop_duplicates(keep=False)

    @staticmethod
    def concat_safe(datas: list[Optional[DataFrame]]) -> Optional[DataFrame]:
        """
        安全地concat DataFrame

        :return:
        """
        if any(isinstance(x, DataFrame) and not x.empty for x in datas):
            return pd_concat(datas)
        return None
