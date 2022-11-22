from pandas import (
    DataFrame as pd_DataFrame,
    Series as pd_Series,
    concat as pd_concat,
    isna as pd_isna,
    merge as pd_merge,
    to_datetime as pd_to_datetime,
)

# constants

# types
DataFrame = pd_DataFrame
Series = pd_Series


class pd:
    # methods
    concat = pd_concat
    isna = pd_isna
    merge = pd_merge
    to_datetime = pd_to_datetime

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
