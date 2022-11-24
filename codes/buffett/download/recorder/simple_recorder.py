from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import FREQ, FUQUAN, SOURCE
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.types import FreqType, FuquanType, SourceType


class SimpleRecorder:
    def __init__(self, operator: Operator, table_name: str, meta: DataFrame):
        self._operator = operator
        self._exist = False
        self._TABLE_NAME = table_name
        self._META = meta

    def save_to_database(self, df: DataFrame) -> None:
        """
        基于DataFrame保存

        :param df:
        :return:
        """
        if not self._exist:
            self._operator.create_table(
                name=self._TABLE_NAME, meta=self._META, if_not_exist=True
            )
            self._exist = True
        self._operator.try_insert_data(
            name=self._TABLE_NAME, meta=self._META, df=df, update=True
        )  # 如果原纪录已存在，则更新

    def select_data(self, *args, **kwargs) -> Optional[DataFrame]:
        """
        获取Recorder所有记录

        :return:
        """
        df = self._operator.select_data(self._TABLE_NAME)
        if dataframe_not_valid(df):
            return
        df[FREQ] = df[FREQ].apply(lambda x: FreqType(x))
        df[FUQUAN] = df[FUQUAN].apply(lambda x: FuquanType(x))
        df[SOURCE] = df[SOURCE].apply(lambda x: SourceType(x))
        return df
