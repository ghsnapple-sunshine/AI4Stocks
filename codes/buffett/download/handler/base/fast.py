from abc import abstractmethod
from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.common.tools import dataframe_is_valid
from buffett.download import Para
from buffett.download.handler import Handler


class FastHandler(Handler):
    """
    FastHandler:实现单张表的下载，存储
    """

    # region 公有方法
    def obtain_data(self) -> Optional[DataFrame]:
        df = self._download()
        if dataframe_is_valid(df):
            self._save_to_database(df)
        return df

    @abstractmethod
    def select_data(self, *args, **kwargs) -> Optional[DataFrame]:
        pass

    # endregion

    # region 私有方法
    @abstractmethod
    def _download(self) -> Optional[DataFrame]:
        pass

    @abstractmethod
    def _save_to_database(self, df: DataFrame) -> None:
        pass

    # endregion
