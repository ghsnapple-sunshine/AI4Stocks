from abc import abstractmethod

from pandas import DataFrame

from buffett.download.handler import Handler, Para


# FastHandler:
# SlowHandler： 实现多张表的下载，存储
class FastHandler(Handler):
    """
    FastHandler:实现单张表的下载，存储
    """

    # region 公有方法
    def obtain_data(self, para: Para = None) -> DataFrame:
        df = self._download()
        self._save_to_database(df)
        return df

    @abstractmethod
    def get_data(self, para: Para = None) -> DataFrame:
        pass

    # endregion

    # region 私有方法
    @abstractmethod
    def _download(self) -> DataFrame:
        pass

    @abstractmethod
    def _save_to_database(self, df: DataFrame) -> None:
        pass
# endregion
