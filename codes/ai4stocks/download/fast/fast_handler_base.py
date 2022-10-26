from pandas import DataFrame

from ai4stocks.download.connect import MysqlOperator
from ai4stocks.download.handler_base import HandlerBase as Hdl, HandlerParameter as Para


# FastHandler:  实现单张表的下载，存储
# SlowHandler： 实现多张表的下载，存储
class FastHandlerBase(Hdl):
    """
    def __init__(self, op: MysqlOperator):
        super(FastHandlerBase, self).__init__(op)
    """

# region 公有方法
    def download_and_save(self) -> DataFrame:
        df = self.__download__()
        self.__save_to_database__(df)
        return df

    def obtain(self, para: Para):
        return self.download_and_save()

    def get_table(self) -> DataFrame:
        pass

    def search(self, para: Para) -> DataFrame:
        return self.get_table()
# endregion

# region 私有方法
    def __download__(self) -> DataFrame:
        pass

    def __save_to_database__(self, df: DataFrame) -> None:
        pass
# endregion
