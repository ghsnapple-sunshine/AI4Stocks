from buffett.download.handler.index import IndexListHandler
from test import Tester, DbSweeper


class TestIndexListHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        DbSweeper.cleanup()

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        hdl = IndexListHandler(operator=self._operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        assert self.compare_dataframe(df1, df2)

    def _repeat_download(self):
        # 测试重复下载不报错
        IndexListHandler(operator=self._operator).obtain_data()
