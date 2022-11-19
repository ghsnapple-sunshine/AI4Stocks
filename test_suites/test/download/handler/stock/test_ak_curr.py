from buffett.download.handler.stock import AkCurrHandler
from test import Tester


class TestStockCurrHandler(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkCurrHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        tbl = self._hdl.obtain_data()
        db = self._hdl.select_data()
        # cmp = pd.concat([tbl, db]).drop_duplicates(keep=False)  # error: 允许存入数据库后存在精度损失
        assert tbl.shape[0] == db.shape[0]
