from buffett.adapter.akshare import ak
from buffett.adapter.pendulum import DateTime
from test import Tester
from test.suites.acc import Accelerator


class TestAccelerator(Tester):
    def _setup_oncemore(self):
        pass

    def _setup_always(self) -> None:
        pass

    def _setup_once(cls):
        cls._atom_download()

    def test_repeat_download_stock_list(self):
        start = DateTime.now()
        self._atom_download()
        duration = DateTime.now() - start
        assert duration.total_seconds() < 0.1

    @staticmethod
    def _atom_download():
        ak.stock_info_a_code_name = Accelerator(ak.stock_info_a_code_name).mock()
        for x in range(0, 10):
            res = ak.stock_info_a_code_name()
