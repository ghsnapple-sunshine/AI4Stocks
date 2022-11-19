from buffett.common.pendelum import DateTime
from buffett.task import StockListTask
from test import Tester


class FastDownloadTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_download_fast(self):
        """
        download的镜像测试（股票清单、不循环）

        :return:
        """
        now = DateTime.now()
        tasks = [StockListTask(operator=self._operator, start_time=now)]
        for task in tasks:
            task.run()
