from buffett.common.magic import empty_init
from buffett.common.pendulum import DateTime
from buffett.common.wrapper import Wrapper
from buffett.download.handler.stock import AkDailyHandler, BsMinuteHandler
from buffett.task import StockDailyTask, StockReformTask, StockMinuteTask
from test import Tester, create_2stocks


class SlowDownloadTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        # 定义动态类
        cls.StockDailyTaskSubClass = type(
            "StockDailyTaskSubClass",
            (StockDailyTask,),
            {
                "__init__": empty_init,
                "_operator": cls._operator,
                "_wrapper": Wrapper(AkDailyHandler(operator=cls._operator).obtain_data),
                "_args": (cls._long_para,),
                "_kwargs": {},
                "_start_time": DateTime.now(),
                "_task_id": None,
            },
        )
        cls.StockMinuteTaskSubClass = type(
            "StockMinuteTaskSubClass",
            (StockMinuteTask,),
            {
                "__init__": empty_init,
                "_operator": cls._operator,
                "_wrapper": Wrapper(
                    BsMinuteHandler(operator=cls._operator).obtain_data
                ),
                "_args": (cls._long_para,),
                "_kwargs": {},
                "_start_time": DateTime.now(),
                "_task_id": None,
            },
        )

    def _setup_always(self) -> None:
        # 初始化StockList
        create_2stocks(operator=self._operator)

    def test_download(self):
        """
        download的镜像测试

        :return:
        """
        now = DateTime.now()
        operator = self._operator

        tasks = [
            self.StockDailyTaskSubClass(),
            self.StockMinuteTaskSubClass(),
            StockReformTask(operator=operator, start_time=now.add(seconds=3)),
        ]
        for task in tasks:
            task.run()

        assert operator.select_row_num("dc_stock_dayinfo_000001_") == 293
        assert operator.select_row_num("dc_stock_dayinfo_000001_qfq") == 293
        assert operator.select_row_num("dc_stock_dayinfo_000001_hfq") == 293
        assert operator.select_row_num("dc_stock_dayinfo_600000_") == 293
        assert operator.select_row_num("dc_stock_dayinfo_600000_qfq") == 293
        assert operator.select_row_num("dc_stock_dayinfo_600000_hfq") == 293
        assert operator.select_row_num("bs_stock_min5info_000001_") == 293 * 240 / 5
        assert operator.select_row_num("bs_stock_min5info_600000_") == 293 * 240 / 5
