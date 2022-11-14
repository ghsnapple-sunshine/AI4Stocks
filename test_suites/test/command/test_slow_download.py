from buffett.adapter import logging
from buffett.common.pendelum import DateTime, Date
from buffett.common.wrapper import Wrapper
from buffett.constants.magic import empty_init
from buffett.download import Para
from buffett.download.slow import AkDailyHandler, BsMinuteHandler
from buffett.task import StockDailyTask, StockReformTask, StockMinuteTask
from test import Tester, create_2stocks


class SlowDownloadTest(Tester):
    def test_download(self):
        """
        download的镜像测试

        :return:
        """
        # 初始化StockList
        create_2stocks(operator=self.operator)
        logging.basicConfig(level=logging.INFO)
        now = DateTime.now()
        operator = self.operator
        # 定义动态类
        StockDailyTaskSubClass = type(
            'StockDailyTaskSubClass',
            (StockDailyTask,),
            {'__init__': empty_init,
             '_operator': operator,
             '_wrapper': Wrapper(AkDailyHandler(operator=operator).obtain_data),
             '_args': (Para().with_start_n_end(start=Date(2020, 1, 1), end=Date(2022, 11, 1)),),
             '_kwargs': {},
             '_start_time': DateTime.now(),
             '_task_id': None})
        StockMinuteTaskSubClass = type(
            'StockMinuteTaskSubClass',
            (StockMinuteTask,),
            {'__init__': empty_init,
             '_operator': operator,
             '_wrapper': Wrapper(BsMinuteHandler(operator=operator).obtain_data),
             '_args': (Para().with_start_n_end(start=Date(2020, 1, 1), end=Date(2022, 11, 1)),),
             '_kwargs': {},
             '_start_time': DateTime.now(),
             '_task_id': None})

        tasks = [StockDailyTaskSubClass(),
                 StockMinuteTaskSubClass(),
                 StockReformTask(operator=operator, start_time=now.add(seconds=3))]
        for task in tasks:
            task.run()

        assert self.operator.select_row_num('dc_stock_dayinfo_000001_') == 684
        assert self.operator.select_row_num('dc_stock_dayinfo_000001_qfq') == 684
        assert self.operator.select_row_num('dc_stock_dayinfo_000001_hfq') == 684
        assert self.operator.select_row_num('dc_stock_dayinfo_600000_') == 684
        assert self.operator.select_row_num('dc_stock_dayinfo_600000_qfq') == 684
        assert self.operator.select_row_num('dc_stock_dayinfo_600000_hfq') == 684
        assert self.operator.select_row_num('dc_stock_dayinfo_600000_hfq') == 684
        assert self.operator.select_row_num('bs_stock_min5info_000001_') == 684 * 240 / 5
        assert self.operator.select_row_num('bs_stock_min5info_600000_') == 684 * 240 / 5
