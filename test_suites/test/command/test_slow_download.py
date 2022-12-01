from buffett.adapter.pendulum import DateTime
from buffett.common.magic import empty_init
from buffett.common.wrapper import Wrapper
from buffett.download.handler.industry import DcIndustryDailyHandler
from buffett.download.handler.reform import ReformHandler
from buffett.download.handler.stock import (
    DcDailyHandler,
    BsDailyHandler,
    BsMinuteHandler,
)
from buffett.task import (
    BsStockDailyTask,
    DcStockDailyTask,
    StockReformTask,
    StockMinuteTask,
    IndustryDailyTask,
)
from test import Tester, create_1stock, create_1industry
from test.command.tools import create_task_no_subsequent_n_shorter_span


class TestSlowDownload(Tester):
    @classmethod
    def _get_cls_dict(cls, func):
        return {
            "__init__": empty_init,
            "_operator": cls._operator,
            "_wrapper": Wrapper(func),
            "_args": (cls._long_para,),
            "_kwargs": {},
            "_start_time": DateTime.now(),
            "_task_id": None,
        }

    @classmethod
    def _setup_oncemore(cls):
        # 初始化StockList, IndustryList
        create_1stock(operator=cls._operator)
        create_1stock(operator=cls._operator, is_sse=False)
        create_1industry(operator=cls._operator)
        # 定义动态类
        cls._task_cls = [
            create_task_no_subsequent_n_shorter_span(
                TaskCls=DcStockDailyTask, HandlerCls=DcDailyHandler, para=cls._long_para
            ),
            create_task_no_subsequent_n_shorter_span(
                TaskCls=BsStockDailyTask, HandlerCls=BsDailyHandler, para=cls._long_para
            ),
            create_task_no_subsequent_n_shorter_span(
                TaskCls=StockMinuteTask,
                HandlerCls=BsMinuteHandler,
                para=cls._long_para,
            ),
            create_task_no_subsequent_n_shorter_span(
                TaskCls=IndustryDailyTask,
                HandlerCls=DcIndustryDailyHandler,
                para=cls._long_para,
            ),
            create_task_no_subsequent_n_shorter_span(
                TaskCls=StockReformTask,
                HandlerCls=ReformHandler,
                para=cls._long_para,
                func="reform_data",
            ),
        ]

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        """
        download的镜像测试
        (股票日线，股票分钟线，行业日线，股票重构）

        :return:
        """
        task_num = len(self._task_cls)
        operator = self._operator
        secs_before = DateTime.now().subtract(seconds=task_num)
        tasks = [
            self._task_cls[i](operator=operator, start_time=secs_before.add(seconds=i))
            for i in range(0, task_num)
        ]
        for t in tasks:
            t.run()

        # 校验结果
        assert operator.select_row_num("dc_stock_dayinfo_000001_") == 293
        assert operator.select_row_num("dc_stock_dayinfo_000001_qfq") == 293
        assert operator.select_row_num("dc_stock_dayinfo_000001_hfq") == 293
        assert operator.select_row_num("bs_stock_dayinfo_000001_") == 293
        assert operator.select_row_num("bs_stock_dayinfo_000001_qfq") == 293
        assert operator.select_row_num("bs_stock_dayinfo_000001_hfq") == 293
        assert operator.select_row_num("bs_stock_min5info_000001_") == 293 * 240 / 5
        assert operator.select_row_num("dc_industry_dayinfo_bk1029_") == 29
