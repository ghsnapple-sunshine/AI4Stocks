from buffett.adapter.pendulum import DateTime
from buffett.common.constants.meta.handler import (
    BS_DAILY_META,
    DC_DAILY_META,
    BS_MINUTE_META,
)
from buffett.common.magic import empty_init
from buffett.common.wrapper import Wrapper
from buffett.download.handler.concept import DcConceptDailyHandler
from buffett.download.handler.index import DcIndexDailyHandler
from buffett.download.handler.industry import DcIndustryDailyHandler
from buffett.download.handler.reform import ReformHandler
from buffett.download.handler.stock import (
    DcDailyHandler,
    BsDailyHandler,
    BsMinuteHandler,
)
from buffett.task.download import (
    BsStockDailyTask,
    DcStockDailyTask,
    StockReformTask,
    StockMinuteTask,
    IndustryDailyTask,
    ConceptDailyTask,
    IndexDailyTask,
)
from test import Tester, create_1stock, create_1industry, create_1concept, create_1index
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
        # 初始化StockList, ConceptList, IndustryList, IndexList
        create_1stock(operator=cls._operator, source="both")
        create_1concept(operator=cls._operator)
        create_1industry(operator=cls._operator)
        create_1index(operator=cls._operator)
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
                TaskCls=ConceptDailyTask,
                HandlerCls=DcConceptDailyHandler,
                para=cls._long_para,
            ),
            create_task_no_subsequent_n_shorter_span(
                TaskCls=IndustryDailyTask,
                HandlerCls=DcIndustryDailyHandler,
                para=cls._long_para,
            ),
            create_task_no_subsequent_n_shorter_span(
                TaskCls=IndexDailyTask,
                HandlerCls=DcIndexDailyHandler,
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
        assert (
            operator.select_row_num(name="dc_stock_dayinfo_000001_", meta=DC_DAILY_META)
            == 293
        )
        assert (
            operator.select_row_num(
                name="dc_stock_dayinfo_000001_hfq", meta=DC_DAILY_META
            )
            == 293
        )
        assert (
            operator.select_row_num(name="bs_stock_dayinfo_000001_", meta=BS_DAILY_META)
            == 293
        )
        assert (
            operator.select_row_num(
                name="bs_stock_dayinfo_000001_hfq", meta=BS_DAILY_META
            )
            == 293
        )
        assert (
            operator.select_row_num(
                name="bs_stock_min5info_000001_", meta=BS_MINUTE_META
            )
            == 293 * 240 / 5
        )
        assert (
            operator.select_row_num(
                name="dc_industry_dayinfo_bk1029_", meta=DC_DAILY_META
            )
            == 29
        )
