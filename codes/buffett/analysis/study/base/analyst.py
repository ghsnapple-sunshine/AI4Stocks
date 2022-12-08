from abc import abstractmethod
from typing import Optional

from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import pd, DataFrame
from buffett.analysis import Para
from buffett.analysis.recorder.analysis_recorder import AnalysisRecorder
from buffett.analysis.study.calendar_man import CalendarManager
from buffett.analysis.study.data_man import DataManager
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import (
    SOURCE,
    FREQ,
    START_DATE,
    END_DATE,
    FUQUAN,
    DATETIME,
)
from buffett.common.constants.col.analysis import ANALYSIS
from buffett.common.constants.col.my import DORCD_START, DORCD_END
from buffett.common.constants.col.target import (
    INDEX_CODE,
    CODE,
    INDEX_NAME,
    NAME,
    CONCEPT_CODE,
    CONCEPT_NAME,
    INDUSTRY_CODE,
    INDUSTRY_NAME,
)
from buffett.common.constants.meta.analysis import ANALY_EVENT_META
from buffett.common.interface import ProducerConsumer
from buffett.common.logger import LoggerBuilder, Logger
from buffett.common.pendulum import DateSpan, convert_datetime
from buffett.common.tools import dataframe_is_valid, dataframe_not_valid
from buffett.common.wrapper import Wrapper
from buffett.download.handler.concept import DcConceptListHandler
from buffett.download.handler.index import DcIndexListHandler
from buffett.download.handler.industry import DcIndustryListHandler
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.types import FreqType, SourceType, FuquanType


class Analyst:
    def __init__(
        self,
        datasource_op: Operator,
        operator: Operator,
        analyst: AnalystType,
        use_economy: bool,
        offset: int,
        meta: DataFrame = ANALY_EVENT_META,
        use_stock: bool = True,
        use_index: bool = True,
        use_concept: bool = True,
        use_industry: bool = True,
    ):
        #
        self._datasource_op = datasource_op
        self._operator = operator
        self._analyst = analyst
        self._use_economy = use_economy
        self._offset = offset
        self._meta = meta
        self._use_stock = use_stock
        self._use_index = use_index
        self._use_concept = use_concept
        self._use_industry = use_industry
        #
        if not (use_stock or use_index or use_concept or use_industry):
            raise ValueError("Should use at least one type of data.")
        #
        self._logger = LoggerBuilder.build(AnalysisLogger)(analyst)
        self._dataman = DataManager(operator=datasource_op)
        self._calendarman = CalendarManager(operator=datasource_op)
        self._recorder = AnalysisRecorder(operator=operator)

    def calculate(self, span: DateSpan) -> None:
        """
        在指定的时间范围内计算某类型数据

        :param span:
        :return:
        """
        todo_records = self._get_todo_records(span=span)
        done_records = self._recorder.select_data()
        comb_records = self._get_comb_records(
            todo_records=todo_records, done_records=done_records
        )
        # 使用生产者/消费者模式，异步下载/保存数据
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._calculate_1target),
            consumer=Wrapper(self._save_1target),
            queue_size=30,
            args_map=comb_records.itertuples(index=False),
            task_num=len(comb_records),
        )
        prod_cons.run()

    def _calculate_1target(self, row: tuple) -> Optional[tuple[Para, DataFrame]]:
        #
        para = Para.from_tuple(row)
        self._logger.info_calculate_start(para)
        # select & calculate
        result = self._calculate(para)
        # filter & save
        if dataframe_not_valid(result):
            return None
        result = result[para.span.is_insides(result[DATETIME])]
        return para, result


    @staticmethod
    @abstractmethod
    def _calculate(para: Para) -> Optional[DataFrame]:
        """
        执行计算逻辑

        :param para:
        :return:
        """
        pass

    def _save_1target(self, obj: Optional[tuple[Para, DataFrame]]):
        """
        将数据存储到数据库

        :param obj:
        :return:
        """
        if obj is None:
            return
        para, data = obj
        if dataframe_is_valid(data):
            table_name = TableNameTool.get_by_code(para=para)
            self._operator.create_table(name=table_name, meta=self._meta)
            self._operator.insert_data_safe(name=table_name, df=data, meta=self._meta)
            self._recorder.save(para=para)
        self._logger.info_calculate_end(para)

    def _get_todo_records(self, span: DateSpan) -> DataFrame:
        """
        获取StockList, IndexList, ConceptList, IndustryList

        :param span
        :return:
        """
        operator = self._datasource_op
        # stock_list
        stock_list = None
        if self._use_stock:
            sse_stock_list = SseStockListHandler(operator=operator).select_data()
            bs_stock_list = BsStockListHandler(operator=operator).select_data()[
                [CODE, NAME]
            ]
            stock_list = pd.concat([sse_stock_list, bs_stock_list]).drop_duplicates()
            stock_list[SOURCE] = SourceType.ANALYSIS_STOCK
            stock_list[FUQUAN] = FuquanType.HFQ
        # index_list
        index_list = None
        if self._use_index:
            index_list = (
                DcIndexListHandler(operator=operator)
                .select_data()
                .rename(columns={INDEX_CODE: CODE, INDEX_NAME: NAME})
            )
            index_list[SOURCE] = SourceType.ANALYSIS_INDEX
            index_list[FUQUAN] = FuquanType.BFQ
        # concept_list
        concept_list = None
        if self._use_concept:
            concept_list = (
                DcConceptListHandler(operator=operator)
                .select_data()
                .rename(columns={CONCEPT_CODE: CODE, CONCEPT_NAME: NAME})
            )
            concept_list[SOURCE] = SourceType.ANALYSIS_CONCEPT
            concept_list[FUQUAN] = FuquanType.BFQ
        # industry_list
        industry_list = None
        if self._use_industry:
            industry_list = (
                DcIndustryListHandler(operator=operator)
                .select_data()
                .rename(columns={INDUSTRY_CODE: CODE, INDUSTRY_NAME: NAME})
            )
            industry_list[SOURCE] = SourceType.ANALYSIS_INDUSTRY
            industry_list[FUQUAN] = FuquanType.BFQ
        # merge
        todo_records = pd.concat([stock_list, index_list, concept_list, industry_list])
        todo_records[FREQ] = FreqType.DAY
        todo_records[START_DATE] = convert_datetime(span.start)
        todo_records[END_DATE] = convert_datetime(span.end)
        todo_records[ANALYSIS] = self._analyst
        return todo_records

    @staticmethod
    def _get_comb_records(todo_records: DataFrame, done_records: DataFrame):
        """
        将待完成/已完成记录进行拼接

        :param todo_records:    待完成记录
        :param done_records:    已完成记录
        :return:
        """
        if dataframe_not_valid(done_records):
            todo_records[DORCD_START] = NAN
            todo_records[DORCD_END] = NAN
            return todo_records
        todo_records = pd.subtract(todo_records, done_records)
        done_records = done_records.rename(
            columns={START_DATE: DORCD_START, END_DATE: DORCD_END}
        )
        comb_list = pd.merge(
            todo_records,
            done_records,
            how="left",
            on=[CODE, SOURCE, FREQ, FUQUAN, ANALYSIS],
        )
        return comb_list


class AnalysisLogger(Logger):
    def __init__(self, analyst: AnalystType):
        self._analyst = str(analyst)

    def info_calculate_start(self, para: Para):
        Logger.info(f"Start to Calculate {self._analyst} for {para}")

    def info_calculate_end(self, para: Para):
        Logger.info(f"Successfully Calculate {self._analyst} for {para}")