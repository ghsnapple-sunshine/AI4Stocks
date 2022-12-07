from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import pd, DataFrame, Series
from buffett.adapter.talib import PatternRecognize
from buffett.analysis import Para
from buffett.analysis.recorder.analysis_recorder import AnalysisRecorder
from buffett.analysis.study.calendar_man import CalendarManager
from buffett.analysis.study.data_man import DataManager
from buffett.analysis.types import AnalysisType
from buffett.common.constants.col import (
    SOURCE,
    FREQ,
    START_DATE,
    END_DATE,
    FUQUAN,
    DATETIME,
)
from buffett.common.constants.col.analysis import ANALYSIS, EVENT, VALUE
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
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_is_valid, dataframe_not_valid
from buffett.common.wrapper import Wrapper
from buffett.download.handler.concept import DcConceptListHandler
from buffett.download.handler.index import DcIndexListHandler
from buffett.download.handler.industry import DcIndustryListHandler
from buffett.download.handler.list import SseStockListHandler, BsStockListHandler
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.types import FreqType, SourceType, FuquanType


class PatternHandler:
    def __init__(self, select_op: Operator, insert_op: Operator):
        self._select_op = select_op
        self._insert_op = insert_op
        self._dataman = DataManager(operator=select_op)
        self._calendarman = CalendarManager(operator=select_op)
        self._recorder = AnalysisRecorder(operator=insert_op)
        self._logger = LoggerBuilder.build(PatternHandlerLogger)()

    def calculate(self, span: DateSpan) -> None:
        """
        在指定的时间范围内计算pattern数据

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
            consumer=Wrapper(self._save_to_database),
            queue_size=30,
            args_map=comb_records.itertuples(index=False),
            task_num=len(comb_records),
        )
        prod_cons.run()

    def _calculate_1target(self, row: tuple) -> tuple[Para, DataFrame]:
        # select
        para = Para.from_tuple(row)
        self._logger.info_calculate_start(para)
        start = self._calendarman.query(para.span.start, offset=-5)
        select_para = para.clone().with_start(start)
        data = self._dataman.select_data(para=select_para, economy=True)
        # calculate
        pattern = PatternRecognize.all(inputs=data)
        pattern = self._convert_pattern(pattern)
        # save
        save_para = para.clone().with_source(SourceType.ANALYSIS_STOCK)
        return save_para, pattern

    @staticmethod
    def _convert_pattern(pattern: DataFrame) -> DataFrame:
        """
        将模式识别结果转换为事件表达结构

        :param pattern:
        :return:
        """

        def _filter(col: Series) -> DataFrame:
            col = col[col != 0]
            data = DataFrame({DATETIME: col.index, EVENT: col.name, VALUE: col.values})
            return data

        pattern = pd.concat([_filter(pattern[x]) for x in pattern.columns])
        return pattern

    def _save_to_database(self, obj: [Para, DataFrame]):
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
            self._insert_op.create_table(name=table_name, meta=ANALY_EVENT_META)
            self._insert_op.insert_data(name=table_name, df=data)
            self._recorder.save(para=para)
        self._logger.info_calculate_end(para)

    def _get_todo_records(self, span: DateSpan) -> DataFrame:
        """
        获取StockList, IndexList, ConceptList, IndustryList

        :param span
        :return:
        """
        operator = self._select_op
        # stock_list
        sse_stock_list = SseStockListHandler(operator=operator).select_data()
        bs_stock_list = BsStockListHandler(operator=operator).select_data()[
            [CODE, NAME]
        ]
        stock_list = pd.concat([sse_stock_list, bs_stock_list]).drop_duplicates()
        stock_list[SOURCE] = SourceType.ANALYSIS_STOCK
        stock_list[FUQUAN] = FuquanType.HFQ
        # index_list
        index_list = (
            DcIndexListHandler(operator=operator)
            .select_data()
            .rename(columns={INDEX_CODE: CODE, INDEX_NAME: NAME})
        )
        index_list[SOURCE] = SourceType.ANALYSIS_INDEX
        index_list[FUQUAN] = FuquanType.BFQ
        # concept_list
        concept_list = (
            DcConceptListHandler(operator=operator)
            .select_data()
            .rename(columns={CONCEPT_CODE: CODE, CONCEPT_NAME: NAME})
        )
        concept_list[SOURCE] = SourceType.ANALYSIS_CONCEPT
        concept_list[FUQUAN] = FuquanType.BFQ
        # industry_list
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
        todo_records[START_DATE] = span.start
        todo_records[END_DATE] = span.end
        todo_records[ANALYSIS] = AnalysisType.PATTERN
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
        done_records.rename(columns={START_DATE: DORCD_START, END_DATE: DORCD_END})
        comb_list = pd.merge(
            todo_records,
            done_records,
            how="left",
            on=[CODE, SOURCE, FREQ, FUQUAN, ANALYSIS],
        )
        return comb_list


class PatternHandlerLogger(Logger):
    @staticmethod
    def info_calculate_start(para: Para):
        Logger.info(f"Start to Calculate Pattern for {para}")

    @staticmethod
    def info_calculate_end(para: Para):
        Logger.info(f"Successfully Calculate Pattern for {para}")
