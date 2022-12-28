from abc import abstractmethod
from typing import Optional

from buffett.adapter.numpy import NAN
from buffett.adapter.pandas import pd, DataFrame
from buffett.analysis import Para
from buffett.analysis.recorder.analysis_recorder import AnalysisRecorder
from buffett.analysis.study.supporter import CalendarManager, DataManager
from buffett.analysis.study.tools import get_stock_list, TableNameTool
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
from buffett.download.mysql import Operator
from buffett.download.types import FreqType, SourceType, FuquanType


class Analyst:
    def __init__(
        self,
        datasource_op: Operator,
        operator: Operator,
        analyst: AnalystType,
        meta: DataFrame = ANALY_EVENT_META,
        use_stock: bool = True,
        use_stock_minute: bool = False,
        use_index: bool = True,
        use_concept: bool = True,
        use_industry: bool = True,
        kwd: str = DATETIME,
    ):
        #
        self._datasource_op = datasource_op
        self._operator = operator
        self._analyst = analyst
        self._meta = meta
        self._use_stock = use_stock
        self._use_stock_minute = use_stock_minute
        self._use_index = use_index
        self._use_concept = use_concept
        self._use_industry = use_industry
        self._kwd = kwd
        #
        if not (
            use_stock or use_stock_minute or use_index or use_concept or use_industry
        ):
            raise ValueError("Should use at least one type of data.")
        #
        self._logger = LoggerBuilder.build(AnalysisLogger)(analyst)
        self._dataman = DataManager(datasource_op=datasource_op)
        self._calendarman = CalendarManager(datasource_op=datasource_op)
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

    def _calculate_1target(self, row: tuple) -> tuple[Para, DataFrame]:
        #
        para = Para.from_tuple(row)
        self._logger.info_calculate_start(para)
        # select & calculate
        result = self._calculate(para)
        # filter & save
        if dataframe_is_valid(result):
            result = result[para.span.is_insides(result[self._kwd])]
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

    def _save_1target(self, obj: tuple[Para, DataFrame]):
        """
        将数据存储到数据库

        :param obj:
        :return:
        """
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
        stock_list = self._get_stock_list()
        index_list = self._get_index_list()
        concept_list = self._get_concept_list()
        industry_list = self._get_industry_list()
        # merge
        todo_records = pd.concat([stock_list, index_list, concept_list, industry_list])
        todo_records[START_DATE] = convert_datetime(span.start)
        todo_records[END_DATE] = convert_datetime(span.end)
        todo_records[ANALYSIS] = self._analyst
        return todo_records

    def _get_stock_list(self) -> Optional[DataFrame]:
        """
        获取股票清单

        :return:
        """
        if not (self._use_stock or self._use_stock_minute):
            return
        stock_list = get_stock_list(operator=self._datasource_op)
        if dataframe_not_valid(stock_list):
            return
        stock_list[SOURCE] = SourceType.ANA
        stock_list[FUQUAN] = FuquanType.HFQ
        freq = DataFrame({FREQ: [FreqType.DAY, FreqType.MIN5]}).iloc[
            [self._use_stock, self._use_stock_minute]
        ]
        stock_list = pd.merge(stock_list, freq, how="cross")
        return stock_list

    def _get_index_list(self):
        """
        获取指数清单

        :return:
        """
        if not self._use_index:
            return
        index_list = DcIndexListHandler(operator=self._datasource_op).select_data()
        if dataframe_not_valid(index_list):
            return
        index_list = index_list.rename(columns={INDEX_CODE: CODE, INDEX_NAME: NAME})
        index_list[SOURCE] = SourceType.ANA_INDEX
        index_list[FUQUAN] = FuquanType.BFQ
        index_list[FREQ] = FreqType.DAY
        return index_list

    def _get_concept_list(self) -> Optional[DataFrame]:
        """
        获取概念清单

        :return:
        """
        if not self._use_concept:
            return
        concept_list = DcConceptListHandler(operator=self._datasource_op).select_data()
        if dataframe_not_valid(concept_list):
            return
        concept_list = concept_list.rename(
            columns={CONCEPT_CODE: CODE, CONCEPT_NAME: NAME}
        )
        concept_list[SOURCE] = SourceType.ANA_CONCEPT
        concept_list[FUQUAN] = FuquanType.BFQ
        concept_list[FREQ] = FreqType.DAY
        return concept_list

    def _get_industry_list(self) -> Optional[DataFrame]:
        """
        获取行业清单

        :return:
        """
        if not self._use_industry:
            return
        industry_list = DcIndustryListHandler(
            operator=self._datasource_op
        ).select_data()
        if dataframe_not_valid(industry_list):
            return
        industry_list = industry_list.rename(
            columns={INDUSTRY_CODE: CODE, INDUSTRY_NAME: NAME}
        )
        industry_list[SOURCE] = SourceType.ANA_INDUSTRY
        industry_list[FUQUAN] = FuquanType.BFQ
        industry_list[FREQ] = FreqType.DAY
        return industry_list

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
        todo_records_without_name = todo_records[
            [CODE, SOURCE, FREQ, FUQUAN, ANALYSIS, START_DATE, END_DATE]
        ]
        todo_records = pd.subtract(todo_records_without_name, done_records)
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

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        从数据库中筛选数据

        :param para:        code, source, freq, fuquan, analysis, [start, end]
        :return:
        """
        table_name = TableNameTool.get_by_code(para)
        data = self._operator.select_data(
            name=table_name, meta=self._meta, span=para.span
        )
        return data


class AnalysisLogger(Logger):
    def __init__(self, analyst: AnalystType):
        self._analyst = str(analyst)

    def info_calculate_start(self, para: Para):
        Logger.info(f"Start to Calculate {self._analyst} for {para}")

    def info_calculate_end(self, para: Para):
        Logger.info(f"Successfully Calculate {self._analyst} for {para}")

    def warning_calculate_end(self, para: Para):
        Logger.info(
            f"End Calculate {self._analyst} for {para}, nothing is found for calculation."
        )
