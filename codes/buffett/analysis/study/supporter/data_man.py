from typing import Optional

from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.common.constants.col import FREQ, FUQUAN, SOURCE, DATE, DATETIME
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.handler import META_DICT
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.tools import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import SourceType, FreqType

SOURCE_DICT: dict[SourceType, tuple[SourceType, SourceType]] = {
    # stock
    SourceType.AK_DC: (SourceType.AK_DC, SourceType.BS),
    SourceType.BS: (SourceType.BS, SourceType.AK_DC),
    SourceType.ANA: (SourceType.AK_DC, SourceType.BS),
    # index
    SourceType.ANA_INDEX: (SourceType.AK_DC_INDEX, None),
    SourceType.AK_DC_INDEX: (SourceType.AK_DC_INDEX, None),
    # concept
    SourceType.ANA_CONCEPT: (SourceType.AK_DC_CONCEPT, None),
    SourceType.AK_DC_CONCEPT: (SourceType.AK_DC_CONCEPT, None),
    # industry
    SourceType.ANA_INDUSTRY: (SourceType.AK_DC_INDUSTRY, None),
    SourceType.AK_DC_INDUSTRY: (SourceType.AK_DC_INDUSTRY, None),
}


class DataManager:
    """
    尽管太多数时候“头等舱”可以提供必要的数据，但凡事总有例外。
    DataManager可以在“头等舱”不可用时，自动选择“经济舱”的数据。
    """

    def __init__(self, operator: Operator):
        self._operator = operator
        self._dl_record = None

    def select_data(
        self, para: Para, economy: bool = False, index: bool = True
    ) -> Optional[DataFrame]:
        """
        查询数据

        :param para:
        :param economy:
        :param index:
        :return:
        """
        #
        self._dl_record = DownloadRecorder(operator=self._operator).select_data()
        para = para.clone()
        #
        if (para.target is not None) and (para.target.code is not None):
            source = self._determine_source(para=para, economy=economy)
            if source is None:
                return
            para = para.with_source(source)
            data = self._select_data(by_code=True, para=para)
        else:
            source, economy_source = SOURCE_DICT[para.comb.source]
            para = para.with_source(source)
            data = self._select_data(para=para, by_code=False)
            economy_data = (
                self._select_economy_data(para=para, economy_source=economy_source)
                if economy
                else None
            )
            data = pd.concat_safe([data, economy_data])
        #
        if dataframe_not_valid(data):
            return
        if index:
            key = DATE if para.comb.freq == FreqType.DAY else DATETIME
            data = data.set_index(key)
        return data

    def _determine_source(self, para: Para, economy=False) -> Optional[SourceType]:
        """
        para中未指定数据源的情况下选择优先级高的数据源。

        :param para:
        :return:
        """
        dl_record = self._dl_record
        source, economy_source = SOURCE_DICT[para.comb.source]

        dl_record = dl_record[
            (dl_record[CODE] == para.target.code)
            & (dl_record[FREQ] == para.comb.freq)
            & (dl_record[FUQUAN] == para.comb.fuquan)
        ]
        if dl_record.empty:
            return None
        if (dl_record[SOURCE] == source).any():
            return source
        if not economy:
            return None
        if (dl_record[SOURCE] == economy_source).any():
            return economy_source
        return None

    def _select_economy_data(
        self, para: Para, economy_source: SourceType
    ) -> Optional[DataFrame]:
        """
        从“经济舱”筛选数据

        :param para:            not by_code: source, freq, fuquan, start, end
        :return:
        """
        #
        dl_record = self._dl_record
        source, freq, fuquan = para.comb.source, para.comb.freq, para.comb.fuquan
        #
        economy_records = dl_record[
            (dl_record[SOURCE] != economy_source)
            & (dl_record[FREQ] == freq)
            & (dl_record[FUQUAN] == fuquan)
        ]
        if economy_records.empty:
            return
        #
        first_records = dl_record[
            (dl_record[SOURCE] == source)
            & (dl_record[FREQ] == freq)
            & (dl_record[FUQUAN] == fuquan)
        ]
        economy_filter = pd.subtract(first_records[[CODE]], economy_records[[CODE]])
        economy_para = para.clone().with_source(economy_source)
        economy_data = self._select_data(by_code=False, para=economy_para)
        economy_data = pd.merge(economy_filter, economy_data, how="left", on=[CODE])
        return economy_data

    def _select_data(self, by_code: bool, para: Para) -> Optional[DataFrame]:
        """
        按照by_code或者by_date从指定表格中筛选数据

        :param by_code:
        :param para:            if by_code: code, source, freq, fuquan
                                else:       source, freq, fuquan, start, end
        :return:
        """
        DT = DATE if para.comb.freq == FreqType.DAY else DATETIME
        if by_code:
            table_name = TableNameTool.get_by_code(para=para)
            data = self._operator.select_data(
                name=table_name, span=para.span, meta=META_DICT[para.comb]
            )
            sort_by = [DT]
        else:
            table_names = TableNameTool.gets_by_span(para=para)
            data = pd.concat_safe(
                [
                    self._operator.select_data(
                        name=x, span=para.span, meta=META_DICT[para.comb]
                    )
                    for x in table_names
                ]
            )
            sort_by = [DT, CODE]
        if dataframe_not_valid(data):
            return
        data = data.sort_values(by=sort_by, ascending=True, ignore_index=True)
        return data
