from typing import Optional, Union

from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.study.tools import TableNameTool as AnaTool
from buffett.analysis.types import CombExType, AnalystType
from buffett.common.constants.col import DATE, DATETIME
from buffett.common.constants.meta.analysis import META_DICT as ANA_DICT
from buffett.common.constants.meta.handler import META_DICT as DL_DICT
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para as DPara
from buffett.download.handler.tools import TableNameTool as DlTool
from buffett.download.mysql import Operator
from buffett.download.types import SourceType

SOURCE_DICT = {
    SourceType.ANA_INDEX: SourceType.AK_DC_INDEX,
    SourceType.ANA_CONCEPT: SourceType.AK_DC_CONCEPT,
    SourceType.ANA_INDUSTRY: SourceType.AK_DC_INDUSTRY,
}


class DataManager:
    def __init__(self, datasource_op: Operator):
        self._datasource_op = datasource_op

    def select_data(
        self,
        para: Union[Para, DPara],
        index: bool = True,
    ) -> Optional[DataFrame]:
        """
        查询数据

        :param para:
        :param index:
        :return:
        """
        if para.target.code is not None:
            by_code = True
            method = "get_by_code"
        else:
            by_code = False
            method = "get_by_date"
        if para.comb.source == SourceType.ANA:
            para = para.clone().with_analysis(AnalystType.CONV)
            meta = ANA_DICT[para.comb]
            table_name = getattr(AnaTool, method)(para=para)
        else:
            comb = para.comb
            comb = CombExType.to_base(comb) if isinstance(comb, CombExType) else comb
            if para.comb.source.is_analysis():
                comb.with_source(SOURCE_DICT[comb.source])
            meta = DL_DICT[comb]
            table_name = getattr(DlTool, method)(para=para)
        data = self._select_data(
            by_code=by_code,
            operator=self._datasource_op,
            table_name=table_name,
            meta=meta,
            span=para.span,
        )
        if dataframe_not_valid(data) or not index:
            return data
        INDEX = DATE if DATE in data.columns else DATETIME
        return data.set_index(INDEX)

    @staticmethod
    def _select_data(
        by_code: bool,
        operator: Operator,
        table_name: Union[str, list[str]],
        meta: DataFrame,
        span: DateSpan,
    ) -> Optional[DataFrame]:
        """
        按照要求获取数据

        :param by_code:         按照code索引还是按照date索引
        :param operator:        operator
        :param table_name:      表名
        :param meta:            表元数据
        :param span:            时间段
        :return:
        """
        if by_code:
            return operator.select_data(name=table_name, meta=meta, span=span)
        else:
            raise NotImplemented
