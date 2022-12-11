from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst
from buffett.analysis.study.tool.zdf_stat import calculate_zdf
from buffett.analysis.types import AnalystType
from buffett.common.constants.meta.analysis import ANALY_ZDF_META
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator


class StatisticsAnalyst(Analyst):
    def __init__(
        self,
        datasource_op: Operator,
        operator: Operator,
    ):
        super(StatisticsAnalyst, self).__init__(
            datasource_op=datasource_op,
            operator=operator,
            analyst=AnalystType.PATTERN,
            meta=ANALY_ZDF_META,
            use_stock=True,
            use_index=False,
            use_concept=False,
            use_industry=False,
        )

    def _calculate(self, para: Para) -> Optional[DataFrame]:
        """
        计算统计量

        :param para:
        :return:
        """
        start = self._calendarman.query(para.span.start, offset=-20)
        end = self._calendarman.query(para.span.end, offset=20)
        select_para = para.clone().with_start_n_end(start, end)
        data = self._dataman.select_data(para=select_para, use_economy=True)
        if dataframe_not_valid(data):
            return
        stat = calculate_zdf(data)
        return stat

