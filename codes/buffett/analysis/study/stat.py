from buffett.adapter.pandas import DataFrame
from buffett.analysis import Para
from buffett.analysis.study.base import Analyst
from buffett.analysis.types import AnalystType
from buffett.common.constants.meta.analysis import ANALY_ZDF_META
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
            use_economy=True,
            offset=20,
            meta=ANALY_ZDF_META,
            use_stock=True,
            use_index=False,
            use_concept=False,
            use_industry=False,
        )

    @staticmethod
    def _calculate(para: Para) -> DataFrame:
        pass
