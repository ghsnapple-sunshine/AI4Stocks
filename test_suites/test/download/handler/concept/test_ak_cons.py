from buffett.common.constants.col.stock import CONCEPT_CODE
from buffett.common.constants.table import CNCP_LS
from buffett.download.handler.concept import ConceptConsHandler, ConceptListHandler
from buffett.download.handler.stock import StockListHandler
from test import Tester


class TestConceptConsHandler(Tester):
    def setUp(self) -> None:
        super(TestConceptConsHandler, self).setUp()
        StockListHandler(self.operator).obtain_data()
        ConceptListHandler(self.operator).obtain_data()
        self.operator.execute(f"DELETE FROM {CNCP_LS} WHERE {CONCEPT_CODE} > 'BK0500'")  # 减少concept个数，提升测试性能

    def test_download(self):
        hdl = ConceptConsHandler(operator=self.operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        assert self.compare_dataframe(df1, df2)

    def test_repeat_download(self):
        # 测试重复下载不报错
        ConceptConsHandler(operator=self.operator).obtain_data()
