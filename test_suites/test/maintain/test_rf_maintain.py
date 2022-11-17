from buffett.common import Code
from buffett.common.pendelum import Date
from buffett.download import Para
from buffett.download.handler.reform import ReformHandler
from buffett.download.handler.stock import AkDailyHandler

from buffett.maintain import ReformMaintain
from test import Tester, create_2stocks, create_ex_1stock


class TestReformMaintain(Tester):
    def setUp(self) -> None:
        super().setUp()

    def test_maintain(self):
        create_2stocks(operator=self.operator)
        para = Para().with_start_n_end(start=Date(2022, 1, 1), end=Date(2023, 1, 1))
        AkDailyHandler(operator=self.operator).obtain_data(para=para)
        ReformHandler(operator=self.operator).reform_data()
        assert ReformMaintain(operator=self.operator).run()

    def test_datanum_irregular(self):
        create_2stocks(operator=self.operator)
        para = Para().with_start_n_end(start=Date(2022, 1, 1), end=Date(2023, 1, 1))
        AkDailyHandler(operator=self.operator).obtain_data(para=para)
        ReformHandler(operator=self.operator).reform_data()
        self.operator.execute(
            "delete from `dc_stock_dayinfo_2022_09_` where `date` > '2022-9-15'"
        )
        assert not ReformMaintain(operator=self.operator).run()

    def test_datanum_diff_span(self):
        create_2stocks(operator=self.operator)
        para = Para().with_start_n_end(start=Date(2022, 1, 1), end=Date(2023, 1, 1))
        AkDailyHandler(operator=self.operator).obtain_data(para=para)
        create_ex_1stock(operator=self.operator, code=Code("000004"))
        para = Para().with_start_n_end(start=Date(2022, 1, 1), end=Date(2022, 7, 1))
        AkDailyHandler(operator=self.operator).obtain_data(para=para)

        ReformHandler(operator=self.operator).reform_data()
        assert ReformMaintain(operator=self.operator).run()
