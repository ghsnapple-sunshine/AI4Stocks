import unittest

from buffett.common.pendelum import Date
from buffett.download import Para
from buffett.download.slow.ak_minute_handler import AkMinuteHandler
from test.tester import Tester
from test.tools import create_2stocks


class AkStockMinuteHandlerTest(Tester):
    def test_download(self) -> None:
        stocks = create_2stocks(self.operator)
        para = Para().with_start_n_end(Date(2022, 9, 1), Date(2022, 9, 30))
        tbls = AkMinuteHandler(operator=self.operator).obtain_data(para=para)
        assert stocks.shape[0] == len(tbls)


if __name__ == '__main__':
    unittest.main()
