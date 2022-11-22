from collections import namedtuple

from buffett.adapter.numpy import NAN
from buffett.adapter.pendulum import DateTime
from buffett.common import Code
from buffett.common.constants.col import (
    END_DATE,
    DATE,
    FREQ,
    FUQUAN,
    SOURCE,
    START_DATE,
)
from buffett.common.constants.col.my import DORCD_START, DORCD_END
from buffett.common.constants.col.stock import CODE
from buffett.download import Para
from buffett.download.handler.stock.ak_pepb import AkStockPePbHandler
from buffett.download.types import FreqType, FuquanType, SourceType
from test import Tester, create_2stocks


class AkStockPePbHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = AkStockPePbHandler(operator=cls._operator)
        create_2stocks(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        self._hdl.obtain_data()
        df1 = self._hdl.select_data(para=Para().with_code(Code("000001")))
        dat = namedtuple(
            "TUPLE",
            [CODE, FREQ, FUQUAN, SOURCE, START_DATE, END_DATE, DORCD_START, DORCD_END],
        )(
            "000001",
            FreqType.DAY,
            FuquanType.BFQ,
            SourceType.AKSHARE_LGLG_PEPB,
            DateTime(2000, 1, 1),
            DateTime.today(),
            NAN,
            NAN,
        )  # Don't worry, it works.
        df2 = self._hdl._download(tup=dat)
        assert self.dataframe_almost_equals(df1, df2, join=[CODE, DATE])

    def _repeat_download(self):
        # 测试重复下载不报错
        self._hdl.obtain_data()
