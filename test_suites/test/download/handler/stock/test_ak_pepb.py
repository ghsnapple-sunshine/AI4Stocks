from collections import namedtuple

from buffett.adapter.numpy import NAN
from buffett.common import Code
from buffett.common.constants.col import (
    END_DATE,
    DATE,
    FREQ,
    FUQUAN,
    START_DATE,
    SOURCE,
)
from buffett.common.constants.col.my import DORCD_START, DORCD_END
from buffett.common.constants.col.stock import CODE
from buffett.download import Para
from buffett.download.handler.stock.ak_pepb import AkStockPePbHandler
from buffett.download.types import FreqType, FuquanType, SourceType
from test import Tester, create_2stocks, DbSweeper


class AkStockPePbHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        DbSweeper.cleanup()
        cls._hdl = AkStockPePbHandler(operator=cls._operator)
        create_2stocks(operator=cls._operator)

    def _setup_always(self) -> None:
        pass

    def test_download(self):
        self._download()
        self._repeat_download()

    def _download(self):
        self._hdl.obtain_data(span=self._long_para.span)
        df1 = self._hdl.select_data(Para().with_code(Code("000001")))
        data = {
            CODE: "000001",
            FREQ: FreqType.DAY,
            FUQUAN: FuquanType.BFQ,
            SOURCE: SourceType.AKSHARE_LGLG_PEPB,
            START_DATE: self._long_para.span.start,
            END_DATE: self._long_para.span.end,
            DORCD_START: NAN,
            DORCD_END: NAN,
        }
        Dat = namedtuple("TUPLE", list(data.keys()))
        row = Dat(**data)
        df2 = self._hdl._download(row=row)
        df2 = self._hdl._filter(row=row, df=df2)
        assert self.dataframe_almost_equals(df1, df2, join=[DATE])

    def _repeat_download(self):
        # 测试重复下载不报错
        self._hdl.obtain_data(span=self._long_para.span)
