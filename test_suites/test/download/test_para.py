from buffett.adapter.pandas import Series
from buffett.common import Code
from buffett.common.constants.col import FREQ, SOURCE, FUQUAN, START_DATE, END_DATE
from buffett.common.constants.col.stock import CODE, NAME
from buffett.common.pendulum import Date
from buffett.download import Para
from buffett.download.types import FuquanType, FreqType, SourceType
from test import SimpleTester


class TestPara(SimpleTester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_from_series_partial(self):
        freq = FreqType.DAY
        source = SourceType.BAOSTOCK
        fuquan = FuquanType.BFQ
        series = Series({FREQ: freq, SOURCE: source, FUQUAN: fuquan})
        para = Para.from_series(series)
        assert para.comb.freq == freq
        assert para.comb.source == source
        assert para.comb.fuquan == fuquan
        assert para.span is None
        assert para.stock is None

    def test_from_series_incorrect_key(self):
        freq = FreqType.DAY
        source = SourceType.BAOSTOCK
        fuquan = FuquanType.BFQ
        series = Series({0: freq, SOURCE: source, FUQUAN: fuquan})
        para = Para.from_series(series)
        assert para.comb.freq is None
        assert para.comb.source == source
        assert para.comb.fuquan == fuquan
        assert para.span is None
        assert para.stock is None

    def test_from_series_all(self):
        freq = FreqType.DAY
        source = SourceType.BAOSTOCK
        fuquan = FuquanType.BFQ
        code = Code("000001")
        name = "平安银行"
        start_date = Date(2022, 1, 1)
        end_date = Date(2022, 1, 10)
        series = Series(
            {
                CODE: code,
                NAME: name,
                FREQ: freq,
                SOURCE: source,
                FUQUAN: fuquan,
                START_DATE: start_date,
                END_DATE: end_date,
            }
        )
        para = Para.from_series(series)
        assert para.comb.freq == freq
        assert para.comb.source == source
        assert para.comb.fuquan == fuquan
        assert para.stock.code == code
        assert para.stock.name == name
        assert para.span.start == start_date
        assert para.span.end == end_date

    def test_from_series_nothing(self):
        series = Series()
        para = Para.from_series(series)
        assert para.comb is None
        assert para.span is None
        assert para.stock is None
