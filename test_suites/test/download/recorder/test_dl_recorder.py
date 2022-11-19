from buffett.adapter.pandas import DataFrame
from buffett.common import Code
from buffett.common.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.common.constants.col.stock import CODE
from buffett.common.pendelum import Date
from buffett.download import Para
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import FuquanType, FreqType, SourceType
from test import Tester


class TestDownloadRecorder(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._recorder = DownloadRecorder(cls._operator)

    def _setup_always(self):
        pass

    def test_all(self) -> None:
        self._save_to_database()
        self._save_to_database2()
        self._select_data()

    def _save_to_database(self):
        ls = [
            [
                "000001",
                FreqType.DAY,
                FuquanType.BFQ,
                SourceType.AKSHARE_DONGCAI,
                Date(2000, 1, 1),
                Date(2022, 1, 1),
            ]
        ]
        cols = [CODE, "freq", "fuquan", "source", "start_date", "end_date"]
        df = DataFrame(data=ls, columns=cols)
        self._recorder.save_to_database(df)

    def _save_to_database2(self):
        code = Code("000002")
        freq = FreqType.DAY
        fuquan = FuquanType.BFQ
        source = SourceType.AKSHARE_DONGCAI
        start_date = Date(2000, 1, 1)
        end_date = Date(2022, 1, 1)
        para = (
            Para()
            .with_code(code)
            .with_freq(freq)
            .with_fuquan(fuquan)
            .with_source(source)
            .with_start_n_end(start_date, end_date)
        )
        self._recorder.save(para=para)

    def _select_data(self):
        data = self._recorder.select_data()
        assert type(data) == DataFrame
        # 验证数据正确
        assert data[data[CODE] == "000001"][FREQ][0] == FreqType.DAY
        assert data[data[CODE] == "000001"][FUQUAN][0] == FuquanType.BFQ
        assert data[data[CODE] == "000001"][SOURCE][0] == SourceType.AKSHARE_DONGCAI
        assert data[data[CODE] == "000001"][START_DATE][0].date() == Date(2000, 1, 1)
        assert data[data[CODE] == "000001"][END_DATE][0].date() == Date(2022, 1, 1)
        # 验证记录是否存在
        assert data[data[CODE] == "000002"].shape[0] == 1
        assert data[data[CODE] == "000003"].shape[0] == 0
