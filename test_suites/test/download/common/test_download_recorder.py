from pandas import DataFrame
from pendulum import DateTime

from ai4stocks.common.types import FuquanType, DataFreqType, DataSourceType
from ai4stocks.download.slow.download_recorder import DownloadRecorder
from test.common.base_test import BaseTest


class TestDownloadRecorder(BaseTest):
    def setUp(self):
        super().setUp()
        self.recorder = DownloadRecorder(self.op)

    def test_all(self) -> None:
        self.__save_to_database__()
        self.__save_to_database2__()
        self.__get_table__()

    def __save_to_database__(self):
        ls = [
            ['000001',
             DataFreqType.DAY,
             FuquanType.NONE,
             DataSourceType.AKSHARE_DONGCAI,
             DateTime(2000, 1, 1),
             DateTime(2022, 1, 1)]
        ]
        cols = ['code', 'freq', 'fuquan', 'source', 'start_date', 'end_date']
        df = DataFrame(data=ls, columns=cols)
        self.recorder.save_to_database(df)

    def __save_to_database2__(self):
        code = '000002'
        freq = DataFreqType.DAY
        fuquan = FuquanType.NONE
        source = DataSourceType.AKSHARE_DONGCAI
        start_date = DateTime(2000, 1, 1)
        end_date = DateTime(2022, 1, 1)
        self.recorder.save(code, freq, fuquan, source, start_date, end_date)

    def __get_table__(self):
        data = self.recorder.get_table()
        assert type(data) == DataFrame
        # 验证数据正确
        assert data[data['code'] == '000001']['freq'][0] == DataFreqType.DAY
        assert data[data['code'] == '000001']['fuquan'][0] == FuquanType.NONE
        assert data[data['code'] == '000001']['source'][0] == DataSourceType.AKSHARE_DONGCAI
        assert data[data['code'] == '000001']['start_date'][0] == DateTime(2000, 1, 1)
        assert data[data['code'] == '000001']['end_date'][0] == DateTime(2022, 1, 1)
        # 验证记录是否存在
        assert data[data['code'] == '000002'].shape[0] == 1
        assert data[data['code'] == '000003'].shape[0] == 0
