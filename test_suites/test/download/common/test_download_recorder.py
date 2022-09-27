from pendulum import DateTime
from pandas import DataFrame

from ai4stocks.common.types import FuquanType, DataFreqType, DataSourceType
from test.common.base_test import BaseTest


class TestDownloadRecorder(BaseTest):

    def test_all(self) -> None:
        self.Save2Database()
        self.Save2Database2()
        self.GetTable()

    def Save2Database(self):
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
        self.recorder.Save2Database(df)

    def Save2Database2(self):
        code = '000002'
        freq = DataFreqType.DAY
        fuquan = FuquanType.NONE
        source = DataSourceType.AKSHARE_DONGCAI
        start_date = DateTime(2000, 1, 1)
        end_date = DateTime(2022, 1, 1)
        self.recorder.Save(code, freq, fuquan, source, start_date, end_date)

    def GetTable(self):
        data = self.recorder.GetTable()
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
