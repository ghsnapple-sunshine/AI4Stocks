from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.pendulum import Date
from buffett.common.constants.col import DATE
from buffett.common.constants.col.target import CODE, NAME
from buffett.common.constants.meta.handler import TH_DAILY_META
from buffett.common.tools import dataframe_is_valid, list_is_valid
from buffett.download import Para
from buffett.download.handler import Handler
from buffett.download.handler.base.slow import SlowHandler
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.handler.tools import select_data_slow
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.download.recorder import DownloadRecorder
from buffett.download.types import FuquanType, SourceType, FreqType

BS_VALID, DC_VALID = "bs_valid", "dc_valid"


class ThDailyHandler(SlowHandler):
    def __init__(self, operator: Operator, target_list_handler: Handler):
        super().__init__(
            operator=operator,
            target_list_handler=target_list_handler,
            calendar_handler=CalendarHandler(operator=operator),
            recorder=DownloadRecorder(operator=operator),
            source=SourceType.AK_TH,
            # fuquans=[FuquanType.BFQ, FuquanType.HFQ],
            fuquans=[FuquanType.BFQ],
            freq=FreqType.DAY,
            meta=TH_DAILY_META,
            field_code=CODE,
            field_name=NAME,
        )
        self._cache = []

    def _download(self, para: Para) -> Optional[DataFrame]:
        """
        根据para中指定的条件下载数据

        :param para:            code, fuquan, start, end
        :return:
        """
        # 使用接口（stock_zh_a_hist_ths，源：同花顺）
        key = (para.target.code, para.comb.fuquan)
        find = list(filter(lambda x: x[0] == key, self._cache))
        if list_is_valid(find):
            daily_info = find[0][1]
        else:
            daily_info = ak.stock_zh_a_hist_ths(
                symbol=para.target.code, adjust=para.comb.fuquan.ths_format()
            )
            if dataframe_is_valid(daily_info):
                if len(self._cache) >= 5:
                    self._cache = self._cache[1:]
                self._cache.append((key, daily_info))
            else:
                return
        start = para.span.start.format("YYYYMMDD")
        end = para.span.end.format("YYYYMMDD")
        daily_info = daily_info[(daily_info[DATE] >= start) & (daily_info[DATE] < end)]
        return daily_info

    def select_data(self, para: Para) -> Optional[DataFrame]:
        """
        查询某支股票、某种复权方式下、某个时间段内的全部数据

        :param para:        code, fuquan, [start, end]
        :return:
        """
        para = para.clone().with_freq(self._freq).with_source(self._source)
        return select_data_slow(operator=self._operator, meta=self._META, para=para)


class ThsStockListHandler(Handler):
    def __init__(self, operator: Operator, file_name: str):
        super(ThsStockListHandler, self).__init__(operator)
        self._file_name = file_name
        self._codes = None

    def obtain_data(self, *args, **kwargs):
        with open(self._file_name, "rb") as file:
            file = pd.read_feather(file)
            file = file[file[BS_VALID] & file[DC_VALID]]
            self._codes = file[[CODE]]

    def select_data(self):
        if self._codes is None:
            self.obtain_data()
        return self._codes


if __name__ == "__main__":
    FILE_NAME = "E:/BuffettData/daily_maintain/20221224_010507/report"
    op = Operator(RoleType.DbStock)
    stock_list_hdl = ThsStockListHandler(operator=op, file_name=FILE_NAME)
    ths_daily_hdl = ThDailyHandler(operator=op, target_list_handler=stock_list_hdl)
    ths_daily_hdl.obtain_data(
        para=Para().with_start_n_end(Date(1990, 1, 1), Date.today())
    )
