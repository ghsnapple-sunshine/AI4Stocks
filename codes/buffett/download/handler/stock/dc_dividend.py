from typing import Optional

from buffett.adapter.akshare import ak
from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.pendulum import Date
from buffett.common.constants.col import (
    SGc,
    ZGc,
    SGZGc,
    XJc,
    GXLc,
)
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.handler import STK_DVD_META
from buffett.common.constants.table import STK_DVD
from buffett.common.pendulum import DateSpan
from buffett.common.tools import list_not_valid, dataframe_not_valid
from buffett.download import Para
from buffett.download.handler import Handler
from buffett.download.recorder import EasyRecorder

YEAR = "year"
MONTH = "month"
DAY = "day"

"""
表头为：

代码
名称
送转股份-送转总比例
送转股份-送转比例
送转股份-转股比例
现金分红-现金分红比例
现金分红-股息率
每股收益
每股净资产
每股公积金
每股未分配利润
净利润同比增长
总股本
预案公告日
股权登记日
除权除息日
方案进度
最新公告日期
"""

MC = "名称"

_RENAME = {
    "代码": CODE,
    "送转股份-送转总比例": SGZGc,
    "送转股份-送转比例": SGc,
    "送转股份-转股比例": ZGc,
    "现金分红-现金分红比例": XJc,
    "现金分红-股息率": GXLc,
}


class DcDividendHandler(Handler):
    def obtain_data(self, para: Para) -> Optional[DataFrame]:
        """
        获取数据

        :param para:        start, end
        :return:
        """
        # 跳过重复下载的部分
        recorder = EasyRecorder(operator=self._operator)
        todo_span = para.span.clone().with_end(
            Date.today(), para.span.end > Date.today()
        )
        curr_span = recorder.select_data(cls=DcDividendHandler)
        todo_ls = todo_span.subtract(curr_span)
        if list_not_valid(todo_ls):
            return

        time_series = self._create_time_series(span=todo_ls[0])
        if len(todo_ls) == 2:
            time_series.extend(self._create_time_series(span=todo_ls[1]))

        # 下载和保存
        df = self._download(time_series)
        self._save_to_database(df)

        # 记录
        total_span = todo_span.add(curr_span)
        total_span.with_end(
            total_span.end.subtract(months=6),
            total_span.end.add(months=6) > Date.today(),
        )
        recorder.save(cls=DcDividendHandler, span=total_span)
        return df

    @staticmethod
    def _download(time_series: list[Date]) -> Optional[DataFrame]:
        """
        遍历下载所有报告期内的数据并拼接返回

        :param time_series:
        :return:
        """
        if list_not_valid(time_series):
            return
        data = pd.concat(  # Assure safe
            [
                ak.stock_fhps_em(date=x.subtract(days=1).format("YYYYMMDD"))
                for x in time_series
            ]
        )  # XXXX-[6-30|12-31]
        data = data.rename(columns=_RENAME)
        del data[MC]
        return data

    @staticmethod
    def _create_time_series(span: DateSpan) -> list[Date]:
        """
        创建报告期清单

        :param span:
        :return:
        """
        time_series = []
        start = Date(span.start.year, span.start.month // 6 * 6 + 1, 1)  # XXXX-[1|7]-1
        end = span.end
        while start < end:
            time_series.append(start)
            start = start.add(months=6)
        return time_series

    def _save_to_database(self, df: DataFrame) -> None:
        """
        保存至数据库

        :param df:
        :return:
        """
        if dataframe_not_valid(df):
            return
        self._operator.create_table(name=STK_DVD, meta=STK_DVD_META)
        self._operator.try_insert_data(name=STK_DVD, meta=STK_DVD_META, df=df)  # 忽略重复记录

    def select_data(self, para: Para = None) -> Optional[DataFrame]:
        """
        按条件查询

        :param para:        start, end
        :return:
        """
        df = self._operator.select_data(name=STK_DVD, meta=STK_DVD_META)
        return df
