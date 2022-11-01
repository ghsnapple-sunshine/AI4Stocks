from typing import Type

import numpy as np
import pandas as pd

from buffett.common.pendelum import Date
from buffett.download import Para
from buffett.download.slow import AkDailyHandler as AHandler, BsMinuteHandler as BHandler, ReformHandler as RHandler, \
    DownloadRecorder as DRecorder, ReformRecorder as RRecorder
from buffett.download.slow.handler import SlowHandler
from test import Tester, DbSweeper, create_1stock


class ReformHandlerTest(Tester):
    def setUp(self) -> None:
        super(ReformHandlerTest, self).setUp()

    def _prepare(self):
        # 清理数据库
        DbSweeper.cleanup()
        # 初始化StockList
        create_1stock(operator=self.operator)

    def _atom_test(self,
                   Cls: Type[SlowHandler],
                   start: Date,
                   end: Date,
                   table_name: str,
                   rf_table_names: list[str]):
        # 1.下载数据
        Cls(operator=self.operator).obtain_data(
            para=Para().with_start_n_end(start=start, end=end))
        # 2. Reform
        RHandler(operator=self.operator).reform_data()
        # 3. 比较结果
        dl_data = self.operator.get_row_num(table_name)
        rf_data = np.sum([self.operator.get_row_num(x) for x in rf_table_names])
        assert dl_data == rf_data
        dl_record = DRecorder(operator=self.operator).get_data()
        rf_record = RRecorder(operator=self.operator).get_data()
        assert pd.concat([dl_record, rf_record]).drop_duplicates(keep=False).empty

    def test_ak_daily_10days(self):
        self._prepare()
        # S1:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 4),
                        end=Date(2022, 1, 5),
                        table_name='dc_stock_dayinfo_000001_hfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_hfq'])
        # S2:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 1),
                        end=Date(2022, 1, 10),
                        table_name='dc_stock_dayinfo_000001_hfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_hfq'])

    def test_ak_daily_2months(self):
        self._prepare()
        # S1:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 4),
                        end=Date(2022, 2, 4),
                        table_name='dc_stock_dayinfo_000001_hfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_hfq',
                                        'dc_stock_dayinfo_2022_02_hfq'])
        # S2:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 2, 4),
                        end=Date(2022, 3, 4),
                        table_name='dc_stock_dayinfo_000001_hfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_hfq',
                                        'dc_stock_dayinfo_2022_02_hfq',
                                        'dc_stock_dayinfo_2022_03_hfq'])

    def test_bs_minute_10days(self):
        self._prepare()
        # S1:
        self._atom_test(Cls=BHandler,
                        start=Date(2022, 1, 4),
                        end=Date(2022, 1, 5),
                        table_name='bs_stock_min5info_000001_',
                        rf_table_names=['bs_stock_min5info_2022_01_'])
        # S2:
        self._atom_test(Cls=BHandler,
                        start=Date(2022, 1, 1),
                        end=Date(2022, 1, 10),
                        table_name='bs_stock_min5info_000001_',
                        rf_table_names=['bs_stock_min5info_2022_01_'])

    def test_bs_minute_2months(self):
        self._prepare()
        # S1:
        self._atom_test(Cls=BHandler,
                        start=Date(2022, 1, 4),
                        end=Date(2022, 1, 5),
                        table_name='bs_stock_min5info_000001_',
                        rf_table_names=['bs_stock_min5info_2022_01_'])
        # S2:
        self._atom_test(Cls=BHandler,
                        start=Date(2021, 12, 31),
                        end=Date(2022, 2, 10),
                        table_name='bs_stock_min5info_000001_',
                        rf_table_names=['bs_stock_min5info_2021_12_',
                                        'bs_stock_min5info_2022_01_',
                                        'bs_stock_min5info_2022_02_'])
