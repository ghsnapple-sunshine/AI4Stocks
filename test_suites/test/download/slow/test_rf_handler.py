from typing import Type, Union

import numpy as np
import pandas as pd

from buffett.common import Code
from buffett.common.pendelum import Date
from buffett.download import Para
from buffett.download.slow import AkDailyHandler as AHandler, BsMinuteHandler as BHandler, ReformHandler as RHandler, \
    DownloadRecorder as DRecorder, ReformRecorder as RRecorder, BsMinuteHandler
from buffett.download.slow.handler import SlowHandler
from test import Tester, DbSweeper, create_1stock, create_ex_1stock


class ReformHandlerTest(Tester):
    def setUp(self) -> None:
        super(ReformHandlerTest, self).setUp()

    def _prepare(self):
        # 清理数据库
        DbSweeper.cleanup()
        # 初始化StockList
        create_1stock(operator=self.operator)

    def _prepare_2nd(self):
        # 初始化StockList
        create_ex_1stock(operator=self.operator, code=Code('000002'))
        create_ex_1stock(operator=self.operator, code=Code('000004'))

    def _atom_test(self,
                   Cls: Type[SlowHandler],
                   start: Date,
                   end: Date,
                   dl_table_names: Union[str, list[str]],
                   rf_table_names: Union[str, list[str]]):
        # 1.下载数据
        Cls(operator=self.operator).obtain_data(
            para=Para().with_start_n_end(start=start, end=end))
        # 2. Reform
        RHandler(operator=self.operator).reform_data()
        # 3. 比较结果
        dl_table_names = dl_table_names if isinstance(dl_table_names, list) else [dl_table_names]
        dl_data = np.sum([self.operator.get_row_num(x) for x in dl_table_names])
        rf_table_names = rf_table_names if isinstance(rf_table_names, list) else [rf_table_names]
        rf_data = np.sum([self.operator.get_row_num(x) for x in rf_table_names])
        assert dl_data == rf_data
        dl_record = DRecorder(operator=self.operator).get_data()
        rf_record = RRecorder(operator=self.operator).get_data()
        assert pd.concat([dl_record, rf_record]).drop_duplicates(keep=False).empty

    def test_ak_daily_10days(self):
        """
        测试一支股票，分两次下载10天的日线数据并重组

        :return:
        """
        self._prepare()
        # S1:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 4),
                        end=Date(2022, 1, 5),
                        dl_table_names='dc_stock_dayinfo_000001_hfq',
                        rf_table_names='dc_stock_dayinfo_2022_01_hfq')
        # S2:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 1),
                        end=Date(2022, 1, 10),
                        dl_table_names='dc_stock_dayinfo_000001_hfq',
                        rf_table_names='dc_stock_dayinfo_2022_01_hfq')

    def test_ak_daily_2months(self):
        """
        测试一支股票，分两次下载2个月的日线数据并重组

        :return:
        """
        self._prepare()
        # S1:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 4),
                        end=Date(2022, 2, 4),
                        dl_table_names='dc_stock_dayinfo_000001_hfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_hfq',
                                        'dc_stock_dayinfo_2022_02_hfq'])
        # S2:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 2, 4),
                        end=Date(2022, 3, 4),
                        dl_table_names='dc_stock_dayinfo_000001_hfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_hfq',
                                        'dc_stock_dayinfo_2022_02_hfq',
                                        'dc_stock_dayinfo_2022_03_hfq'])

    def test_ak_daily_3months(self):
        """
        测试一支股票，一次下载3个月的日线数据并重组（覆盖刚好月初-月末场景）

        :return:
        """
        self._prepare()
        # S1:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 1),
                        end=Date(2022, 4, 1),
                        dl_table_names='dc_stock_dayinfo_000001_hfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_hfq',
                                        'dc_stock_dayinfo_2022_02_hfq',
                                        'dc_stock_dayinfo_2022_03_hfq'])

    def test_ak_daily_append(self):
        """
        测试日线增加新股票的场景

        :return:
        """
        self._prepare()
        # S1:
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 1, 4),
                        end=Date(2022, 4, 4),
                        dl_table_names='dc_stock_dayinfo_000001_qfq',
                        rf_table_names=['dc_stock_dayinfo_2022_01_qfq',
                                        'dc_stock_dayinfo_2022_02_qfq',
                                        'dc_stock_dayinfo_2022_03_qfq',
                                        'dc_stock_dayinfo_2022_04_qfq'])
        # S2:
        self._prepare_2nd()
        self._atom_test(Cls=AHandler,
                        start=Date(2022, 2, 4),
                        end=Date(2022, 3, 4),
                        dl_table_names=['dc_stock_dayinfo_000001_qfq',
                                        'dc_stock_dayinfo_000002_qfq',
                                        'dc_stock_dayinfo_000004_qfq'],
                        rf_table_names=['dc_stock_dayinfo_2022_01_qfq',
                                        'dc_stock_dayinfo_2022_02_qfq',
                                        'dc_stock_dayinfo_2022_03_qfq',
                                        'dc_stock_dayinfo_2022_04_qfq'])

        def test_bs_minute_10days(self):
            """
            测试一支股票，分两次下载10天的分钟线数据并重组

            :return:
            """
            self._prepare()
            # S1:
            self._atom_test(Cls=BHandler,
                            start=Date(2022, 1, 4),
                            end=Date(2022, 1, 5),
                            dl_table_names='bs_stock_min5info_000001_',
                            rf_table_names='bs_stock_min5info_2022_01_')
            # S2:
            self._atom_test(Cls=BHandler,
                            start=Date(2022, 1, 1),
                            end=Date(2022, 1, 10),
                            dl_table_names='bs_stock_min5info_000001_',
                            rf_table_names='bs_stock_min5info_2022_01_')

        def test_bs_minute_2months(self):
            """
            测试一支股票，分两次下载2个月的分钟线数据并重组

            :return:
            """
            self._prepare()
            # S1:
            self._atom_test(Cls=BHandler,
                            start=Date(2022, 1, 4),
                            end=Date(2022, 1, 5),
                            dl_table_names='bs_stock_min5info_000001_',
                            rf_table_names='bs_stock_min5info_2022_01_')
            # S2:
            self._atom_test(Cls=BHandler,
                            start=Date(2021, 12, 31),
                            end=Date(2022, 2, 10),
                            dl_table_names='bs_stock_min5info_000001_',
                            rf_table_names=['bs_stock_min5info_2021_12_',
                                            'bs_stock_min5info_2022_01_',
                                            'bs_stock_min5info_2022_02_'])

    def test_irregular(self):
        DbSweeper.cleanup()
        create_ex_1stock(self.operator, Code('000795'))
        para = Para().with_start_n_end(Date(2002, 4, 16), Date(2002, 4, 17))
        BsMinuteHandler(operator=self.operator).obtain_data(para=para)
        RHandler(operator=self.operator).reform_data()


