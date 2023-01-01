import numpy as np

from buffett.adapter.pandas import DataFrame
from buffett.analysis.study.stat_zdf import output_columns
from buffett.common.constants.col import DATETIME
from buffett.common.logger import LogType, Logger
from buffett.common.pendulum import DateTime
from buffett.download.mysql.insert_parser import InsertSqlParser
from test import Tester


class TestMysqlPerf(Tester):
    @classmethod
    def _setup_oncemore(cls):
        pass

    def _setup_always(self) -> None:
        Logger.Level = LogType.DEBUG

    def tearDown(self) -> None:
        Logger.Level = LogType.INFO

    def test_sql_perf(self):

        # Create
        create_sql = (
            "create table if not exists `ana_stock_zdf_min5info_000017_hfq` "
            "(`datetime` DATETIME NOT NULL,`zhangfu_day5_max` FLOAT ,`zhangfu_day5_pct99` FLOAT ,"
            "`zhangfu_day5_pct95` FLOAT ,`zhangfu_day5_pct90` FLOAT ,`diefu_day5_max` FLOAT ,"
            "`diefu_day5_pct99` FLOAT ,`diefu_day5_pct95` FLOAT ,`diefu_day5_pct90` FLOAT ,"
            "`zhangfu_day20_max` FLOAT ,`zhangfu_day20_pct99` FLOAT ,`zhangfu_day20_pct95` FLOAT ,"
            "`zhangfu_day20_pct90` FLOAT ,`diefu_day20_max` FLOAT ,`diefu_day20_pct99` FLOAT ,"
            "`diefu_day20_pct95` FLOAT ,`diefu_day20_pct90` FLOAT ,primary key (`datetime`))"
        )
        t0 = DateTime.now()
        self._operator.execute(sql=create_sql, fetch=False)
        t1 = DateTime.now()
        # Insert
        insert_sql = (
            "insert into `ana_stock_zdf_min5info_000017_hfq`(`diefu_day5_pct99`, `diefu_day5_pct95`, "
            "`diefu_day5_pct90`, `zhangfu_day5_pct90`, `zhangfu_day5_pct95`, `zhangfu_day5_pct99`, "
            "`diefu_day5_max`, `zhangfu_day5_max`, `diefu_day20_pct99`, `diefu_day20_pct95`, "
            "`diefu_day20_pct90`, `zhangfu_day20_pct90`, `zhangfu_day20_pct95`, `zhangfu_day20_pct99`, "
            "`diefu_day20_max`, `zhangfu_day20_max`, `datetime`) "
            "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = DataFrame(np.random.random(size=(257663, 16)), columns=output_columns)
        data[DATETIME] = [DateTime.now().add(seconds=x) for x in range(257663)]
        vals = InsertSqlParser._get_format_list(data)
        t2 = DateTime.now()
        self._operator.execute_many(sql=insert_sql, vals=vals, commit=True)
        t3 = DateTime.now()
        print(f"create time: {(t1-t0).total_seconds()}.")
        print(f"insert time: {(t3-t2).total_seconds()}.")