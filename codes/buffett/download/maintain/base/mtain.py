from abc import abstractmethod
from typing import Optional

from buffett.adapter.os import os
from buffett.adapter.pandas import DataFrame
from buffett.adapter.pendulum import DateTime


class BaseMaintain:
    _save = True

    def __init__(self, folder: str):
        self._folder = folder

    @abstractmethod
    def run(self) -> Optional[DataFrame]:
        """
        运行自检程序

        :return:
        """
        pass

    def _save_report(self, df: DataFrame, csv=True, feather=False):
        _dir = (
            f"e:/BuffettData/{self._folder}/{DateTime.now().format('YYYYMMDD_HHmmss')}/"
        )
        os.makedirs(_dir)
        df = df.reset_index(drop=True)
        if feather:
            df.to_feather(f"{_dir}report")
        # csv文件分卷输出
        if csv:
            VSIZE = 1_000_000
            total = df.shape[0]
            df.to_csv(f"{_dir}report.csv", header=True, index=False, encoding="gbk")
            if total > VSIZE:
                i = 0
                for i in range(total // VSIZE):
                    df.iloc[i * VSIZE : (i + 1) * VSIZE].to_csv(
                        f"{_dir}report_{i}.csv",
                        header=True,
                        index=False,
                        encoding="gbk",
                    )
                curr = total // VSIZE * VSIZE
                if total > curr:
                    df.iloc[curr:].to_csv(
                        f"{_dir}report_{i + 1}.csv",
                        header=True,
                        index=False,
                        encoding="gbk",
                    )
