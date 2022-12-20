from abc import abstractmethod

from buffett.adapter import os
from buffett.adapter.pandas import DataFrame
from buffett.adapter.pendulum import DateTime
from buffett.common.magic import empty_method


class BaseMaintain:
    _save = True

    @classmethod
    def set_save_report(cls, save: bool):
        cls._save = save
        if cls._save:
            cls._save_report = cls._do_save_report
        else:
            cls._save_report = empty_method

    def __init__(self, folder: str):
        self._folder = folder

    @abstractmethod
    def run(self) -> bool:
        """
        运行自检程序

        :return:
        """
        pass

    def _do_save_report(self, df: DataFrame):
        _dir = (
            f"e:/BuffettData/{self._folder}/{DateTime.now().format('YYYYMMDD_HHmmss')}/"
        )
        os.makedirs(_dir)
        df.to_csv(f"{_dir}report.csv", header=True, index=False, encoding="gbk")

    _save_report = _do_save_report


