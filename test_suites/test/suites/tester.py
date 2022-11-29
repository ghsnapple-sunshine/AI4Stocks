from abc import abstractmethod
from typing import final, Optional

from buffett.adapter.akshare import ak
from buffett.adapter.baostock import bs
from buffett.adapter.pendulum import Date
from buffett.common.pendulum import DateTime
from buffett.common.tools import list_is_valid
from buffett.download import Para
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.download.types import FuquanType
from test import DbSweeper, Accelerator, SimpleTester


class Tester(SimpleTester):
    """
    Hint: 每个Tester初始化时基类会调用一次清理数据库。
    """

    _table_name = None
    _operator = None
    _short_para = None
    _long_para = None

    @classmethod
    @final
    def _setup_once(cls):
        """
        按类进行初始化

        :return:
        """
        # 定义变量
        cls._operator = Operator(role=RoleType.DbTest)
        cls._operator.connect()
        cls._table_name = "test_{0}".format(DateTime.now().format("YYYYMMDD_HHmmss"))
        cls._short_para = (
            Para()
            .with_start_n_end(Date(2020, 1, 15), Date(2020, 3, 28))
            .with_code("000001")
            .with_fuquan(FuquanType.BFQ)
        )
        cls._long_para = (
            Para()
            .with_start_n_end(Date(2020, 1, 15), Date(2021, 4, 2))
            .with_code("000001")
            .with_fuquan(FuquanType.BFQ)
        )
        cls._great_para = (
            Para()
            .with_start_n_end(Date(2000, 1, 1), Date(2020, 12, 31))
            .with_code("000001")
            .with_fuquan(FuquanType.BFQ)
        )
        # 定义Accelerator
        cls._accelerate(ak)
        cls._accelerate(bs)
        # 清理数据库
        DbSweeper.cleanup()
        # 调用oncemore
        cls._setup_oncemore()

    @classmethod
    @abstractmethod
    def _setup_oncemore(cls):
        """
        按类进行初始化（补充）

        :return:
        """
        pass

    @abstractmethod
    def _setup_always(self) -> None:
        """
        按测试用例进行初始化

        :return:
        """
        pass

    @staticmethod
    def _accelerate(cls: type, excepts: Optional[list[str]] = None):
        """
        把类下面的方法使用加速器封装

        :param cls:
        :return:
        """
        if excepts is None:
            excepts = []
        for att_name in dir(cls):
            if att_name.startswith("_") or (
                list_is_valid(excepts) and att_name in excepts
            ):
                continue
            setattr(cls, att_name, Accelerator(getattr(cls, att_name)).mock())

    @staticmethod
    def official_select(cls: type, para: Para()):
        """
        从商用数据库中筛选数据

        :param cls:         Handler的类
        :param para:
        :return:
        """
        operator = Operator(RoleType.DbStock)
        handler = cls(operator=operator)
        return handler.select_data(para=para)
