from abc import abstractmethod
from typing import final, Optional
from unittest import TestCase

from buffett.adapter.datacompy import compare
from buffett.adapter.pandas import DataFrame, pd
from buffett.common.magic import get_class


class SimpleTester(TestCase):
    _prepared = False

    @final
    def setUp(self):
        """
        unittest框架调用的setup方法

        :return:
        """
        cls = get_class(self)
        if not cls._prepared:
            self._setup_once()
            cls._prepared = True
        self._setup_always()

    @classmethod
    @abstractmethod
    def _setup_once(cls):
        """
        按类进行初始化

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

    def tearDown(self) -> None:
        """
        清理用例现场

        :return:
        """
        pass

    @staticmethod
    def compare_dataframe(df1: DataFrame, df2: DataFrame):
        """
        比较两个dataframe是否相同

        :param df1:
        :param df2:
        :return:
        """
        cmp = pd.concat([df1, df2]).drop_duplicates(keep=False).empty
        return cmp

    @staticmethod
    def dataframe_almost_equals(
        df1: DataFrame,
        df2: DataFrame,
        join_columns: Optional[list[str]] = None,
        on_index: bool = False,
        rel_tol: float = 1e-5,
    ):
        res = compare(
            df1, df2, join_columns=join_columns, on_index=on_index, rel_tol=rel_tol
        )
        return res.matches()
