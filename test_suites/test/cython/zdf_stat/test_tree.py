import numpy as np

from buffett.cython.zdf.origin import Tree
from test import SimpleTester


class TestTree(SimpleTester):
    @classmethod
    def _setup_once(cls):
        pass

    def _setup_always(self) -> None:
        pass

    def test_add(self):
        """
        测试基本添加

        :return:
        """
        tree = Tree()
        for i in range(0, 10):
            tree.add(i)
            assert tree.size == i + 1
        for i in range(0, 10):
            assert tree.get_nth(i) == i

    def test_add_repeat_data(self):
        """
        测试往树里添加相同的数据

        :return:
        """
        tree = Tree()
        for i in np.linspace(0, 9.5, num=20, dtype=int):
            tree.add(i)
        assert tree.size == 20

    def test_delete(self):
        """
        测试基本删除

        :return:
        """
        tree = Tree()
        for i in range(0, 10):
            tree.add(i)
        for i in range(0, 10):
            tree.delete(i)
            assert tree.size == 9 - i

    def test_get_nth(self):
        """
        测试get_nth

        :return:
        """
        datas = [9, 4, 5, 1, 18, 20, 1]
        tree = self._init_tree(datas)
        sort_datas = list(np.sort(datas))
        tree_datas = [tree.get_nth(i) for i in range(0, tree.size)]
        assert tree_datas == sort_datas

    def test_modify(self):
        """
        现网问题镜像

        :return:
        """
        tree = self._init_tree([2730.61, 2759.86, 2793.99, 2797.24, 2803.74])
        tree.delete(2797.24)
        assert [tree.get_nth(i) for i in range(0, tree.size)] == [
            2730.61,
            2759.86,
            2793.99,
            2803.74,
        ]

    @classmethod
    def _init_tree(cls, ls: list):
        tree = Tree()
        for d in ls:
            tree.add(d)
        return tree
