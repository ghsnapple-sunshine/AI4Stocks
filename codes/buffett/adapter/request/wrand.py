from random import Random

from buffett.adapter.numpy import ndarray, np


class Wrand:
    _INIT = 2**20
    _COUNTER = 20

    def __init__(self, items: ndarray):
        items = np.unique(items)
        self._items = items
        self._num = len(items)
        self._order = dict((v, k) for k, v in enumerate(items))
        self._weights = np.array([1] * self._num) * self._INIT
        self._w_sums = np.concatenate(
            [np.arange(1, self._num + 1) * self._INIT, np.arange(self._num)]
        ).reshape((self._num, 2), order="F")
        self._random = Random()

    def random_item(self):
        """
        随机获取一个对象

        :return:
        """
        total = sum(self._weights)
        rand = self._random.random() * total
        order = self._w_sums[self._w_sums[:, 0] >= rand][0, 1]
        return self._items[order]

    def get_weight(self, item: str):
        """
        获取weight

        :param item:
        :return:
        """
        order = self._order[item]
        return self._weights[order]

    def set_weight(self, item: str, value: int):
        """
        设置weight

        :param item:
        :param value:
        :return:
        """
        order = self._order[item]
        diff = value - self._weights[order]
        self._weights[order] = value
        self._w_sums[order:, 0] = self._w_sums[order:, 0] + diff

    def reset_weight(self, item: str):
        """
        重置weight

        :param item:
        :return:
        """
        return self.set_weight(item, self._INIT)
