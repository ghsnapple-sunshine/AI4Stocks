from __future__ import annotations

from typing import Optional


class SizeBalancedTree:
    def __init__(self):
        self._root: Optional[Node] = None

    @classmethod
    def _right_rotate(cls, node: Node) -> Node:
        """
        右旋

        :param node:    Rotate Node
        :return:        Updated Rotate Node
        """
        # 置换节点
        new_root = node.left
        node.left = new_root.right
        new_root.right = node
        # 计算size
        new_root.size = node.size
        node.size = node.left_size + node.right_size + 1
        return new_root

    @classmethod
    def _left_rotate(cls, node: Node) -> Node:
        """
        左旋

        :param node:    Rotate Node
        :return:        Updated Rotate Node
        """
        # 置换节点
        new_root = node.right
        node.right = new_root.left
        new_root.left = node
        # 计算size
        new_root.size = node.size
        node.size = node.left_size + node.right_size + 1
        return new_root

    @classmethod
    def _maintain(cls, cur: Optional[Node]) -> Optional[Node]:
        """
        维护以保持树的平衡。

        :param cur:
        :return:
        """
        if cur is None:
            return
        left_size = cur.left_size
        left_left_size = 0 if left_size == 0 else cur.left.left_size
        left_right_size = 0 if left_size == 0 else cur.left.right_size
        right_size = cur.right_size
        right_left_size = 0 if right_size == 0 else cur.right.left_size
        right_right_size = 0 if right_size == 0 else cur.right.right_size

        if left_left_size > right_size:  # LL型
            cur = cls._right_rotate(cur)  # 右旋
            cur.right = cls._maintain(cur.right)
            cur = cls._maintain(cur)
        elif left_right_size > right_size:  # LR型
            cur.left = cls._left_rotate(cur.left)
            cur = cls._right_rotate(cur)
            cur.left = cls._maintain(cur.left)
            cur.right = cls._maintain(cur.right)
            cur = cls._maintain(cur)
        elif right_left_size > left_size:  # RL型
            cur.right = cls._right_rotate(cur.right)
            cur = cls._left_rotate(cur)
            cur.left = cls._maintain(cur.left)
            cur.right = cls._maintain(cur.right)
            cur = cls._maintain(cur)
        elif right_right_size > left_size:  # RR型
            cur = cls._left_rotate(cur)
            cur.left = cls._maintain(cur.left)
            cur = cls._maintain(cur)
        return cur

    def add(self, val):
        """
        向树中添加元素

        :param val:
        :return:
        """
        self._root = self._add(self._root, val)

    @classmethod
    def _add(cls, cur: Optional[Node], val) -> Optional[Node]:
        if cur is None:
            return Node(val)
        # else
        cur.size += 1
        if val < cur.val:
            cur.left = cls._add(cur.left, val)
        else:
            cur.right = cls._add(cur.right, val)
        return cls._maintain(cur)

    def delete(self, val):
        """
        在树中删除目标值
        如果不能确保树中包含待删除的元素，请使用delete_safe

        :param val:     Target Value(Object) to delete
        :return:
        """
        self._root = self._delete(self._root, val)

    def delete_safe(self, val):
        """
        在树中删除目标值

        :param val:     Target Value(Object) to delete
        :return:
        """
        if self.contains(val):
            self._root = self._delete(self._root, val)

    @classmethod
    def _delete(cls, cur: Node, val) -> Optional[Node]:
        """
        删除目标值

        :param cur:     Search Node
        :param val:     Target Value(Object) to delete
        :return:        Updated Search Node
        """
        cur.size -= 1
        if cur.val > val:
            cur.left = cls._delete(cur.left, val)
        elif cur.val < val:
            cur.right = cls._delete(cur.right, val)
        else:
            left_valid = cur.left is not None
            right_valid = cur.right is not None
            if not left_valid and not right_valid:  # 没有左右子树
                cur = None
            elif not left_valid and right_valid:  # 只有右子树
                cur = cur.right
            elif left_valid and not right_valid:  # 只有左子树
                cur = cur.left
            else:  # 左右子树都有
                pre = None
                des = cur.right
                des.size -= 1
                while des.left is not None:
                    pre = des
                    des = des.left
                    des.size -= 1
                if pre is not None:
                    pre.left = des.right
                    des.right = cur.right  # 将des节点，替换cur节点
                des.left = cur.left  # 连接原先的左子树
                des.size = des.left.size + des.right_size + 1  # 左子树无需判断，右子树需判断
                cur = des

        cur = cls._maintain(cur)
        return cur

    def contains(self, val):
        """
        在树中查找目标值

        :param val:     Target value(object) to lookup
        :return:        contains or not
        """
        return self._contains(self._root, val)

    @classmethod
    def _contains(cls, cur: Node, val) -> bool:
        """
        查找目标值

        :param cur:     Search Node
        :param val:     Target value(object) to lookup
        :return:        contains or not
        """
        if cur is None:
            return False
        if cur.val == val:
            return True
        if cur.val > val:
            return cls._contains(cur.left, val)
        return cls._contains(cur.right, val)

    def get_nth(self, n: int):
        """
        查找树中第n个元素

        :param n:       the n(th) value(object) to lookup
                        n=0 indicates the first value(object)
                        n can be minus, e.g. get_n(-1) means get the last value(object).
        :return:
        """
        if self._root is None:
            raise ValueError("Empty Size Balanced Tree to get.")
        size = self._root.size
        if n >= size or n <= -size:
            raise ValueError("Exceed the size of the Size Balanced Tree.")
        n = (n + size) % size
        return self._get_n(self._root, n).val

    @classmethod
    def _get_n(cls, cur: Node, n: int) -> Node:
        """
        查找第n个元素

        :param cur:     Search Node
        :param n:       the n(th) value(object) to lookup
                        n=0 indicates the first value(object)
        :return:
        """
        left_size = cur.left_size
        if left_size == n:
            return cur
        elif left_size > n:
            return cls._get_n(cur.left, n)
        # elif left_size < n:
        return cls._get_n(cur.right, n - left_size - 1)

    @property
    def is_empty(self):
        """
        判断树是否为空

        :return:
        """
        return self._root is not None

    @property
    def not_empty(self):
        """
        判断树是否非空

        :return:
        """
        return self._root is None

    @property
    def size(self):
        return 0 if self._root is None else self._root.size


class Node:
    def __init__(self, val):
        self.val = val
        self.size = 1
        self.left: Optional[Node] = None
        self.right: Optional[Node] = None

    @property
    def left_size(self):
        """
        左子树的size

        :return:
        """
        return 0 if self.left is None else self.left.size

    @property
    def right_size(self):
        """
        右子树的size

        :return:
        """
        return 0 if self.right is None else self.right.size

    def __str__(self):
        left_val = None if self.left is None else self.left.val
        right_val = None if self.right is None else self.right.val
        return f"Node:{self.val}, left:{left_val}, right:{right_val}"
