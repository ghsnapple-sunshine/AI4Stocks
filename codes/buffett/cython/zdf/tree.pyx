cdef class Tree:
    def __init__(self):
        self._root = None

    cdef Node _right_rotate(self, Node node):
        """
        右旋

        :param node:    Rotate Node
        :return:        Updated Rotate Node
        """
        # 置换节点
        cdef Node new_root = node.left
        node.left = new_root.right
        new_root.right = node
        # 计算size
        new_root.size = node.size
        node.size = node.left_size() + node.right_size() + 1
        return new_root

    cdef Node _left_rotate(self, Node node):
        """
        左旋

        :param node:    Rotate Node
        :return:        Updated Rotate Node
        """
        # 置换节点
        cdef Node new_root = node.right
        node.right = new_root.left
        new_root.left = node
        # 计算size
        new_root.size = node.size
        node.size = node.left_size() + node.right_size() + 1
        return new_root

    cdef Node _maintain(self, Node cur):
        """
        维护以保持树的平衡。

        :param cur:
        :return:
        """
        if cur is None:
            return cur
        cdef int left_size = cur.left_size()
        cdef int left_left_size = 0 if left_size == 0 else cur.left.left_size()
        cdef int left_right_size = 0 if left_size == 0 else cur.left.right_size()
        cdef int right_size = cur.right_size()
        cdef int right_left_size = 0 if right_size == 0 else cur.right.left_size()
        cdef int right_right_size = 0 if right_size == 0 else cur.right.right_size()

        if left_left_size > right_size:  # LL型
            cur = self._right_rotate(cur)  # 右旋
            cur.right = self._maintain(cur.right)
            cur = self._maintain(cur)
        elif left_right_size > right_size:  # LR型
            cur.left = self._left_rotate(cur.left)
            cur = self._right_rotate(cur)
            cur.left = self._maintain(cur.left)
            cur.right = self._maintain(cur.right)
            cur = self._maintain(cur)
        elif right_left_size > left_size:  # RL型
            cur.right = self._right_rotate(cur.right)
            cur = self._left_rotate(cur)
            cur.left = self._maintain(cur.left)
            cur.right = self._maintain(cur.right)
            cur = self._maintain(cur)
        elif right_right_size > left_size:  # RR型
            cur = self._left_rotate(cur)
            cur.left = self._maintain(cur.left)
            cur = self._maintain(cur)
        return cur

    cdef void add(self, float val):
        """
        向树中添加元素

        :param val:
        :return:
        """
        self._root = self._add(self._root, val)

    cdef Node _add(self, Node cur, float val):
        if cur is None:
            return Node(val)
        # else
        cur.size += 1
        if val < cur.val:
            cur.left = self._add(cur.left, val)
        else:
            cur.right = self._add(cur.right, val)
        return self._maintain(cur)

    cdef void delete(self, float val):
        """
        在树中删除目标值
        如果不能确保树中包含待删除的元素，请使用delete_safe

        :param val:     Target Value(Object) to delete
        :return:
        """
        self._root = self._delete(self._root, val)

    cdef void delete_safe(self, float val):
        """
        在树中删除目标值

        :param val:     Target Value(Object) to delete
        :return:
        """
        if self.contains(val):
            self._root = self._delete(self._root, val)

    cdef Node _delete(self, Node cur, float val):
        """
        删除目标值

        :param cur:     Search Node
        :param val:     Target Value(Object) to delete
        :return:        Updated Search Node
        """
        cdef Node pre, des

        cur.size -= 1
        if cur.val > val:
            cur.left = self._delete(cur.left, val)
        elif cur.val < val:
            cur.right = self._delete(cur.right, val)
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
                # pre = None（Not necessary assign）
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
                des.size = des.left.size + des.right_size() + 1  # 左子树无需判断，右子树需判断
                cur = des

        cur = self._maintain(cur)
        return cur

    cdef bint contains(self, float val):
        """
        在树中查找目标值

        :param val:     Target value(object) to lookup
        :return:        contains or not
        """
        return self._contains(self._root, val)

    cdef bint _contains(self, Node cur, float val):
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
            return self._contains(cur.left, val)
        return self._contains(cur.right, val)

    cdef float get_nth(self, int n):
        """
        查找树中第n个元素

        :param n:       the n(th) value(object) to lookup
                        n=0 indicates the first value(object)
                        n can be minus, e.g. get_n(-1) means get the last value(object).
        :return:
        """
        if self._root is None:
            raise ValueError("Empty Size Balanced Tree to get.")
        cdef int size = self._root.size
        if n >= size or n <= -size:
            raise ValueError("Exceed the size of the Size Balanced Tree.")
        n = (n + size) % size
        return self._get_nth(self._root, n).val

    cdef Node _get_nth(self, Node cur, int n):
        """
        查找第n个元素

        :param cur:     Search Node
        :param n:       the n(th) value(object) to lookup
                        n=0 indicates the first value(object)
        :return:
        """
        cdef int left_size = cur.left_size()
        if left_size == n:
            return cur
        elif left_size > n:
            return self._get_nth(cur.left, n)
        # elif left_size < n:
        return self._get_nth(cur.right, n - left_size - 1)

    property is_empty:
        def __get__(self):
            """
            判断树是否为空

            :return:
            """
            return self._root is not None

    property not_empty:
        def __get__(self):
            """
            判断树是否非空

            :return:
            """
            return self._root is None

    property size:
        def __get__(self):
            """
            获取树的size

            :return:
            """
            return 0 if self._root is None else self._root.size