cdef class Node:
    def __init__(self, val):
        self.val = val
        self.size = 1
        self.left = None
        self.right = None

    cdef int left_size(self):
        """
        左子树的size

        :return:
        """
        return 0 if self.left is None else self.left.size

    cdef int right_size(self):
        """
        右子树的size

        :return:
        """
        return 0 if self.right is None else self.right.size