from typing import Optional


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
