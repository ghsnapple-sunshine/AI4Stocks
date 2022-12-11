from node cimport Node


cdef class Tree:
    cdef Node _root

    cdef void add(self, float val)
    cdef Node _add(self, Node cur, float val)

    cdef void delete(self, float val)
    cdef void delete_safe(self, float val)
    cdef Node _delete(self, Node cur, float val)

    cdef bint contains(self, float val)
    cdef bint _contains(self, Node cur, float val)

    cdef float get_nth(self, int n)
    cdef Node _get_nth(self, Node cur, int n)

    cdef Node _right_rotate(self, Node node)
    cdef Node _left_rotate(self, Node node)
    cdef Node _maintain(self, Node cur)
