from node cimport Node


cdef class Tree:
    cdef Node _root

    # cdef void add(self, double val)
    cpdef void add(self, double val)
    cdef Node _add(self, Node cur, double val)

    # cdef void delete(self, double val)
    cpdef void delete(self, double val)
    cdef void delete_safe(self, double val)
    cdef Node _delete(self, Node cur, double val)

    cdef bint contains(self, double val)
    cdef bint _contains(self, Node cur, double val)

    cdef double get_nth(self, int n)
    cdef Node _get_nth(self, Node cur, int n)

    cdef Node _right_rotate(self, Node node)
    cdef Node _left_rotate(self, Node node)
    cdef Node _maintain(self, Node cur)
