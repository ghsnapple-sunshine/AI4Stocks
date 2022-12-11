cdef class Node:
    cdef public:
        float val
        int size
        Node left, right

    cdef int left_size(self)
    cdef int right_size(self)