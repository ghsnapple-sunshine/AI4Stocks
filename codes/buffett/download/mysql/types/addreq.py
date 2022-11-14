from buffett.adapter.enum import Enum


class AddReqType(Enum):
    NONE = 1
    KEY = 2

    # UNSIGNED = 3
    # UNSIGNED_KEY = 4

    def sql_format(self):
        COL_ADDREQ_DICT = {AddReqType.NONE: '',
                           AddReqType.KEY: 'NOT NULL'}
        return COL_ADDREQ_DICT[self]

    def is_key(self):
        return self == AddReqType.KEY

    def not_key(self):
        return self != AddReqType.KEY
