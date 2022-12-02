from buffett.common import create_meta
from buffett.common.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.common.constants.col.target import CODE
from buffett.common.constants.col.task import CLASS, MODULE
from buffett.download.mysql.types import ColType, AddReqType

"""
Metadata for DL_RCD & RF_RCD
"""
DL_RF_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [FREQ, ColType.ENUM_BOOL, AddReqType.KEY],
        [FUQUAN, ColType.ENUM_BOOL, AddReqType.KEY],
        [SOURCE, ColType.ENUM_BOOL, AddReqType.KEY],
        [START_DATE, ColType.DATETIME, AddReqType.NONE],
        [END_DATE, ColType.DATETIME, AddReqType.NONE],
    ]
)

"""
Metadata for EA_RCD
"""
EA_META = create_meta(
    meta_list=[
        [CLASS, ColType.SHORT_DESC, AddReqType.KEY],
        [MODULE, ColType.SHORT_DESC, AddReqType.KEY],
        [START_DATE, ColType.DATETIME, AddReqType.NONE],
        [END_DATE, ColType.DATETIME, AddReqType.NONE],
    ]
)
