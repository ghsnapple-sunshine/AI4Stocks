from buffett.common import create_meta
from buffett.common.constants.col import DATETIME, FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.common.constants.col.analysis import EVENT, VALUE, ANALYSIS
from buffett.common.constants.col.target import CODE
from buffett.download.mysql.types import ColType, AddReqType

"""
Metadata for AnalysisEvent
"""
ANALY_EVENT_META = create_meta(
    meta_list=[
        [DATETIME, ColType.DATE, AddReqType.KEY],
        [EVENT, ColType.SHORT_DESC, AddReqType.KEY],
        [VALUE, ColType.INT32, AddReqType.NONE],
    ]
)

"""
Metadata for ANALY_RCD
"""
ANALY_RCD_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [FREQ, ColType.ENUM_BOOL, AddReqType.KEY],
        [FUQUAN, ColType.ENUM_BOOL, AddReqType.KEY],
        [SOURCE, ColType.ENUM_BOOL, AddReqType.KEY],
        [ANALYSIS, ColType.ENUM_BOOL, AddReqType.KEY],
        [START_DATE, ColType.DATETIME, AddReqType.NONE],
        [END_DATE, ColType.DATETIME, AddReqType.NONE],
    ]
)
