from buffett.common import create_meta
from buffett.common.constants.col import (
    DATETIME,
    FREQ,
    FUQUAN,
    SOURCE,
    START_DATE,
    END_DATE,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
)
from buffett.common.constants.col.analysis import (
    EVENT,
    VALUE,
    ANALYSIS,
    ZF5_MAX,
    ZF5_PCT99,
    ZF5_PCT95,
    ZF5_PCT90,
    DF5_MAX,
    DF5_PCT99,
    DF5_PCT95,
    DF5_PCT90,
    ZF20_MAX,
    ZF20_PCT99,
    ZF20_PCT95,
    ZF20_PCT90,
    DF20_MAX,
    DF20_PCT99,
    DF20_PCT95,
    DF20_PCT90,
)
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
Metadata for AnalysisStat
"""
ANALY_ZDF_META = create_meta(
    meta_list=[
        [DATETIME, ColType.DATE, AddReqType.KEY],
        [ZF5_MAX, ColType.FLOAT, AddReqType.NONE],
        [ZF5_PCT99, ColType.FLOAT, AddReqType.NONE],
        [ZF5_PCT95, ColType.FLOAT, AddReqType.NONE],
        [ZF5_PCT90, ColType.FLOAT, AddReqType.NONE],
        [DF5_MAX, ColType.FLOAT, AddReqType.NONE],
        [DF5_PCT99, ColType.FLOAT, AddReqType.NONE],
        [DF5_PCT95, ColType.FLOAT, AddReqType.NONE],
        [DF5_PCT90, ColType.FLOAT, AddReqType.NONE],
        [ZF20_MAX, ColType.FLOAT, AddReqType.NONE],
        [ZF20_PCT99, ColType.FLOAT, AddReqType.NONE],
        [ZF20_PCT95, ColType.FLOAT, AddReqType.NONE],
        [ZF20_PCT90, ColType.FLOAT, AddReqType.NONE],
        [DF20_MAX, ColType.FLOAT, AddReqType.NONE],
        [DF20_PCT99, ColType.FLOAT, AddReqType.NONE],
        [DF20_PCT95, ColType.FLOAT, AddReqType.NONE],
        [DF20_PCT90, ColType.FLOAT, AddReqType.NONE],
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

"""
Metadata for BS_MIN5
"""
BS_MIN5_META = create_meta(
    meta_list=[
        [DATETIME, ColType.DATETIME, AddReqType.KEY],
        [OPEN, ColType.FLOAT, AddReqType.NONE],
        [CLOSE, ColType.FLOAT, AddReqType.NONE],
        [HIGH, ColType.FLOAT, AddReqType.NONE],
        [LOW, ColType.FLOAT, AddReqType.NONE],
    ]
)
