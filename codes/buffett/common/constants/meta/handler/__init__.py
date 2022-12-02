from buffett.common import create_meta
from buffett.common.constants.col import (
    DATE,
    OPEN,
    CLOSE,
    HIGH,
    LOW,
    CJL,
    CJE,
    ZF,
    ZDF,
    ZDE,
    HSL,
    DATETIME,
    LB,
    DSYL,
    SJL,
    ZSZ,
    LTSZ,
    SYL,
    SXL,
    DSXL,
    GXL,
    DGXL,
    SGZGc,
    SGc,
    ZGc,
    XJc,
    GXLc,
    SYc,
    JZCc,
    GJJc,
    WFPc,
    LRZZc,
    ZGBc,
    GGRc,
    DJRc,
    CXRc,
    JDc,
    ZXGGRc,
    START_DATE,
    END_DATE,
)
from buffett.common.constants.col.target import (
    INDEX_CODE,
    INDEX_NAME,
    CONCEPT_CODE,
    CONCEPT_NAME,
    CODE,
    NAME,
    INDUSTRY_CODE,
    INDUSTRY_NAME,
)
from buffett.common.constants.col.task import (
    TASK_ID,
    PARENT_ID,
    CLASS,
    MODULE,
    CREATE_TIME,
    START_TIME,
    END_TIME,
    SUCCESS,
    ERR_MSG,
)
from buffett.download.mysql.types import AddReqType, ColType

"""
Metadata for STK_LS & LG_STK_LS
"""
STK_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [NAME, ColType.STOCK_NAME, AddReqType.NONE],
    ]
)

"""
Metadata for BS_STK_LS
"""
BS_STK_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [NAME, ColType.STOCK_NAME, AddReqType.NONE],
        [START_DATE, ColType.DATE, AddReqType.NONE],
        [END_DATE, ColType.DATE, AddReqType.NONE],
    ]
)

"""
Metadata for DC_STOCK_DAYINFO 
"""
AK_DAILY_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [OPEN, ColType.FLOAT, AddReqType.NONE],
        [CLOSE, ColType.FLOAT, AddReqType.NONE],
        [HIGH, ColType.FLOAT, AddReqType.NONE],
        [LOW, ColType.FLOAT, AddReqType.NONE],
        [CJL, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
        [CJE, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
        [ZF, ColType.FLOAT, AddReqType.NONE],
        [ZDF, ColType.FLOAT, AddReqType.NONE],
        [ZDE, ColType.FLOAT, AddReqType.NONE],
        [HSL, ColType.FLOAT, AddReqType.NONE],
    ]
)

"""
Metadata for BS_STOCK_DAYINFO
"""
BS_DAILY_META = AK_DAILY_META  # TODO:Seems key should be 'DATETIME'?

"""
Metadata for DC_STOCK_MIN5INFO
"""
AK_MINUTE_META = AK_DAILY_META  # TODO:Seems key should be 'DATETIME'?

"""
Metadata for BS_STOCK_MIN5INFO
"""
BS_MINUTE_META = create_meta(
    meta_list=[
        [DATETIME, ColType.DATETIME, AddReqType.KEY],
        [OPEN, ColType.FLOAT, AddReqType.NONE],
        [CLOSE, ColType.FLOAT, AddReqType.NONE],
        [HIGH, ColType.FLOAT, AddReqType.NONE],
        [LOW, ColType.FLOAT, AddReqType.NONE],
        [CJL, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
        [CJE, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
    ]
)

"""
Metadata for STK_RT
"""
STK_RT_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [DATETIME, ColType.DATETIME, AddReqType.KEY],
        [OPEN, ColType.FLOAT, AddReqType.NONE],
        [CLOSE, ColType.FLOAT, AddReqType.NONE],
        [HIGH, ColType.FLOAT, AddReqType.NONE],
        [LOW, ColType.FLOAT, AddReqType.NONE],
        [CJL, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
        [CJE, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
        [ZF, ColType.FLOAT, AddReqType.NONE],
        [ZDF, ColType.FLOAT, AddReqType.NONE],
        [ZDE, ColType.FLOAT, AddReqType.NONE],
        [HSL, ColType.FLOAT, AddReqType.NONE],
        [LB, ColType.FLOAT, AddReqType.NONE],
        [DSYL, ColType.FLOAT, AddReqType.NONE],
        [SJL, ColType.FLOAT, AddReqType.NONE],
        [ZSZ, ColType.FLOAT, AddReqType.NONE],
        [LTSZ, ColType.FLOAT, AddReqType.NONE],
    ]
)

"""
Metadata for STK_DVD
"""
STK_DVD_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [SGZGc, ColType.FLOAT, AddReqType.NONE],
        [SGc, ColType.FLOAT, AddReqType.NONE],
        [ZGc, ColType.FLOAT, AddReqType.NONE],
        [XJc, ColType.FLOAT, AddReqType.NONE],
        [GXLc, ColType.FLOAT, AddReqType.NONE],
        [SYc, ColType.FLOAT, AddReqType.NONE],
        [JZCc, ColType.FLOAT, AddReqType.NONE],
        [GJJc, ColType.FLOAT, AddReqType.NONE],
        [WFPc, ColType.FLOAT, AddReqType.NONE],
        [LRZZc, ColType.FLOAT, AddReqType.NONE],
        [ZGBc, ColType.FLOAT, AddReqType.NONE],
        [GGRc, ColType.DATE, AddReqType.KEY],
        [DJRc, ColType.DATE, AddReqType.NONE],
        [CXRc, ColType.DATE, AddReqType.NONE],
        [JDc, ColType.MINI_DESC, AddReqType.NONE],
        [ZXGGRc, ColType.DATE, AddReqType.NONE],
    ]
)

"""
Metadata for INDEX_LS
"""
INDEX_META = create_meta(
    meta_list=[
        [INDEX_CODE, ColType.CODE, AddReqType.KEY],
        [INDEX_NAME, ColType.INDEX_NAME, AddReqType.NONE],
    ]
)

"""
Metadata for INDUS_LS
"""
INDUS_META = create_meta(
    meta_list=[
        [INDUSTRY_CODE, ColType.CODE, AddReqType.KEY],
        [INDUSTRY_NAME, ColType.CONCEPT_NAME, AddReqType.NONE],
    ]
)

"""
Metadata for INDUS_CONS
"""
INDUS_CONS_META = create_meta(
    meta_list=[
        [INDUSTRY_CODE, ColType.CODE, AddReqType.KEY],
        [INDUSTRY_NAME, ColType.CONCEPT_NAME, AddReqType.NONE],
        [CODE, ColType.CODE, AddReqType.KEY],
        [NAME, ColType.STOCK_NAME, AddReqType.NONE],
    ]
)

"""
Metadata for CNCP_LS
"""
CNCP_META = create_meta(
    meta_list=[
        [CONCEPT_CODE, ColType.CODE, AddReqType.KEY],
        [CONCEPT_NAME, ColType.CONCEPT_NAME, AddReqType.NONE],
    ]
)

"""
Metadata for CNCP_CONS
"""
CNCP_CONS_META = create_meta(
    meta_list=[
        [CONCEPT_CODE, ColType.CODE, AddReqType.KEY],
        [CONCEPT_NAME, ColType.CONCEPT_NAME, AddReqType.NONE],
        [CODE, ColType.CODE, AddReqType.KEY],
        [NAME, ColType.STOCK_NAME, AddReqType.NONE],
    ]
)

"""
Metadata for PEPB_DAILY_INFO
"""
PEPB_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [SYL, ColType.FLOAT, AddReqType.NONE],
        [DSYL, ColType.FLOAT, AddReqType.NONE],
        [SJL, ColType.FLOAT, AddReqType.NONE],
        [SXL, ColType.FLOAT, AddReqType.NONE],
        [DSXL, ColType.FLOAT, AddReqType.NONE],
        [GXL, ColType.FLOAT, AddReqType.NONE],
        [DGXL, ColType.FLOAT, AddReqType.NONE],
        [ZSZ, ColType.FLOAT, AddReqType.NONE],
    ]
)

"""
Metadata for TRA_CAL
"""
CAL_META = create_meta(meta_list=[[DATE, ColType.DATE, AddReqType.NONE]])

"""
Metadata for TASK_RCD
"""
TASK_META = create_meta(
    meta_list=[
        [TASK_ID, ColType.INT32, AddReqType.KEY],
        [PARENT_ID, ColType.INT32, AddReqType.NONE],
        [CLASS, ColType.SHORT_DESC, AddReqType.NONE],
        [MODULE, ColType.SHORT_DESC, AddReqType.NONE],
        [CREATE_TIME, ColType.DATETIME, AddReqType.NONE],
        [START_TIME, ColType.DATETIME, AddReqType.NONE],
        [END_TIME, ColType.DATETIME, AddReqType.NONE],
        [SUCCESS, ColType.ENUM_BOOL, AddReqType.NONE],
        [ERR_MSG, ColType.LONG_DESC, AddReqType.NONE],
    ]
)
