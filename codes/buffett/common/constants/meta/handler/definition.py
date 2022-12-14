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
    PRECLOSE,
    TYPE,
    PXBLc,
    SGBLc,
    PGBLc,
    PGJGc,
    ZFBLc,
    ZFGSc,
    ZFJGc,
    A,
    B,
    MO,
    M0TB,
    M0HB,
    M1,
    M1TB,
    M1HB,
    M2,
    M2TB,
    M2HB,
    ST,
    TP,
)
from buffett.common.constants.col.my import USE
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
DC_DAILY_META = create_meta(
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
BS_DAILY_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [OPEN, ColType.FLOAT, AddReqType.NONE],
        [CLOSE, ColType.FLOAT, AddReqType.NONE],
        [HIGH, ColType.FLOAT, AddReqType.NONE],
        [LOW, ColType.FLOAT, AddReqType.NONE],
        [PRECLOSE, ColType.FLOAT, AddReqType.NONE],
        [CJL, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
        [CJE, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
        [ZF, ColType.FLOAT, AddReqType.NONE],
        [ZDF, ColType.FLOAT, AddReqType.NONE],
        [ZDE, ColType.FLOAT, AddReqType.NONE],
        [HSL, ColType.FLOAT, AddReqType.NONE],
        [TP, ColType.ENUM_BOOL, AddReqType.NONE],
        [ST, ColType.ENUM_BOOL, AddReqType.NONE],
    ]
)


"""
Metadata for THS_STOCK_DAYINFO 
"""
TH_DAILY_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [OPEN, ColType.FLOAT, AddReqType.NONE],
        [CLOSE, ColType.FLOAT, AddReqType.NONE],
        [HIGH, ColType.FLOAT, AddReqType.NONE],
        [LOW, ColType.FLOAT, AddReqType.NONE],
        [CJL, ColType.BIGINT_UNSIGNED, AddReqType.NONE],
    ]
)

"""
Metadata for DC_STOCK_MIN5INFO
"""
DC_MINUTE_META = DC_DAILY_META  # TODO:Seems key should be 'DATETIME'?

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
CAL_META = create_meta(meta_list=[[DATE, ColType.DATE, AddReqType.KEY]])

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


"""
Metadata for stock_fhpg
"""
STK_FHPG_META = create_meta(
    meta_list=[
        [CODE, ColType.CODE, AddReqType.KEY],
        [DATE, ColType.DATE, AddReqType.KEY],
        [TYPE, ColType.ENUM_BOOL, AddReqType.KEY],
        [PXBLc, ColType.FLOAT, AddReqType.NONE],
        [SGBLc, ColType.FLOAT, AddReqType.NONE],
        [PGBLc, ColType.FLOAT, AddReqType.NONE],
        [PGJGc, ColType.FLOAT, AddReqType.NONE],
        [ZFBLc, ColType.FLOAT, AddReqType.NONE],
        [ZFGSc, ColType.FLOAT, AddReqType.NONE],
        [ZFJGc, ColType.FLOAT, AddReqType.NONE],
    ]
)


"""
Metadata for money_supply
"""
MONEY_SPLY_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [MO, ColType.FLOAT, AddReqType.NONE],
        [M0TB, ColType.FLOAT, AddReqType.NONE],
        [M0HB, ColType.FLOAT, AddReqType.NONE],
        [M1, ColType.FLOAT, AddReqType.NONE],
        [M1TB, ColType.FLOAT, AddReqType.NONE],
        [M1HB, ColType.FLOAT, AddReqType.NONE],
        [M2, ColType.FLOAT, AddReqType.NONE],
        [M2TB, ColType.FLOAT, AddReqType.NONE],
        [M2HB, ColType.FLOAT, AddReqType.NONE],
    ]
)


"""
Metadata for daily_maintain
"""
DAILY_MTAIN_META = create_meta(
    meta_list=[
        [DATE, ColType.DATE, AddReqType.KEY],
        [CODE, ColType.CODE, AddReqType.KEY],
        [USE, ColType.ENUM_BOOL, AddReqType.NONE],
    ]
)
