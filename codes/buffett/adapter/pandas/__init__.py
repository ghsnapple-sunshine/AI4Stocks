from pandas import DataFrame as pd_DataFrame
from pandas import Series as pd_Series
from pandas import concat as pd_concat
from pandas import isna as pd_isna
from pandas import merge as pd_merge
from pandas import to_datetime as pd_to_datetime

# constants

# types
DataFrame = pd_DataFrame
Series = pd_Series


class pd:
    # methods
    concat = pd_concat
    isna = pd_isna
    merge = pd_merge
    to_datetime = pd_to_datetime
