from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import DATE, DATETIME
from buffett.common.tools import dataframe_not_valid
from buffett.download import Para
from buffett.download.handler.tools.table_name import TableNameTool
from buffett.download.mysql import Operator
from buffett.download.types import FreqType


def select_data_slow(operator: Operator, para: Para) -> Optional[DataFrame]:
    """
    SlowHandler按照指定条件在数据库中查找数据

    :param operator:
    :param para:            source, freq, fuquan, code, [start, end]
    :return:
    """
    KEY = DATE if para.comb.freq == FreqType.DAY else DATETIME
    table_name = TableNameTool.get_by_code(para=para)
    df = operator.select_data(name=table_name, span=para.span)
    if dataframe_not_valid(df):
        return
    df.index = df[KEY]
    del df[KEY]
    return df
