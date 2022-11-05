from pandas import DataFrame

from buffett.common.tools import dataframe_not_valid
from buffett.constants.meta import COLUMN, TYPE, ADDREQ
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType, ColType, AddReqType


class TableManager:
    def __init__(self, operator):
        self._operator = Operator(role=RoleType.DbInfo)
        self._db = operator.db

    def get_meta(self, name: str) -> DataFrame:
        sql = f"SELECT `COLUMN_NAME` AS `{COLUMN}`, `COLUMN_TYPE` AS `{TYPE}`, `COLUMN_KEY` AS `{ADDREQ}` " \
              f"FROM `COLUMNS` WHERE table_schema = '{self._db}' AND TABLE_NAME='{name}'"
        data = self._operator.execute(sql=sql, fetch=True)

        if dataframe_not_valid(data):
            return DataFrame()

        data[TYPE] = data[TYPE].apply(lambda x: ColType.create(x))
        data[ADDREQ] = data[ADDREQ].apply(lambda x: AddReqType.KEY if x == 'PRI' else AddReqType.NONE)
        return data
