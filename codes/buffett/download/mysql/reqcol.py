from __future__ import annotations

from typing import Optional

from buffett.constants.dbcol import ALL, ROW_NUM
from buffett.download.mysql.types import AggType


class ReqCol:
    def __init__(self,
                 field: str,
                 agg: AggType = AggType.NONE,
                 as_field: Optional[str] = None):
        self._field = field
        self._trans = agg
        self._as_field = as_field

    @classmethod
    def all(cls) -> ReqCol:
        return ReqCol('*')

    @classmethod
    def row_num(cls) -> ReqCol:
        return ReqCol('*', AggType.COUNT, ROW_NUM)

    def sql_format(self) -> str:
        field = self.simple_format()
        sql = '%s %s' if self._trans == AggType.NONE else f'{self._trans.sql_format()}(%s) %s'
        as_field = '' if self._as_field is None else f'as `{self._as_field}`'
        sql = sql % (field, as_field)
        return sql

    def simple_format(self) -> str:
        return self._field if self._field == ALL else f'`{self._field}`'


ReqCol.ROW_NUM = ReqCol.row_num()
ReqCol.ALL = ReqCol.all()
