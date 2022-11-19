from typing import Optional

from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import DATE, DATETIME
from buffett.common.constants.col.meta import COLUMN
from buffett.common.pendulum import DateSpan
from buffett.common.tools import list_not_valid
from buffett.download.mysql.reqcol import ReqCol


class SelectSqlParser:
    @staticmethod
    def select(
        name, cols: list[ReqCol], meta: DataFrame, span: DateSpan, groupby: list[ReqCol]
    ):
        """
        select

        :param name:            表名
        :param cols:            列名
        :param meta:            表元数据
        :param span:            时间区间
        :param groupby:         分组
        :return:
        """
        cols.extend(groupby)
        cols_str = ",".join([x.sql_format() for x in cols])
        where_str = "" if span is None else SelectSqlParser._span(meta=meta, span=span)
        groupby_str = (
            "" if list_not_valid(groupby) else SelectSqlParser._groupby(groupby=groupby)
        )
        sql = f"select {cols_str} from `{name}` {where_str} {groupby_str}"
        return sql

    @staticmethod
    def _span(meta: DataFrame, span: Optional[DateSpan]) -> str:
        """
        扩展sql: where

        :param meta:            表元数据
        :param span:            时间区间
        :return:
        """
        key = DATE if DATE in meta[COLUMN].values else DATETIME
        start_valid = span.start is not None
        end_valid = span.end is not None
        if start_valid and end_valid:
            return f"where `{key}` >= '{span.start}' and `{key}` < '{span.end}'"
        elif start_valid:
            return f"where `{key}` >= '{span.start}'"
        elif end_valid:
            return f"where `{key}` < '{span.end}'"
        return ""

    @staticmethod
    def _groupby(groupby: list[ReqCol]) -> str:
        """
        扩展sql：groupby

        :param groupby:         需要groupby的列
        :return:
        """
        sql = ",".join([x.simple_format() for x in groupby])
        sql = f"group by {sql}"
        return sql
