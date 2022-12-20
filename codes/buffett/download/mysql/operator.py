from typing import Optional, Union

from buffett.adapter.pandas import DataFrame
from buffett.adapter.pymysql import IntegrityError
from buffett.common.constants.col.meta import COLUMN, TYPE, ADDREQ, KEY, PRI
from buffett.common.constants.col.my import TABLE_NAME
from buffett.common.constants.col.mysql import FIELD
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_not_valid, list_not_valid, dataframe_is_valid
from buffett.download.mysql.connector import Connector
from buffett.download.mysql.create_parser import CreateSqlTools
from buffett.download.mysql.insert_parser import InsertSqlParser
from buffett.download.mysql.reqcol import ReqCol
from buffett.download.mysql.select_parser import SelectSqlParser
from buffett.download.mysql.types import ColType, AddReqType


class Operator(Connector):
    def create_table(
        self, name: str, meta: DataFrame, if_not_exist=True, update=False
    ) -> None:
        """
        在Mysql中创建表

        :param name:                表名
        :param meta:                表元数据
        :param if_not_exist:        检查：表不存在才创建
        :param update:              检查：表存在时需更新
        :return:
        """
        sql = None
        not_exist = True
        if update:
            curr_meta = self.get_meta(name)
            if dataframe_is_valid(curr_meta):
                not_exist = False
                sql = CreateSqlTools.alter(
                    name=name, curr_meta=curr_meta, new_meta=meta
                )
        if not_exist:
            sql = CreateSqlTools.create(name=name, meta=meta, if_not_exist=if_not_exist)
        if sql is not None:
            self.execute(sql)

    def insert_data(self, name: str, df: DataFrame) -> None:
        """
        插入数据到Mysql表

        :param name:                表名
        :param df:                  数据
        :return:
        """
        if dataframe_not_valid(df):
            return
        sql, vals = InsertSqlParser.insert(name=name, df=df, ignore=False)
        self.execute_many(sql, vals, commit=True)

    def try_insert_data(
        self,
        name: str,
        df: DataFrame,
        meta: Optional[DataFrame] = None,
        update: bool = False,
    ) -> None:
        """
        尝试插入数据到Mysql表

        :param name:                表名
        :param df:                  数据
        :param meta:                表元数据
        :param update:              True: 插入失败则更新; False: 插入失败则跳过
        :return:
        """
        if dataframe_not_valid(df):
            return
        if update:
            sql, vals = InsertSqlParser.insert_n_update(name=name, df=df, meta=meta)
        else:
            sql, vals = InsertSqlParser.insert(name=name, df=df, ignore=True)
        self.execute_many(sql, vals, commit=True)

    def insert_data_safe(
        self,
        name: str,
        df: DataFrame,
        meta: Optional[DataFrame],
        update: bool = False,
    ) -> None:
        """
        先使用insert_data，如果出现keyError，则改用try_insert_data

        :param name:                表名
        :param df:                  数据
        :param meta:                表元数据
        :param update:              True: 插入失败则更新; False: 插入失败则跳过
        :return:
        """
        try:
            self.insert_data(name=name, df=df)
        except IntegrityError as e:
            if e.args[0] == 1062:
                from buffett.common.logger import Logger

                Logger.warning(e)
                self.try_insert_data(name=name, df=df, meta=meta, update=update)
            else:
                raise e

    def drop_table(self, name: str):
        """
        在Mysql中删除表

        :param name:                表名
        :return:
        """
        sql = f"drop table if exists `{name}`"
        self.execute(sql)

    def delete_data(self, name: str):
        """
        删除表格的数据

        :param name:                表名
        :return:
        """
        sql = f"delete from `{name}`"
        self.execute(sql)

    def select_row_num(
        self,
        name: str,
        meta: Optional[DataFrame] = None,
        span: Optional[DateSpan] = None,
        groupby: Optional[list[str]] = None,
    ) -> Union[None, int, DataFrame]:
        """
        查询表格的行数

        :param name:            表名
        :param meta:            表元数据
        :param span:            时间范围
        :param groupby:         聚合条件
        :return:                表格行数
        """
        if meta is None and span is not None:
            meta = self.get_meta(name)
            if meta is None:
                return
        groupby = [] if groupby is None else [ReqCol(x) for x in groupby]

        sql = SelectSqlParser.select(
            name=name,
            cols=[ReqCol.row_num()],
            meta=meta,
            span=span,
            groupby=groupby,
        )
        res = self.execute(sql, fetch=True)
        if list_not_valid(groupby):
            return 0 if dataframe_not_valid(res) else res.iloc[0, 0]
        return res

    def select_data(
        self,
        name: str,
        meta: Optional[DataFrame] = None,
        span: Optional[DateSpan] = None,
    ) -> Optional[DataFrame]:
        """
        查询表格的数据

        :param name:            表名
        :param meta:            表元数据
        :param span:            时间范围
        :return:                表格行数
        """
        if meta is None and span is not None:
            meta = self.get_meta(name)
            if meta is None:
                return
        groupby = []
        sql = SelectSqlParser.select(
            name=name,
            cols=[ReqCol.all()],
            meta=meta,
            span=span,
            groupby=groupby,
        )
        res = self.execute(sql, fetch=True)
        return res

    def get_table_list(self) -> DataFrame:
        """
        获取当前数据库的表格清单

        :return:
        """
        sql = "show tables"
        data = self.execute(sql=sql, fetch=True)
        data.columns = [TABLE_NAME]
        return data

    def get_meta(self, name: str) -> Optional[DataFrame]:
        """
        获取表格的元数据

        :param name:            获取表格的元数据
        :return:
        """
        sql = f"desc {name}"
        data = self.execute(sql=sql, fetch=True)

        if dataframe_not_valid(data):
            return

        data.columns = [x.lower() for x in data.columns]
        df = DataFrame(
            {
                COLUMN: data[FIELD],
                TYPE: data[TYPE].apply(lambda x: ColType.create(x)),
                ADDREQ: data[KEY].apply(
                    lambda x: AddReqType.KEY if x.lower() == PRI else AddReqType.NONE
                ),
            }
        )
        return df
