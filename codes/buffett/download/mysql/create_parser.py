from typing import Optional

from buffett.adapter.pandas import DataFrame, pd, Series
from buffett.common.constants.col.meta import COLUMN, TYPE, ADDREQ

CURR_TYPE = TYPE + '_l'
CURR_ADDREQ = ADDREQ + '_l'


class CreateSqlTools:
    """
    创建调用Operator.create_table方法下的sql
    """

    @staticmethod
    def create(name: str,
               meta: DataFrame,
               if_not_exist=True) -> str:
        """
        create table
        """
        cols = [CreateSqlTools._describe(row) for index, row in meta.iterrows()]

        prim_key = meta[meta[ADDREQ].apply(lambda x: x.is_key())][COLUMN]
        if not prim_key.empty:
            apd = 'primary key ({0})'.format(
                ','.join([f'`{x}`' for x in prim_key]))
            cols.append(apd)

        joinCols = ','.join(cols)
        str_if_exist = 'if not exists ' if if_not_exist else ''
        sql = 'create table {0}`{1}` ({2})'.format(str_if_exist, name, joinCols)
        return sql

    @staticmethod
    def alter(name: str,
              curr_meta: DataFrame,
              new_meta: DataFrame) -> Optional[str]:
        """
        alter table
        """
        comb_meta = pd.merge(curr_meta, new_meta, how='outer', on=[COLUMN], suffixes=['_l', ''])

        diffs = []
        for index, row in comb_meta.iterrows():
            if pd.isna(row[CURR_TYPE]):  # 变更前不存在
                diffs.append(f'add column {CreateSqlTools._describe(row)}')
            elif pd.isna(row[TYPE]):  # 变更后不存在
                diffs.append(f'drop column `{row[COLUMN]}`')
            elif row[CURR_TYPE] != row[TYPE] or row[CURR_ADDREQ] != row[ADDREQ]:  # 有变更
                diffs.append(f'modify column {CreateSqlTools._describe(row)}')
                # 否则什么都不做

        if len(diffs) == 0:
            return None
        sql = 'alter table `{0}` {1}'.format(
            name,
            ','.join(diffs))
        return sql

    @staticmethod
    def _describe(row: Series) -> str:
        return f'`{row[COLUMN]}` {row[TYPE].sql_format()} {row[ADDREQ].sql_format()}'
