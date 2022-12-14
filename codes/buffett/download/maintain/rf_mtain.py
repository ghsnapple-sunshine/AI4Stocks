from typing import Optional

from buffett.adapter.pandas import DataFrame, pd, Series
from buffett.common.constants.col import FREQ, SOURCE, FUQUAN, START_DATE, END_DATE
from buffett.common.constants.col.my import TABLE_NAME, DORCD_START, DORCD_END
from buffett.common.constants.col.mysql import ROW_NUM
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.handler import META_DICT
from buffett.common.logger import Logger
from buffett.common.pendulum import DateSpan
from buffett.common.tools import dataframe_not_valid, dataframe_is_valid
from buffett.download import Para
from buffett.download.handler.tools import TableNameTool
from buffett.download.maintain.base import BaseMaintain
from buffett.download.mysql import Operator
from buffett.download.recorder import DownloadRecorder, ReformRecorder

DL_ROW_NUM = "dl_row_num"


class ReformMaintain(BaseMaintain):
    def __init__(self, operator: Operator):
        super(ReformMaintain, self).__init__(folder="rf_maintain")
        self._operator = operator
        self._dl_recorder = DownloadRecorder(operator)
        self._rf_recorder = ReformRecorder(operator)
        self._dl_records = None
        self._rf_records = None
        self._logger = ReformMaintainLogger()

    def run(self, save: bool = True) -> Optional[DataFrame]:
        """
        检查Reform的结果

        :param save:
        :return:
        """
        self._dl_records = self._dl_recorder.select_data()
        self._rf_records = self._rf_recorder.select_data()
        if dataframe_not_valid(self._dl_records) or dataframe_not_valid(
            self._rf_records
        ):
            return True if save else None
        result = pd.concat_safe([self._region_check(), self._datanum_check(save)])
        # 返回结果
        return dataframe_not_valid(result) if save else result

    def _region_check(self) -> Optional[DataFrame]:
        """
        检查reform_records是否有超出dl_records范围外的数据

        :return:
        """
        rf_records = pd.subtract(self._rf_records, self._dl_records)
        if dataframe_not_valid(rf_records):
            return

        dl_records = self._dl_records.rename(
            columns={START_DATE: DORCD_START, END_DATE: DORCD_END}
        )
        merge_records = pd.merge(
            dl_records, rf_records, how="left", on=[FREQ, SOURCE, FUQUAN, CODE]
        )
        error_records = merge_records[
            (merge_records[START_DATE] < merge_records[DORCD_START])
            | (merge_records[END_DATE] > merge_records[DORCD_END])
        ]

        if dataframe_is_valid(error_records):  # 打印错误信息
            start_date_errors = error_records[
                error_records[START_DATE] < error_records[DORCD_START]
            ]
            start_date_errors.apply(self._logger.start_date_error, axis=1)
            end_date_errors = error_records[
                error_records[END_DATE] > error_records[DORCD_END]
            ]
            end_date_errors.apply(self._logger.end_date_error, axis=1)
        return error_records

    def _datanum_check(self, save: bool) -> Optional[DataFrame]:
        """
        检查reform_records和dl_records的数据量

        :param save:        保存结果到磁盘
        :return:
        """
        dl_records = self._rf_records  # ！不是bug，基于已转换的部分进行对比
        table_names_by_date = TableNameTool.gets_by_records(dl_records)

        dl_records[DL_ROW_NUM] = dl_records.apply(self._get_dl_row_num, axis=1)
        rf_records = pd.concat(  # Assure safe
            [
                self._get_rf_row_num(row)
                for row in table_names_by_date.itertuples(index=False)
            ]
        )
        if save:
            self._save_report(df=rf_records, csv=True, feather=False)

        rf_records = (
            rf_records.groupby(by=[CODE, FREQ, SOURCE, FUQUAN])
            .aggregate({ROW_NUM: "sum"})
            .reset_index()
        )
        merge_records = pd.merge(
            dl_records, rf_records, how="left", on=[CODE, FREQ, SOURCE, FUQUAN]
        )
        error_records = merge_records[
            merge_records[ROW_NUM] != merge_records[DL_ROW_NUM]
        ]
        if dataframe_is_valid(error_records):  # 打印错误信息
            error_records.apply(self._logger.data_num_error, axis=1)
        return error_records

    def _get_dl_row_num(self, row: Series) -> int:
        """
        获取某支股票在按照Code组织的表中的行数

        :param row:            freq, source, fuquan, code, start, end
        :return:
        """
        para = Para.from_series(row)
        table_name_by_code = TableNameTool.get_by_code(para)
        row_num = self._operator.select_row_num(
            name=table_name_by_code, meta=META_DICT[para.comb], span=para.span
        )
        self._logger.get_data_info(table_name_by_code)
        return row_num

    def _get_rf_row_num(self, row: tuple) -> DataFrame:
        """
        获取某支股票在按照Date组织的表中的行数

        :param row:
        :return:
        """
        span = DateSpan(getattr(row, START_DATE), getattr(row, END_DATE))
        row_num = self._operator.select_row_num(
            name=getattr(row, TABLE_NAME), meta=None, span=span, groupby=[CODE]
        )
        if dataframe_is_valid(row_num):
            row_num[FREQ], row_num[FUQUAN], row_num[SOURCE], row_num[TABLE_NAME] = (
                getattr(row, FREQ),
                getattr(row, FUQUAN),
                getattr(row, SOURCE),
                getattr(row, TABLE_NAME),
            )
        self._logger.get_data_info(getattr(row, TABLE_NAME))
        return row_num


class ReformMaintainLogger:
    def start_date_error(self, row: tuple):
        key = self._get_key(row)
        Logger.error(
            f"In {key}, reform start at {getattr(row, START_DATE)} while record start at {getattr(row, DORCD_START)}."
        )

    def end_date_error(self, row: tuple):
        key = self._get_key(row)
        Logger.error(
            f"In {key}, reform end at {getattr(row, END_DATE)} while record end at {getattr(row, DORCD_END)}."
        )

    def data_num_error(self, row: tuple):
        key = self._get_key(row)
        Logger.error(
            f"In {key}, reform data count is {getattr(row, ROW_NUM)} while record data count is {getattr(row, DL_ROW_NUM)} "
        )

    @staticmethod
    def get_data_info(table_name: str):
        Logger.info(f"Successfully get data from {table_name}")

    @staticmethod
    def _get_key(row: tuple):
        return f"{getattr(row, FREQ)}info {getattr(row, CODE)} {getattr(row, FUQUAN)}"
