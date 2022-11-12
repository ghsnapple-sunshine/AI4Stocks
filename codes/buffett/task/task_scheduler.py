import logging
import time
from typing import Optional

import pandas as pd
from numpy import arange
from pandas import DataFrame, Series

from buffett.common import create_meta
from buffett.common.interface import Singleton
from buffett.common.pendelum import DateTime, convert_datetime
from buffett.common.tools import dataframe_not_valid, list_is_valid
from buffett.constants.col.task import TASK_ID, PARENT_ID, SUCCESS, ERR_MSG, MODULE, CLASS, START_TIME, \
    CREATE_TIME, END_TIME
from buffett.constants.magic import load_class
from buffett.constants.table import TASK_RCD
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.task import Task

_META = create_meta(meta_list=[
    [TASK_ID, ColType.INT32, AddReqType.KEY],
    [PARENT_ID, ColType.INT32, AddReqType.NONE],
    [CLASS, ColType.SHORT_DESC, AddReqType.NONE],
    [MODULE, ColType.SHORT_DESC, AddReqType.NONE],
    [CREATE_TIME, ColType.DATETIME, AddReqType.NONE],
    [START_TIME, ColType.DATETIME, AddReqType.NONE],
    [END_TIME, ColType.DATETIME, AddReqType.NONE],
    [SUCCESS, ColType.ENUM_BOOL, AddReqType.NONE],
    [ERR_MSG, ColType.LONG_DESC, AddReqType.NONE]])


class TaskScheduler(Singleton):
    def __new__(cls,
                operator: Operator,
                tasks: Optional[list[Task]] = None):
        return super(TaskScheduler, cls).__new__(cls, operator, tasks)

    def __init__(self,
                 operator: Operator,
                 tasks: Optional[list[Task]] = None):
        super(TaskScheduler, self).__init__()
        self._operator = operator
        operator.create_table(name=TASK_RCD, meta=_META)
        if list_is_valid(tasks):
            self._add_tasks(tasks)

    def _create_task(self,
                     series: Series) -> Task:
        task_cls = load_class(series[MODULE], series[CLASS])
        task = task_cls(operator=self._operator,
                        start_time=convert_datetime(series[START_TIME]))
        task.task_id = series[TASK_ID]
        return task

    def run(self):
        task = self._load_task()
        while task is not None:
            now = DateTime.now()
            if now < task.start_time:
                delay = task.start_time - now
                logging.info('Next task will be executed in {0}(Start at: {1})'.format(
                    delay.in_words(),
                    task.start_time.format_YMDHms()))
                time.sleep(delay.total_seconds())
            self._run_n_update_task(task)
            task = self._load_task()
        logging.info('All tasks are successfully executed.')

    def _save_to_database(self,
                          df: DataFrame) -> None:
        if dataframe_not_valid(df):
            return

        self._operator.insert_data(name=TASK_RCD, df=df)
        self._operator.disconnect()

    def _add_tasks(self,
                   tasks: list[Task]) -> None:
        task_infos = DataFrame([x.info for x in tasks])
        start = self._operator.select_row_num(name=TASK_RCD) + 1
        task_infos[TASK_ID] = arange(start=start, stop=start + len(tasks), dtype=int)
        now = DateTime.now()
        task_infos[CREATE_TIME] = now
        self._save_to_database(task_infos)

    def _add_task(self,
                  task: Task,
                  parent_id: int) -> None:
        task_info = task.info
        task_info[TASK_ID] = self._operator.select_row_num(name=TASK_RCD) + 1
        now = DateTime.now()
        task_info[CREATE_TIME] = now
        task_info[PARENT_ID] = parent_id
        self._save_to_database(DataFrame([task_info]))

    def _run_n_update_task(self,
                           task: Task):
        success, res, new_task, err_msg = task.run()
        now = DateTime.now()
        task_info = DataFrame([{TASK_ID: task.task_id,
                                SUCCESS: success,
                                END_TIME: now,
                                ERR_MSG: err_msg}])
        self._operator.try_insert_data(name=TASK_RCD, df=task_info, meta=_META, update=True)

        if isinstance(new_task, Task):
            self._add_task(new_task, parent_id=task.task_id)

    def _load_task(self) -> Optional[Task]:
        df = self._operator.select_data(name=TASK_RCD)
        if dataframe_not_valid(df):
            return

        df = df[pd.isna(df[SUCCESS])]
        if dataframe_not_valid(df):
            return

        task_info = df.sort_values(by=START_TIME).head(1)
        task = self._create_task(task_info.iloc[0])
        return task
