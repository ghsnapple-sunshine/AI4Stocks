from typing import Optional

from buffett.adapter import logging
from buffett.adapter.numpy import np
from buffett.adapter.pandas import Series, DataFrame, pd
from buffett.adapter.wellknown import sleep
from buffett.common import create_meta
from buffett.common.constants.col.task import (
    TASK_ID,
    PARENT_ID,
    SUCCESS,
    ERR_MSG,
    MODULE,
    CLASS,
    START_TIME,
    CREATE_TIME,
    END_TIME,
)
from buffett.common.constants.table import TASK_RCD
from buffett.common.interface import Singleton
from buffett.common.magic import load_class
from buffett.common.pendulum import DateTime, convert_datetime
from buffett.common.tools import dataframe_not_valid, list_is_valid, dataframe_is_valid
from buffett.download.mysql import Operator
from buffett.download.mysql.types import ColType, AddReqType
from buffett.task.base.task import Task

_META = create_meta(
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


class TaskScheduler(Singleton):
    def __new__(cls, operator: Operator, tasks: Optional[list[Task]] = None):
        return super(TaskScheduler, cls).__new__(cls, operator, tasks)

    def __init__(self, operator: Operator, tasks: Optional[list[Task]] = None):
        super(TaskScheduler, self).__init__()
        self._operator = operator
        operator.create_table(name=TASK_RCD, meta=_META)
        if list_is_valid(tasks):
            self._add_tasks(tasks)

    def _create_task(self, series: Series) -> Optional[Task]:
        """
        根据数据库记录创建Task

        :param series:
        :return:
        """
        task_cls = load_class(series[MODULE], series[CLASS])
        if task_cls is None:
            return
        task = task_cls(
            operator=self._operator, start_time=convert_datetime(series[START_TIME])
        )
        task.task_id = series[TASK_ID]
        return task

    def run(self):
        """
        任务调度器运行

        :return:
        """
        task = self._load_task()
        while task is not None:
            now = DateTime.now()
            if now < task.start_time:
                delay = task.start_time - now
                logging.info(
                    "Next task will be executed in {0}(Start at: {1})".format(
                        delay.in_words(), task.start_time.format_YMDHms()
                    )
                )
                sleep(delay.total_seconds())
            self._run_n_update_task(task)
            task = self._load_task()
        logging.info("All tasks are successfully executed.")

    def _save_to_database(self, df: DataFrame) -> None:
        """
        将df写入数据库

        :param df:
        :return:
        """
        if dataframe_not_valid(df):
            return
        self._operator.insert_data(name=TASK_RCD, df=df)

    def _add_tasks(self, tasks: list[Task]) -> None:
        """
        任务调度器启动时新增多个任务

        :param tasks:
        :return:
        """
        task_infos = DataFrame([x.info for x in tasks])

        df = self._operator.select_data(name=TASK_RCD)
        if dataframe_is_valid(df):
            # 20221128变更：新增task时判断下是否有同类型Task未执行完
            df = df[pd.isna(df[SUCCESS])][[CLASS, MODULE]]
            task_filter = task_infos[[CLASS, MODULE]]
            task_filter = pd.subtract(task_filter, df)
            task_infos = pd.merge(
                task_filter, task_infos, how="left", on=[CLASS, MODULE]
            )

        start = self._operator.select_row_num(name=TASK_RCD) + 1
        task_infos[TASK_ID] = np.arange(
            start=start, stop=start + len(task_infos), dtype=int
        )
        now = DateTime.now()
        task_infos[CREATE_TIME] = now
        self._save_to_database(task_infos)

    def _add_task(self, task: Task, parent_id: int) -> None:
        """
        任务调度器运行时新增一个任务

        :param task:
        :param parent_id:
        :return:
        """
        task_info = task.info
        # 20221128变更：新增task时判断下是否有同类型Task未执行完
        curr_tasks = self._operator.select_data(name=TASK_RCD)
        if dataframe_is_valid(curr_tasks):
            find = (
                pd.isna(curr_tasks[SUCCESS])  # Task未完成
                & (curr_tasks[CLASS] == task_info[CLASS])  # Task Class和Module与现有Task相同
                & (curr_tasks[MODULE] == task_info[MODULE])
            )
            if any(find):
                return  # 跳过插入task

        task_info[TASK_ID] = curr_tasks[TASK_ID].max() + 1
        now = DateTime.now()
        task_info[CREATE_TIME] = now
        task_info[PARENT_ID] = parent_id
        self._save_to_database(DataFrame([task_info]))

    def _run_n_update_task(self, task: Task):
        """
        任务调度器运行并且更新数据库记录

        :param task:
        :return:
        """
        success, res, new_task, err_msg = task.run()
        now = DateTime.now()
        task_info = DataFrame(
            [{TASK_ID: task.task_id, SUCCESS: success, END_TIME: now, ERR_MSG: err_msg}]
        )
        self._operator.try_insert_data(
            name=TASK_RCD, df=task_info, meta=_META, update=True
        )

        if isinstance(new_task, Task):
            self._add_task(new_task, parent_id=task.task_id)

    def _load_task(self) -> Optional[Task]:
        """
        从数据库加载待运行的下一个任务

        :return:
        """
        df = self._operator.select_data(name=TASK_RCD)
        if dataframe_not_valid(df):
            return
        df = df[pd.isna(df[SUCCESS])].sort_values(by=[START_TIME])
        if dataframe_not_valid(df):
            return

        for i in range(0, len(df)):
            row = df.iloc[i]
            task = self._create_task(df.iloc[i])
            if task is not None:
                return task
            else:
                row[END_TIME] = DateTime.now()
                row[SUCCESS] = 0
                row[ERR_MSG] = "Import task failed."
                self._operator.try_insert_data(
                    name=TASK_RCD, df=DataFrame([row]), meta=_META, update=True
                )
        return
