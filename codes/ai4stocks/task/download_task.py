import logging
import traceback
from enum import Enum

from pendulum import Duration, DateTime

from ai4stocks.task.base_task import BaseTask
from ai4stocks.common.tools import GetNowShift


class DownloadTask(BaseTask):
    def cycle(self) -> Duration:
        return Duration(seconds=1)

    def errorCycle(self) -> Duration:
        return Duration(seconds=1)

    def run(self) -> tuple:

        try:
            success, res, new_task = super().run()
            if success:
                new_task = DownloadTask(
                    obj=self.obj,
                    method_name=self.method_name,
                    args=self.args,
                    kwargs=self.kwargs,
                    plan_time=GetNowShift(self.cycle())
                )
                return TaskStatus.Success, res, new_task
            else:
                return TaskStatus.Fail, res, None
        except Exception as e:
            print("---------------Error occured when running method {0} in Type {1} object.---------------".format(
                self.method_name, type(self.obj)
            ))
            logging.error(traceback.format_exc())
            new_task = DownloadTask(
                obj=self.obj,
                method_name=self.method_name,
                args=self.args,
                kwargs=self.kwargs,
                plan_time=GetNowShift(self.cycle(), minus=True)
            )
            return TaskStatus.PartialSuccess, None, new_task


class TaskStatus(Enum):
    Success = 1
    PartialSuccess = 2
    Fail = 3
