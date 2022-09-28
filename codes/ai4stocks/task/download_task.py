from enum import Enum

from pendulum import Duration, DateTime

from ai4stocks.task.base_task import BaseTask
from ai4stocks.tools.tools import GetNowShift


class DownloadTask(BaseTask):
    def PlanTime(self) -> DateTime:
        return self.plan_time

    def Cycle(self) -> Duration:
        return Duration(seconds=1)

    def ErrorCycle(self) -> Duration:
        return Duration(seconds=1)

    def Run(self) -> tuple:

        try:
            success, res, new_task = super().Run()
            if success:
                new_task = DownloadTask(
                    obj=self.obj,
                    method_name=self.method_name,
                    args=self.args,
                    kwargs=self.kwargs,
                    plan_time=GetNowShift(self.Cycle())
                )
                return TaskStatus.Success, res, new_task
            else:
                return TaskStatus.Fail, res, None
        except Exception as e:
            print("---------------Error occured when running method {0} in Type {1} object.---------------".format(
                self.method_name, type(self.obj)
            ))
            new_task = DownloadTask(
                obj=self.obj,
                method_name=self.method_name,
                args=self.args,
                kwargs=self.kwargs,
                plan_time=GetNowShift(self.Cycle(), minus=True)
            )
            return TaskStatus.PartialSuccess, None, new_task


class TaskStatus(Enum):
    Success = 1
    PartialSuccess = 2
    Fail = 3
