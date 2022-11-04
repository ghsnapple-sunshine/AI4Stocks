import logging
import traceback
from abc import abstractmethod

from buffett.common.pendelum import Duration, DateTime
from buffett.task.task import Task


class DownloadTask(Task):
    @abstractmethod
    def cycle(self) -> Duration:
        pass

    @abstractmethod
    def error_cycle(self) -> Duration:
        pass

    def run(self) -> tuple:
        try:
            success, res, new_task = super().run()
            if success:
                new_task = DownloadTask(attr=self._attr,
                                        args=self._args,
                                        kwargs=self._kwargs,
                                        start_time=DateTime.now() + self.cycle())
                return True, res, new_task
            else:
                return True, res, None
        except Exception as e:
            logging.error("---------------Error occured when running {0}.---------------".format(self._get_name()))
            logging.error(traceback.format_exc())
            new_task = DownloadTask(attr=self._attr,
                                    args=self._args,
                                    kwargs=self._kwargs,
                                    start_time=DateTime.now() + self.error_cycle())
            return False, None, new_task
