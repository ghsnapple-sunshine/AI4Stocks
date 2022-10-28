import logging
import traceback
import unittest

from ai4stocks.common.pendelum import Duration
from ai4stocks.task.download_task import DownloadTask


class InnerA:
    def __init__(self):
        self.count = 1

    def run(self):
        if self.count == 1:
            self.count += 1
            raise ValueError('self.count不为2')
        return self.count


class InnerB:
    def run(self):
        raise ValueError('error')


def _cycle(self):
    return Duration(days=1)


def _error_cycle(self):
    return Duration(days=1)


class TestDownloadTask(unittest.TestCase):
    def setUp(self) -> None:
        DownloadTask.cycle = _cycle
        DownloadTask.error_cycle = _error_cycle

    def test_run(self):
        task = DownloadTask(InnerA().run)

        success, res, task = task.run()
        assert not success
        assert res is None

        success, res, task = task.run()
        assert success
        assert res == 2

    def test_catchError(self):
        try:
            task = DownloadTask(InnerB().run)
            task.run()
            assert True
        except ValueError as e:
            logging.error('\n' + traceback.format_exc())
            assert False
