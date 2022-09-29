import logging
import traceback
import unittest

from ai4stocks.task.download_task import DownloadTask, TaskStatus


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


class TestDownloadTask(unittest.TestCase):
    def test_run(self):
        task = DownloadTask(
            obj=InnerA(),
            method_name='run')

        status, res, task = task.Run()
        assert status == TaskStatus.PartialSuccess
        assert res is None

        status, res, task = task.Run()
        assert status == TaskStatus.Success
        assert res == 2

        task = DownloadTask(
            obj=InnerA(),
            method_name='jump')
        status, res, task = task.Run()
        assert status == TaskStatus.Fail
        assert res is None

    def test_catchError(self):
        try:
            task = DownloadTask(
                obj=InnerB(),
                method_name='run')
            task.Run()
            assert True
        except ValueError as e:
            logging.error('\n' + traceback.format_exc())
            assert False
