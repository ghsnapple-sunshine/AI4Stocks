import unittest

from ai4stocks.task.download_task import DownloadTask, TaskStatus


class Inner:
    def __init__(self):
        self.count = 1

    def run(self):
        if self.count == 1:
            self.count += 1
            raise ValueError('self.count不为2')
        return self.count


class TestDownloadTask(unittest.TestCase):
    def test_run(self):
        task = DownloadTask(
            obj=Inner(),
            method_name='run')

        status, res, task = task.Run()
        assert status == TaskStatus.PartialSuccess
        assert res is None

        status, res, task = task.Run()
        assert status == TaskStatus.Success
        assert res == 2

        task = DownloadTask(
            obj=Inner(),
            method_name='jump')
        status, res, task = task.Run()
        assert status == TaskStatus.Fail
        assert res is None
