from test import Tester

from buffett.common.logger import Logger
from buffett.common.logger import LoggerBuilder
from buffett.common.logger import LogType


def my_init(self, v):
    self.v = v


DLogger = type('DLogger',
               (Logger,),
               {'__init__': my_init,
                'debug_add_zero': lambda self, x: x,
                'info_add_one': lambda self, x: x + 1,
                'warning_add_two': lambda self, x: x + 2,
                'error_add_three': lambda self, x: x + 3,
                'other': lambda self, x: x + 100})


class TestLogger(Tester):
    def test_no_build(self):
        logger = DLogger(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) == 0
        assert logger.info_add_one(0) == 1
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_debug(self):
        Logger.Level = LogType.DEBUG
        logger = LoggerBuilder.build(DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) == 0
        assert logger.info_add_one(0) == 1
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_info(self):
        Logger.Level = LogType.INFO
        logger = LoggerBuilder.build(DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) is None
        assert logger.info_add_one(0) == 1
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_warning(self):
        Logger.Level = LogType.WARNING
        logger = LoggerBuilder.build(DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) is None
        assert logger.info_add_one(0) is None
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_error(self):
        Logger.Level = LogType.ERROR
        logger = LoggerBuilder.build(DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) is None
        assert logger.info_add_one(0) is None
        assert logger.warning_add_two(0) is None
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100
