from buffett.common.logger import LogType, Logger, LoggerBuilder
from test import Tester


class TestLogger(Tester):
    @classmethod
    def _setup_oncemore(cls):
        def my_init(s, v):
            s.v = v

        cls.DLogger = type(
            "DLogger",
            (Logger,),
            {
                "__init__": my_init,
                "debug_add_zero": lambda s, x: x,
                "info_add_one": lambda s, x: x + 1,
                "warning_add_two": lambda s, x: x + 2,
                "error_add_three": lambda s, x: x + 3,
                "other": lambda s, x: x + 100,
            },
        )

    def _setup_always(self) -> None:
        pass

    def test_no_build(self):
        logger = self.DLogger(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) == 0
        assert logger.info_add_one(0) == 1
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_debug(self):
        Logger.Level = LogType.DEBUG
        logger = LoggerBuilder.build(self.DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) == 0
        assert logger.info_add_one(0) == 1
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_info(self):
        Logger.Level = LogType.INFO
        logger = LoggerBuilder.build(self.DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) is None
        assert logger.info_add_one(0) == 1
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_warning(self):
        Logger.Level = LogType.WARNING
        logger = LoggerBuilder.build(self.DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) is None
        assert logger.info_add_one(0) is None
        assert logger.warning_add_two(0) == 2
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100

    def test_build_level_error(self):
        Logger.Level = LogType.ERROR
        logger = LoggerBuilder.build(self.DLogger)(5)
        assert logger.v == 5
        assert logger.debug_add_zero(0) is None
        assert logger.info_add_one(0) is None
        assert logger.warning_add_two(0) is None
        assert logger.error_add_three(0) == 3
        assert logger.other(0) == 100
