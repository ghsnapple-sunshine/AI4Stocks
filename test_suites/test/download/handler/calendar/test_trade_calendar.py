from buffett.common.constants.col import DATE
from buffett.common.tools import dataframe_not_valid
from buffett.download.handler.calendar import CalendarHandler
from test import DbSweeper, Tester


class TradeCalendarHandlerTest(Tester):
    @classmethod
    def _setup_oncemore(cls):
        cls._hdl = CalendarHandler(operator=cls._operator)

    def _setup_always(self) -> None:
        DbSweeper.erase()

    def test_download(self):
        df1 = self._hdl.obtain_data()
        df2 = self._hdl.select_data()
        df2[DATE] = df2[DATE].apply(lambda x: str(x))
        assert self.compare_dataframe(df1, df2)

    def test_no_download(self):
        db = self._hdl.select_data()
        assert dataframe_not_valid(db)
