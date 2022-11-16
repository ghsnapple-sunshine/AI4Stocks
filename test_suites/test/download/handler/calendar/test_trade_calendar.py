from buffett.common.tools import dataframe_not_valid
from buffett.common.constants.col import DATE
from buffett.download.handler.calendar import CalendarHandler
from test import DbSweeper, Tester


class TradeCalendarHandlerTest(Tester):
    def test_download(self):
        DbSweeper.cleanup()
        hdl = CalendarHandler(operator=self.operator)
        df1 = hdl.obtain_data()
        df2 = hdl.select_data()
        df2[DATE] = df2[DATE].apply(lambda x: str(x))
        assert self.compare_dataframe(df1, df2)

    def test_no_download(self):
        DbSweeper.cleanup()
        hdl = CalendarHandler(operator=self.operator)
        db = hdl.select_data()
        assert dataframe_not_valid(db)
