from buffett.adapter.pandas import DataFrame
from buffett.backtrader.frame.clock import Clock
from buffett.backtrader.interface.time_sequence import ITimeSequence as Sequence
from buffett.common.pendelum import DateSpan as Span, convert_date
from buffett.common.constants.col import DATE
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.mysql import Operator


class ClockManager(Sequence):
    def __init__(self):
        super().__init__()
        self._calendar: DataFrame = DataFrame()

    @property
    def calendar(self) -> DataFrame:
        return self._calendar

    @property
    def max_tick(self):
        return len(self._calendar)

    def run(self):
        tick = self.curr_tick + 1
        time = 'Final Stage'
        is_end = True
        if tick < self.max_tick:
            time = convert_date(self._calendar.iloc[tick, 0]).format('YYYY-MM-DD')
            is_end = False
        self._clock.turn_next(time, is_end=is_end)


class ClockManagerBuilder:
    class ClockManagerUnderBuilt(ClockManager):
        def set_calendar(self, calendar: DataFrame):
            self._calendar = calendar

        def set_clock(self, clock: Clock):
            self._clock = clock

    def __init__(self):
        self.item = ClockManagerBuilder.ClockManagerUnderBuilt()

    def with_calendar(self, operator: Operator, datespan: Span):
        tbl = CalendarHandler(operator=operator).select_data()
        calendar = tbl[tbl[DATE].apply(lambda x: datespan.is_inside(x))]
        # calendar = tbl[tbl[DATE].apply(lambda x: datespan.start <= x <= datespan.end)] dataSpan definition changed
        # calendar = tbl[datespan.start <= tbl[DATE] <= datespan.end] Error
        self.item.set_calendar(calendar=calendar)
        return self

    def with_clock(self, clock: Clock):
        self.item.set_clock(clock=clock)
        return self

    def build(self):
        self.item.__class__ = ClockManager
        return self.item
