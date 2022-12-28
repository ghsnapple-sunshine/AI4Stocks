from buffett.adapter.pendulum import Date
from buffett.adapter.wellknown import bisect
from buffett.common.constants.col import DATE
from buffett.common.pendulum import convert_date
from buffett.download.handler.calendar import CalendarHandler
from buffett.download.mysql import Operator


class CalendarManager:
    def __init__(self, datasource_op: Operator):
        self._calendar = CalendarHandler(operator=datasource_op).select_data().reset_index()
        self._calendar_list = self._calendar[DATE].to_list()
        self._calendar_dict = dict(
            (k, convert_date(v)) for k, v in self._calendar[DATE].items()
        )

    def query(self, date: Date, offset: int):
        """
        获取之前或者之后的第几个交易日

        :param date:
        :param offset:
        :return:
        """
        if date in self._calendar_dict:
            loc = self._calendar_dict[date] + offset
        elif offset >= 0:
            loc = bisect.bisect_left(self._calendar_list, date) + (offset + 1)
        else:  # offset < 0
            loc = bisect.bisect_right(self._calendar_list, date) + (offset - 1)
        # if loc < 0 or loc > len(self._calendar_list):
        #    raise OutOfRangeError(loc)
        loc = 0 if loc < 0 else loc
        loc = len(self._calendar_list) - 1 if loc >= len(self._calendar_list) else loc
        return self._calendar_list[loc]
