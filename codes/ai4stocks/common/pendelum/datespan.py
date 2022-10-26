from ai4stocks.common.pendelum.date import Date


class DateSpan:
    # DateSpan所描述的范围为[start, end]
    def __init__(self, start: Date, end: Date):
        if start > end:
            err_msg = 'Invalid datespan [{0}, {1})'.format(
                start.format('YYYY-MM-DD'), end.format('YYYY-MM-DD'))
            raise ValueError(err_msg)
        self._start = start
        self._end = end

    @property
    def start(self) -> Date:
        return self._start

    @property
    def end(self) -> Date:
        return self._end
