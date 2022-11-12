from datetime import date

from pendulum import Duration as PDuration


class Duration(PDuration):
    """
    封装Duration
    """
    def __add__(self, other):
        if isinstance(other, date):
            return other.__add__(self)
        raise NotImplemented

