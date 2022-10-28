from pendulum import Duration as PDuration


class Duration(PDuration):
    """
    封装Duration
    """
    def __add__(self, other):
        result = super(Duration, self).__add__(other)
        result.__class__ = Duration
        return result

