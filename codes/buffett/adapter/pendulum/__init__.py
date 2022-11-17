from datetime import (
    datetime as dt_datetime,
    date as dt_date,
    timedelta as dt_timedelta,
    tzinfo as dt_tzinfo,
)

from pendulum import (
    DateTime as pen_DateTime,
    Date as pen_Date,
    Duration as pen_Duration,
)

DateTime = pen_DateTime
Date = pen_Date
Duration = pen_Duration
datetime = dt_datetime
date = dt_date
timedelta = dt_timedelta
tzinfo = dt_tzinfo
