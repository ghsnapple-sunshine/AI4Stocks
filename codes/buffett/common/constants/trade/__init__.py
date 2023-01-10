from buffett.adapter.numpy import np
from buffett.adapter.pandas import Timedelta

TRADE_TIMES = [
    Timedelta(minutes=x)
    for x in np.concatenate([np.arange(5, 125, 5), np.arange(215, 335, 5)]) + 570
]
