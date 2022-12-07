from time import sleep as time_sleep
from traceback import format_exc as tr_format_exc

from tqdm import tqdm as tq_tqdm

from buffett.adapter.wellknown.bisect import bisect

sleep = time_sleep
format_exc = tr_format_exc
tqdm = tq_tqdm
