from typing import Optional

from buffett.adapter.pendulum import DateTime
from buffett.analysis.maintain import StockMinuteBfqMaintain
from buffett.common.wrapper import Wrapper
from buffett.download.mysql import Operator
from buffett.task.base import Task


class StockMinuteBfqMaintainTask(Task):
    def __init__(
        self,
        ana_rop: Operator,
        mtain_wop: Operator,
        stk_rop: Operator,
        start_time: Optional[DateTime] = None,
    ):
        super().__init__(
            wrapper=Wrapper(
                StockMinuteBfqMaintain(
                    ana_rop=ana_rop, mtain_wop=mtain_wop, stk_rop=stk_rop
                ).run
            ),
            start_time=start_time,
        )

    def get_subsequent_task(self, success: bool):
        return None
