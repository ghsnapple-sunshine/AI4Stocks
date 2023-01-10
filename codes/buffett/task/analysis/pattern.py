from buffett.analysis.study import PatternAnalyst
from buffett.common.constants.col.my import ANA_R, ANA_W, STK_R
from buffett.common.pendulum import DateSpan, DateTime, Date
from buffett.common.wrapper import Wrapper
from buffett.download.mysql import Operator
from buffett.task.base import Task


class TargetPatternRecognizeTask(Task):
    def __init__(self, ops: dict[str, Operator], start_time: DateTime):
        super().__init__(
            wrapper=Wrapper(
                PatternAnalyst(
                    ana_rop=ops.get(ANA_R),
                    ana_wop=ops.get(ANA_W),
                    stk_rop=ops.get(STK_R),
                ).calculate
            ),
            args=(DateSpan(start=Date(2000, 1, 1), end=Date(2022, 11, 1)),),
            start_time=start_time,
        )

    def get_subsequent_task(self, success: bool):
        return None
