from buffett.common.constants.meta.recorder import DL_RF_META
from buffett.common.constants.table import RF_RCD
from buffett.download.mysql import Operator
from buffett.download.recorder.recorder import Recorder


class ReformRecorder(Recorder):
    def __init__(self, operator: Operator):
        super(ReformRecorder, self).__init__(
            operator=operator, table_name=RF_RCD, meta=DL_RF_META
        )
