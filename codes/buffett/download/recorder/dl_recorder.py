from buffett.common.constants.meta.recorder import DL_RF_META
from buffett.common.constants.table import DL_RCD
from buffett.download.mysql import Operator
from buffett.download.recorder.recorder import Recorder


class DownloadRecorder(Recorder):
    def __init__(self, operator: Operator):
        super(DownloadRecorder, self).__init__(
            operator=operator, table_name=DL_RCD, meta=DL_RF_META
        )
