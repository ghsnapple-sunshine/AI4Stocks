from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import FREQ, FUQUAN, SOURCE, START_DATE, END_DATE
from buffett.common.constants.col.target import CODE
from buffett.download import Para
from buffett.download.recorder.simple_recorder import SimpleRecorder


class Recorder(SimpleRecorder):
    def save(self, para: Para) -> None:
        """
        基于para保存

        :param para:
        :return:
        """
        ls = [
            [
                para.target.code,
                para.comb.freq,
                para.comb.fuquan,
                para.comb.source,
                para.span.start,
                para.span.end,
            ]
        ]
        cols = [CODE, FREQ, FUQUAN, SOURCE, START_DATE, END_DATE]
        data = DataFrame(data=ls, columns=cols)
        self.save_to_database(df=data)
