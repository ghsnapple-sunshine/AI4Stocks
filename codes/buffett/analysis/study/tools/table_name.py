from buffett.analysis import Para
from buffett.analysis.types import AnalystType
from buffett.common.constants.col import FREQ, SOURCE, FUQUAN
from buffett.common.constants.col.analysis import ANALYSIS
from buffett.download.handler.tools import TableNameTool as DlTableNameTool


class TableNameTool(DlTableNameTool):
    _GROUPBY = [FREQ, SOURCE, FUQUAN, ANALYSIS]

    @classmethod
    def get_by_code(cls, para: Para) -> str:
        """
        获取按Code索引的Mysql表名

        :param para     code, source, freq, fuquan, analysis
        :return:
        """
        if para.comb.analysis == AnalystType.CONV:
            return "{0}_{1}info_{2}_{3}".format(
                para.comb.source.sql_format(),
                para.comb.freq,
                para.target.code,
                para.comb.fuquan.ak_format(),
            )
        else:
            return "{0}_{1}_{2}info_{3}_{4}".format(
                para.comb.source.sql_format(),
                para.comb.analysis.sql_format(),
                para.comb.freq,
                para.target.code,
                para.comb.fuquan.ak_format(),
            )

    @classmethod
    def get_by_date(cls, para: Para) -> str:
        """
        获取按时间索引的Mysql表名

        :return:
        """
        return "{0}_{1}_{2}info_{3}_{4}".format(
            para.comb.source.sql_format(),
            para.comb.analysis.sql_format(),
            para.comb.freq,
            para.span.start.format("YYYY_MM"),
            para.comb.fuquan.ak_format(),
        )
