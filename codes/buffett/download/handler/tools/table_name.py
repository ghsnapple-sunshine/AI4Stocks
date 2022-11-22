from buffett.adapter.pandas import DataFrame, pd, Series
from buffett.common.constants.col import FREQ, SOURCE, FUQUAN, START_DATE, END_DATE
from buffett.common.constants.col.my import MONTH_START, TABLE_NAME
from buffett.common.pendulum import convert_datetime, DateTime
from buffett.download import Para


class TableNameTool:
    @classmethod
    def get_by_code(cls, para: Para) -> str:
        """
        获取按Code索引的Mysql表名

        :param para:            freq, source, fuquan, code
        :return:
        """
        table_name = "{0}_stock_{1}info_{2}_{3}".format(
            para.comb.source.sql_format(),
            para.comb.freq,
            para.stock.code,
            para.comb.fuquan.ak_format(),
        )
        return table_name

    @classmethod
    def get_by_date(cls, para: Para) -> str:
        """
        获取按时间索引的Mysql表名

        :param para:            freq, source, fuquan, start
        :return:
        """
        table_name = "{0}_stock_{1}info_{2}_{3}".format(
            para.comb.source.sql_format(),
            para.comb.freq,
            para.span.start.format("YYYY_MM"),
            para.comb.fuquan.ak_format(),
        )
        return table_name

    @classmethod
    def get_multi_by_date(cls, records: DataFrame) -> DataFrame:
        """
        生成已下载记录/待下载记录的Mysql表名

        :param records:             已下载记录/待下载记录
        :return:
        """
        records = (
            records.groupby(by=[FREQ, SOURCE, FUQUAN])
            .aggregate({START_DATE: "min", END_DATE: "max"})
            .reset_index()
        )
        spans = records[[START_DATE, END_DATE]].drop_duplicates()
        series = pd.concat(
            [
                TableNameTool._create_single_series(row)
                for row in spans.itertuples(index=False)
            ]
        )
        tables = pd.merge(records, series, how="left", on=[START_DATE, END_DATE])
        tables[TABLE_NAME] = tables.apply(
            lambda row: TableNameTool.get_by_date(
                para=Para.from_series(row).with_start(row[MONTH_START])
            ),
            axis=1,
        )
        return tables

    @classmethod
    def _create_single_series(cls, spans: tuple) -> DataFrame:
        """
        获取指定时间范围内的时间分段清单

        :param spans:               时间范围
        :return:                    时间分段清单
        """
        start = convert_datetime(getattr(spans, START_DATE))
        end = convert_datetime(getattr(spans, END_DATE))
        month_start = DateTime(start.year, start.month, 1)

        dates = []
        while month_start < end:
            dates.append([start, end, month_start])
            month_start = month_start.add(months=1)
        return DataFrame(dates, columns=[START_DATE, END_DATE, MONTH_START])
