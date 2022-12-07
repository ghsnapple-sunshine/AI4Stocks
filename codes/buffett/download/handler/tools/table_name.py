from buffett.adapter.pandas import DataFrame, pd
from buffett.common.constants.col import FREQ, SOURCE, FUQUAN, START_DATE, END_DATE
from buffett.common.constants.col.my import MONTH_START, TABLE_NAME
from buffett.common.pendulum import convert_datetime, DateTime, DateSpan
from buffett.download import Para


class TableNameTool:
    @classmethod
    def get_by_code(cls, para: Para) -> str:
        """
        获取按Code索引的Mysql表名

        :param para:            freq, source, fuquan, code
        :return:
        """
        table_name = "{0}_{1}info_{2}_{3}".format(
            para.comb.source.sql_format(),
            para.comb.freq,
            para.target.code,
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
        table_name = "{0}_{1}info_{2}_{3}".format(
            para.comb.source.sql_format(),
            para.comb.freq,
            para.span.start.format("YYYY_MM"),
            para.comb.fuquan.ak_format(),
        )
        return table_name

    @classmethod
    def gets_by_records(cls, records: DataFrame) -> DataFrame:
        """
        获取按时间索引的Mysql表名

        :param records:             已下载记录/待下载记录
        :return:
        """
        records = (
            records.groupby(by=[FREQ, SOURCE, FUQUAN])
            .aggregate({START_DATE: "min", END_DATE: "max"})
            .reset_index()
        )
        spans = records[[START_DATE, END_DATE]].drop_duplicates()
        series = pd.concat(  # Assure_safe
            [cls._create_single_series(row) for row in spans.itertuples(index=False)]
        )
        tables = pd.merge(records, series, how="left", on=[START_DATE, END_DATE])
        tables[TABLE_NAME] = tables.apply(
            lambda row: cls.get_by_date(
                para=Para.from_series(row).with_start(row[MONTH_START])
            ),
            axis=1,
        )
        return tables

    @classmethod
    def _create_single_series(cls, span: tuple) -> DataFrame:
        """
        获取指定时间范围内的时间分段清单

        :param span:                时间范围
        :return:                    时间分段清单
        """
        span = DateSpan(
            start=convert_datetime(getattr(span, START_DATE)),
            end=convert_datetime(getattr(span, END_DATE)),
        )
        dates = cls._create_by_span(span=span)
        return DataFrame(
            {START_DATE: span.start, END_DATE: span.end, MONTH_START: dates}
        )

    @classmethod
    def gets_by_span(cls, para: Para):
        """
        获取按时间索引的Mysql表名

        :param para:
        :return:
        """
        dates = cls._create_by_span(para.span)
        para = para.clone()
        return [cls.get_by_date(para=para.with_start(x)) for x in dates]

    @classmethod
    def _create_by_span(cls, span: DateSpan):
        """
        获取指定时间范围内的月

        :param span:                时间范围
        :return:                    时间分段清单
        """
        start, end = span.start, span.end
        month_start = DateTime(start.year, start.month, 1)
        dates = []
        while month_start < end:
            dates.append(month_start)
            month_start = month_start.add(months=1)
        return dates
