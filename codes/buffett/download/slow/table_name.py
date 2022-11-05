from buffett.download import Para


class TableName:
    @classmethod
    def _get_table_name_by_code(cls, para: Para) -> str:
        """
        获取按Code索引的Mysql表名

        :param para:            freq, source, fuquan, code
        :return:
        """
        table_name = '{0}_stock_{1}info_{2}_{3}'.format(para.comb.source.sql_format(),
                                                        para.comb.freq,
                                                        para.stock.code,
                                                        para.comb.fuquan.ak_format())
        return table_name

    @classmethod
    def _get_table_name_by_date(cls, para: Para) -> str:
        """
        获取按时间索引的Mysql表名

        :param para:            freq, source, fuquan, start
        :return:
        """
        table_name = '{0}_stock_{1}info_{2}_{3}'.format(para.comb.source.sql_format(),
                                                        para.comb.freq,
                                                        para.span.start.format('YYYY_MM'),
                                                        para.comb.fuquan.ak_format())
        return table_name
