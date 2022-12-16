from typing import Optional

from buffett.adapter.numpy import np, ndarray, NAN
from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.study.supporter import DataManager
from buffett.analysis.study.tool import get_stock_list
from buffett.common.constants.col import (
    DATE,
    CLOSE,
    START_DATE,
    END_DATE,
    A,
    B,
    OPEN,
    HIGH,
    LOW,
    DATETIME,
)
from buffett.common.constants.col.target import CODE
from buffett.common.constants.meta.handler.definition import FQ_FAC_META
from buffett.common.constants.table import FQ_FAC
from buffett.common.error import PreStepError
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.pendulum import (
    DateSpan,
    convert_date,
    convert_datetime,
)
from buffett.common.tools import dataframe_is_valid, dataframe_not_valid
from buffett.download.handler.stock.dc_fhpg import DcFhpgHandler
from buffett.download.mysql import Operator
from buffett.download.types import CombType, SourceType, FreqType, FuquanType

HFQ_COMB = CombType(source=SourceType.AK_DC, freq=FreqType.DAY, fuquan=FuquanType.HFQ)
BFQ_COMB = CombType(source=SourceType.AK_DC, freq=FreqType.DAY, fuquan=FuquanType.BFQ)
THD1, THD2, THD3 = 0.9, 0.995, 0.999
ST, ED = 0.02, 0.06
EPS = 1e-10
TAG = "tag"
COLs = [CLOSE, OPEN, HIGH, LOW]
CUR_DATE, PRE_DATE = "current_date", "previous_date"


class FuquanAnalyst:
    def __init__(self, operator: Operator, datasource_op: Operator):
        self._operator = operator
        self._datasource_op = datasource_op
        self._fhpg_handler = DcFhpgHandler(operator=self._datasource_op)
        self._dataman = DataManager(operator=self._datasource_op)
        self._factor = None
        self._logger = LoggerBuilder.build(FuquanAnalystLogger)()

    def calculate(self, span: DateSpan):
        """
        计算时间范围内的分红配股信息

        :param span:
        :return:
        """
        # 获取StockList
        stock_list = get_stock_list(operator=self._datasource_op)
        # 获取分红配股数据
        fhpg_infos = self._fhpg_handler.select_data(index=False)
        if dataframe_not_valid(fhpg_infos):
            raise PreStepError(FuquanAnalyst, DcFhpgHandler)
        fhpg_infos = fhpg_infos[span.is_insides(fhpg_infos[DATE])]
        # 获取不含分红配股信息的StockList
        stocks_without_f = self._split_stock_list(
            fhpg_infos=fhpg_infos, stock_list=stock_list
        )
        # Calculations
        data_with_f = None
        if dataframe_is_valid(fhpg_infos):
            data_with_f = self._calc_with_fhpg(fhpg_infos=fhpg_infos, span=span)
        data_without_f = None
        if dataframe_is_valid(stocks_without_f):
            data_without_f = self._calc_without_fhpg(
                stock_list=stocks_without_f, span=span
            )
        self._factor = pd.concat([data_with_f, data_without_f])
        self._save_to_database(self._factor)

    @classmethod
    def _split_stock_list(
        cls, stock_list: DataFrame, fhpg_infos: DataFrame
    ) -> DataFrame:
        """
        对股票清单按照是否有分红配股信息进行区分

        :param stock_list:      股票清单
        :param fhpg_infos:      分红配股信息
        :return:
        """
        codes_with_f = fhpg_infos[[CODE]].drop_duplicates()
        codes_without_f = pd.merge(codes_with_f, stock_list, on=[CODE], how="left")
        stocks_without_f = pd.subtract(stock_list, codes_without_f)
        return stocks_without_f

    def _calc_with_fhpg(self, span: DateSpan, fhpg_infos: DataFrame) -> DataFrame:
        """
        计算携带有分红跑配股信息的股票

        :param span:        计算周期
        :param fhpg_infos:  分红配股信息
        :return:
        """
        span = DateSpan(convert_date(span.start), convert_date(span.end))
        groups = fhpg_infos.groupby(by=[CODE])
        datas = [
            self._calc_1stock_with_fhpg(fhpg_info=group, span=span)
            for code, group in groups
        ]
        data = pd.concat(datas)
        return data

    def _calc_1stock_with_fhpg(
        self, fhpg_info: DataFrame, span: DateSpan
    ) -> Optional[DataFrame]:
        """
        计算携带有分红跑配股信息的一支股票

        :param fhpg_info:   分红配股信息
        :param span:
        :return:            拟合表
        """
        code = fhpg_info[CODE].values[0]
        self._logger.info_start_calculate(code)
        # 获取数据
        bfq_info = self._dataman.select_data(
            para=Para().with_comb(BFQ_COMB).with_span(span).with_code(code), index=False
        )
        if dataframe_not_valid(bfq_info):
            return
        hfq_info = self._dataman.select_data(
            para=Para().with_comb(HFQ_COMB).with_span(span).with_code(code), index=False
        )
        # 过滤fhgp_info
        _st, _ed = bfq_info[DATE].iloc[0], bfq_info[DATE].iloc[-1]
        _f0 = DataFrame(
            {
                DATE: bfq_info[DATE],
                CUR_DATE: bfq_info[DATE],
                PRE_DATE: np.concatenate([[NAN], bfq_info[DATE].values[:-1]]),
            }
        )
        # 将除权日与数据起始日连接，作为区间起点
        # 股票可能一天有两条记录（e.g.派息+增发），数据起始日也可能与除权日重叠，因此需要去重
        # 指定的span可能大于不复权和后复权数据的区间，因此需要筛选
        _f1 = pd.concat(
            [
                DataFrame({DATE: [_st]}),
                fhpg_info[(fhpg_info[DATE] >= _st) & (fhpg_info[DATE] <= _ed)][[DATE]],
            ]
        ).drop_duplicates()
        _f2 = pd.merge(_f1, _f0, how="outer", on=[DATE], sort=True)
        _f3 = _f2.fillna(method="bfill")
        _f4 = pd.merge(_f1, _f3, how="left", on=[DATE])
        _f5 = np.concatenate([_f4[CUR_DATE].values, _f4[PRE_DATE].values[1:], [_ed]])
        _f5.sort()
        fhpg_info = DataFrame({DATE: _f5})
        # 筛选数据
        bfq_info = pd.merge(fhpg_info, bfq_info, on=[DATE], how="left")
        hfq_info = pd.merge(fhpg_info, hfq_info, on=[DATE], how="left")
        # 计算a， b
        data = self._calc_segment(
            bfq_arr=bfq_info[CLOSE].values,
            hfq_arr=hfq_info[CLOSE].values,
            date_arr=fhpg_info[DATE].values,
        )
        self._fix_segment(
            data=data,
            bfq_info=bfq_info,
            hfq_info=hfq_info,
        )
        data[CODE] = code
        self._logger.info_end_calculate(code)
        return data

    def _calc_without_fhpg(self, stock_list: DataFrame, span) -> DataFrame:
        """
        计算没有分红跑配股信息的股票

        :param stock_list:
        :param span:
        :return:
        """
        datas = [self._calc_1stock_without_fhpg(x, span) for x in stock_list[CODE]]
        data = pd.concat(datas)
        return data

    def _calc_1stock_without_fhpg(
        self, code: str, span: DateSpan
    ) -> Optional[DataFrame]:
        """
        计算没有分红跑配股信息的一支股票

        :param code:
        :param span:
        :return:
        """
        self._logger.info_start_calculate(code)
        # 获取数据
        bfq_info = self._dataman.select_data(
            para=Para().with_comb(BFQ_COMB).with_span(span).with_code(code), index=False
        )
        if dataframe_not_valid(bfq_info):
            return
        hfq_info = self._dataman.select_data(
            para=Para().with_comb(HFQ_COMB).with_span(span).with_code(code), index=False
        )
        bfq_arr = bfq_info[CLOSE].values
        hfq_arr = hfq_info[CLOSE].values
        num = len(bfq_arr)
        """
        向量夹角公式：
     　　cosα = A*B / (|A|*|B|)
        A = (x1,y1),B = (x2,y2);
　　     cosα =(x1*x2+y1*y2) / √((x1^2+y1^2)*(x2^2+y2^2))
        """
        x1, x2, x3 = bfq_arr[:-2], bfq_arr[1:-1], bfq_arr[2:]  # [  2,...]
        vx1, vx2, vx3 = x1 - x2, x1 - x3, x2 - x3
        y1, y2, y3 = hfq_arr[:-2], hfq_arr[1:-1], hfq_arr[2:]
        vy1, vy2 = y1 - y2, y1 - y3
        _a0 = vx1 * vx2 + vy1 * vy2 + EPS
        _a1 = ((vx1**2 + vy1**2) * (vx2**2 + vy2**2)) ** 0.5 + EPS
        abs_cos = np.abs(_a0 / _a1)
        # 应用分段门限
        _t0 = np.vectorize(min)(np.abs(vx1), np.abs(vx2), np.abs(vx3))
        _t1, _t3 = _t0 < ST, _t0 >= ED
        _t2 = 1 - _t1 - _t3
        thd = _t1 * THD1 + _t2 * THD2 + _t3 * THD3
        ind = abs_cos > thd
        # 配对
        ix = np.arange(num - 2)[ind == 0]
        _i = []
        i = 0
        num_i = len(ix)
        while i < num_i - 1:
            if ix[i + 1] - ix[i] == 1:
                _i.append(ix[i])
                _i.append(ix[i + 1])
                i += 2
            else:
                _i.append(ix[i])
                _i.append(ix[i] + 1)
                i += 1
        ix = np.array(_i)
        #
        ix = ix + 1
        """ Head
        False, True , True     [  0:  0] [  1:...]   
        False, False, True     [  0:  1] [  2:...]
        True , False, False    [  0:  2] [  3:...]
        True , True , False    [  0:  3] [  4:...]
        """
        if not ix[0] and ix[1]:  # False, True
            ix = np.concatenate([[0, 0], ix])
        else:  # False, False
            ix = np.concatenate([[0], ix])
        """ Tail
        False ,True , True      [...: -5] [ -4: -1]
        False, False. True      [...: -4] [ -3: -1]
        True , False, False     [...: -3] [ -2: -1]
        True , True , False     [...: -2] [ -1: -1]
            
        """
        if ix[-2] and not ix[-1]:  # True, False
            ix = np.concatenate([ix, [num - 1, num - 1]])
        else:
            ix = np.concatenate([ix, [num - 1]])
        # 计算a, b
        data = self._calc_segment(
            bfq_arr=bfq_info[CLOSE].values[ix],
            hfq_arr=hfq_info[CLOSE].values[ix],
            date_arr=bfq_info[DATE].values[ix],
        )
        self._fix_segment(
            data=data,
            bfq_info=bfq_info,
            hfq_info=hfq_info,
        )
        data[CODE] = code
        #
        self._logger.info_end_calculate(code)
        return data

    @classmethod
    def _calc_segment(cls, bfq_arr: ndarray, hfq_arr: ndarray, date_arr: ndarray):
        """
        计算每个分段的a,b

        :param bfq_arr:    不复权数据（日线）
        :param hfq_arr:    复权数据（日线）
        :param date_arr:       日期
        :return:
        """
        # 使用头和尾计算插值a, b
        """
        有a * x0 + b = y0, a * x1 + b = y1 (x = bfq_data, y = hfq_data)
        则a = (y0 - y1) / (x0 - x1), b = y0 - ax0
        """
        num = len(bfq_arr)
        x0, x1 = bfq_arr[0:num:2], bfq_arr[1:num:2]
        y0, y1 = hfq_arr[0:num:2], hfq_arr[1:num:2]
        a = (y0 - y1) / (x0 - x1)
        b = y0 - a * x0
        #
        rs, re = date_arr[0:num:2], date_arr[1:num:2]
        res = np.concatenate([rs, re, a, b]).reshape((num // 2, 4), order="F")
        res = DataFrame(res, columns=[START_DATE, END_DATE, A, B])
        return res

    @classmethod
    def _fix_segment(
        cls, data: DataFrame, bfq_info: DataFrame, hfq_info: DataFrame
    ) -> None:
        """
        对于首尾相同导致除0的计算进行修正

        :param data:
        :param bfq_info:
        :param hfq_info:
        :return:
        """
        if not pd.isna(data[A]).any():
            return
        a_ = data[A].values
        bfq_low, bfq_high = bfq_info[LOW].values, bfq_info[HIGH].values
        hfq_low, hfq_high = hfq_info[LOW].values, hfq_info[HIGH].values
        for i in range(len(a_)):
            if not pd.isna(a_[i]):
                continue
            region = [2 * i, 2 * i + 1]
            bfq_arr = np.array([np.min(bfq_low[region]), np.max(bfq_high[region])])
            hfq_arr = np.array([np.min(hfq_low[region]), np.max(hfq_high[region])])
            date_arr = np.array([data[START_DATE].iloc[i], data[END_DATE].iloc[i]])
            fix_data = cls._calc_segment(
                bfq_arr=bfq_arr, hfq_arr=hfq_arr, date_arr=date_arr
            )
            data.iloc[i, 2:] = fix_data.iloc[0, 2:]  # 覆盖原NA结果

    def _save_to_database(self, data: DataFrame) -> None:
        """
        将数据保存至数据库

        :param data:
        :return:
        """
        self._operator.create_table(name=FQ_FAC, meta=FQ_FAC_META)
        self._operator.insert_data(name=FQ_FAC, df=data)

    def reform_to_hfq(self, code: str, df: DataFrame):
        """
        将不复权数据转换成后复权数据

        :param code     股票代码
        :param df       待转换的数据
        :return:
        """
        data = self._prepare_for_reform(code=code, df=df)
        x = data[COLs].values
        a = data[[A]].values
        b = data[[B]].values
        y = x * a + b
        res = DataFrame(y, columns=COLs)
        res.index = df.index
        return res

    def reform_to_bfq(self, code: str, df: DataFrame):
        """
        将后复权数据转换成不复权数据

        :param code     股票代码
        :param df       待转换的数据
        :return:
        """
        data = self._prepare_for_reform(code=code, df=df)
        y = data[COLs].values
        a = data[[A]].values
        b = data[[B]].values
        """
        y = a * x + b
        x = (y - b) / a
        """
        x = (y - b) / a
        res = DataFrame(x, columns=COLs)
        res.index = df.index
        return res

    def _prepare_for_reform(self, code: str, df: DataFrame) -> DataFrame:
        """
        准备待转换数据

        :param code     股票代码
        :param df       待转换的数据
        :return:
        """
        if self._factor is None:
            self._factor = self._operator.select_data(name=FQ_FAC, meta=FQ_FAC_META)
        factor = self._factor
        factor = factor[factor[CODE] == code]
        if DATE in df.index.names:
            KEY = DATE
            factor = DataFrame(
                {
                    DATE: factor[START_DATE],
                    A: factor[A],
                    B: factor[B],
                }
            )
        else:
            KEY = DATETIME
            factor = DataFrame(
                {
                    DATETIME: factor[START_DATE].apply(
                        lambda x: convert_datetime(x).add(hours=9, minutes=35)
                    ),
                    A: factor[A],
                    B: factor[B],
                }
            )
        factor = factor.sort_values(by=[KEY])
        df = df.reset_index()
        df = pd.merge(df, factor, how="left", on=[KEY])
        if pd.isna(df[A].iloc[0]):
            dates = factor[DATE]
            date = dates[dates < df[DATE].iloc[0]].max()
            df_f = factor[factor[DATE] == date]
            df[A].iloc[0] = df_f[A]
            df[B].iloc[0] = df_f[B]
        df = df.fillna(method="ffill")
        return df


class FuquanAnalystLogger(Logger):
    @classmethod
    def info_start_calculate(cls, code):
        Logger.info(f"Start to calculate Fuquan Factor for {code}.")

    @classmethod
    def info_end_calculate(cls, code):
        Logger.info(f"Successfully calculate Fuquan Factor for {code}.")
