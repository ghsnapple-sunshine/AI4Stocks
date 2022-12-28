from typing import Optional

from buffett.adapter.numpy import np, NAN
from buffett.adapter.pandas import DataFrame, pd
from buffett.analysis import Para
from buffett.analysis.study.supporter import DataManager
from buffett.analysis.study.tools import get_stock_list
from buffett.analysis.types import CombExType
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
from buffett.common.interface import ProducerConsumer
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.pendulum import (
    DateSpan,
    convert_date,
    convert_datetime,
)
from buffett.common.tools import dataframe_is_valid, dataframe_not_valid
from buffett.common.wrapper import Wrapper
from buffett.download.handler.stock import DcFhpgHandler
from buffett.download.mysql import Operator
from buffett.download.types import SourceType, FreqType, FuquanType

HFQ_COMB = CombExType(source=SourceType.AK_DC, freq=FreqType.DAY, fuquan=FuquanType.HFQ)
BFQ_COMB = CombExType(source=SourceType.AK_DC, freq=FreqType.DAY, fuquan=FuquanType.BFQ)
THD1, THD2, THD3 = 0.9, 0.995, 0.999
ST, ED = 0.02, 0.06
EPS = 1e-10
ID = "id"
BFQ_LOW, BFQ_HIGH, HFQ_LOW, HFQ_HIGH = "bfq_low", "bfq_high", "hfq_low", "hfq_high"
CUR_DATE, PRE_DATE = "current_date", "previous_date"
COLs = [OPEN, CLOSE, HIGH, LOW]
AGG = {
    START_DATE: "min",
    END_DATE: "max",
    BFQ_LOW: "min",
    BFQ_HIGH: "max",
    HFQ_LOW: "min",
    HFQ_HIGH: "max",
}


class FuquanAnalyst:
    def __init__(self, operator: Operator, datasource_op: Operator):
        self._operator = operator
        self._datasource_op = datasource_op
        self._fhpg_handler = DcFhpgHandler(operator=datasource_op)
        self._dataman = DataManager(datasource_op=datasource_op)
        self._factors = None
        self._span = None
        self._logger = LoggerBuilder.build(FuquanAnalystLogger)()

    def calculate(self, span: DateSpan):
        """
        计算时间范围内的分红配股信息

        :param span:
        :return:
        """
        # 准备工作区
        self._span = DateSpan(convert_date(span.start), convert_date(span.end))
        self._factors = {}
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
        if dataframe_is_valid(fhpg_infos):
            self._calc_with_fhpg(fhpg_infos=fhpg_infos)
        if dataframe_is_valid(stocks_without_f):
            self._calc_without_fhpg(stock_list=stocks_without_f)
        self._save_to_database()

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

    def _calc_with_fhpg(self, fhpg_infos: DataFrame) -> None:
        """
        计算携带有分红跑配股信息的股票

        :param fhpg_infos:  分红配股信息
        :return:
        """
        groups = fhpg_infos.groupby(by=[CODE])
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._get_data_with_fhpg),
            consumer=Wrapper(self._calc_1stock_with_fhpg),
            args_map=groups,
            queue_size=30,
            task_num=fhpg_infos[CODE].drop_duplicates().size,
        )
        prod_cons.run()

    def _get_data_with_fhpg(self, group: tuple[str, DataFrame]):
        """
        获取带有分红配股信息的股票数据

        :param group:       股票代码，分红配股信息
        :return:
        """
        code, fhpg_info = group
        span = self._span
        self._logger.info_start_calculate(code)
        # 获取数据
        bfq_info = self._dataman.select_data(
            para=Para().with_comb(BFQ_COMB).with_span(span).with_code(code), index=False
        )
        hfq_info = self._dataman.select_data(
            para=Para().with_comb(HFQ_COMB).with_span(span).with_code(code), index=False
        )
        return code, fhpg_info, bfq_info, hfq_info

    def _calc_1stock_with_fhpg(
        self, info: tuple[str, DataFrame, DataFrame, DataFrame]
    ) -> Optional[DataFrame]:
        """
        计算携带有分红跑配股信息的一支股票

        :param info:        股票代码, 分红配股信息, 不复权信息, 后复权信息
        :return:            拟合表
        """
        code, fhpg_info, bfq_info, hfq_info = info
        if dataframe_not_valid(bfq_info):
            self._logger.warning_calculate_end(code)
            return
        # 过滤fhgp_info
        _st, _ed = bfq_info[DATE].iloc[0], bfq_info[DATE].iloc[-1]
        # 将除权日与数据起始日连接，作为区间起点
        # 股票可能一天有两条记录（e.g.派息+增发），数据起始日也可能与除权日重叠，因此需要去重
        # 指定的span可能大于不复权和后复权数据的区间，因此需要筛选
        _f0 = bfq_info[[DATE]]
        _f1 = pd.concat(
            [
                DataFrame({DATE: [_st]}),
                fhpg_info[(fhpg_info[DATE] >= _st) & (fhpg_info[DATE] <= _ed)][[DATE]],
            ]
        ).drop_duplicates()
        _f1[ID] = range(len(_f1))
        _f2 = pd.merge(_f1, _f0, how="outer", on=[DATE], sort=True)
        fhpg_group = _f2.fillna(method="ffill")
        fhpg_group[START_DATE] = fhpg_group[DATE]
        fhpg_group[END_DATE] = fhpg_group[DATE]
        # 筛选数据
        bfq_hfq = DataFrame(
            {
                DATE: bfq_info[DATE],
                BFQ_LOW: bfq_info[LOW],
                BFQ_HIGH: bfq_info[HIGH],
                HFQ_LOW: hfq_info[LOW],
                HFQ_HIGH: hfq_info[HIGH],
            }
        )
        bfq_hfq = (
            pd.merge(bfq_hfq, fhpg_group, how="left", on=[DATE])
            .groupby(by=[ID])
            .aggregate(AGG)
        )
        # 计算a和b
        self._calc_a_n_b(bfq_hfq, code)

    def _calc_a_n_b(self, bfq_hfq, code):
        """
        计算a和b

        :param bfq_hfq:     不复权数据和后复权数据拼接的结果
        :param code:        股票代码
        :return:
        """
        bfq_hfq[A] = (bfq_hfq[HFQ_LOW] - bfq_hfq[HFQ_HIGH]) / (
            bfq_hfq[BFQ_LOW] - bfq_hfq[BFQ_HIGH]
        )
        bfq_hfq[B] = bfq_hfq[HFQ_LOW] - bfq_hfq[A] * bfq_hfq[BFQ_LOW]
        data = bfq_hfq[[START_DATE, END_DATE, A, B]]
        self._factors[code] = data
        self._logger.info_end_calculate(code)

    def _calc_without_fhpg(self, stock_list: DataFrame) -> None:
        """
        计算没有分红跑配股信息的股票

        :param stock_list:      股票清单
        :return:
        """
        prod_cons = ProducerConsumer(
            producer=Wrapper(self._get_data_without_fhpg),
            consumer=Wrapper(self._calc_1stock_without_fhpg),
            args_map=stock_list[CODE],
            queue_size=30,
            task_num=len(stock_list),
        )
        prod_cons.run()

    def _get_data_without_fhpg(self, code: str):
        """
        获取没有分红配股信息的股票数据

        :param code:        股票代码
        :return:
        """
        span = self._span
        self._logger.info_start_calculate(code)
        # 获取数据
        bfq_info = self._dataman.select_data(
            para=Para().with_comb(BFQ_COMB).with_span(span).with_code(code), index=False
        )
        hfq_info = self._dataman.select_data(
            para=Para().with_comb(HFQ_COMB).with_span(span).with_code(code), index=False
        )
        return code, bfq_info, hfq_info

    def _calc_1stock_without_fhpg(self, info: tuple[str, DataFrame, DataFrame]) -> None:
        """
        计算没有分红跑配股信息的一支股票

        :param info:        股票代码，不复权数据，后复权数据
        :return:
        """
        code, bfq_info, hfq_info = info
        if dataframe_not_valid(bfq_info):
            self._logger.warning_calculate_end(code)
            return
        bfq_high_arr, bfq_low_arr = bfq_info[HIGH].values, bfq_info[LOW].values
        hfq_high_arr, hfq_low_arr = hfq_info[HIGH].values, hfq_info[LOW].values
        num = len(bfq_high_arr)
        ind = [False]
        """
        向量夹角公式：
     　　cosα = A*B / (|A|*|B|)
        A = (x1,y1),B = (x2,y2);
　　     cosα =(x1*x2+y1*y2) / √((x1^2+y1^2)*(x2^2+y2^2))
        """
        if num > 1:
            x1, x2, x3 = bfq_high_arr[:-1], bfq_low_arr[:-1], bfq_high_arr[1:]  # n-1
            vx1, vx2, vx3 = x1 - x2, x1 - x3, x2 - x3
            y1, y2, y3 = hfq_high_arr[:-1], hfq_low_arr[:-1], hfq_high_arr[1:]
            vy1, vy2 = y1 - y2, y1 - y3
            _a0 = vx1 * vx2 + vy1 * vy2 + EPS
            _a1 = ((vx1**2 + vy1**2) * (vx2**2 + vy2**2)) ** 0.5 + EPS
            abs_cos = np.abs(_a0 / _a1)
            # 应用分段门限
            _t0 = np.vectorize(min)(np.abs(vx1), np.abs(vx2), np.abs(vx3))
            _t1, _t3 = _t0 < ST, _t0 >= ED
            _t2 = 1 - _t1 - _t3
            thd = _t1 * THD1 + _t2 * THD2 + _t3 * THD3
            ind = np.concatenate([[False], abs_cos > thd])
        # 分段
        _b = DataFrame(
            {
                START_DATE: bfq_info[DATE],
                END_DATE: bfq_info[DATE],
                BFQ_LOW: bfq_info[LOW],
                BFQ_HIGH: bfq_info[HIGH],
                HFQ_LOW: hfq_info[LOW],
                HFQ_HIGH: hfq_info[HIGH],
                ID: range(num),
            }
        )
        _b.loc[ind, ID] = NAN
        _b = _b.fillna(method="ffill")
        bfq_hfq = _b.groupby(ID).aggregate(AGG)
        # 计算a和b
        self._calc_a_n_b(bfq_hfq, code)

    def _save_to_database(self) -> None:
        """
        将复权因子保存至数据库

        :return:
        """
        self._operator.create_table(name=FQ_FAC, meta=FQ_FAC_META)

        def _get_sub_table(k: str, v: DataFrame):
            v[CODE] = k
            return v

        data = pd.concat([_get_sub_table(k, v) for k, v in self._factors.items()])
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
        if self._factors is None:
            groupby = self._operator.select_data(name=FQ_FAC, meta=FQ_FAC_META).groupby(
                by=[CODE]
            )
            self._factors = dict((k, v[[START_DATE, A, B]]) for k, v in groupby)
        factor = self._factors[code]
        if DATE in df.index.names:
            KEY = DATE
            conv = lambda x: x
        else:
            KEY = DATETIME
            conv = lambda x: convert_datetime(x).add(hours=9, minutes=35)
        factor = DataFrame(
            {
                KEY: factor[START_DATE].apply(conv),
                A: factor[A],
                B: factor[B],
            }
        ).sort_values(by=[KEY])
        df = df.reset_index()
        df = pd.merge(df, factor, how="left", on=[KEY])
        if pd.isna(df[A].iloc[0]):
            dates = factor[factor[KEY] < df.loc[0, KEY]]
            if dataframe_is_valid(dates):
                date = dates.iloc[-1, :]
                df.loc[0, A] = date[A]
                df.loc[0, B] = date[B]
            else:
                self._logger.warning_data_error(code=code, key=KEY, df=df)
        df = df.fillna(method="ffill")
        return df


class FuquanAnalystLogger(Logger):
    @classmethod
    def info_start_calculate(cls, code: str):
        Logger.info(f"Start to calculate Fuquan Factor for {code}.")

    @classmethod
    def info_end_calculate(cls, code: str):
        Logger.info(f"Successfully calculate Fuquan Factor for {code}.")

    @classmethod
    def warning_calculate_end(cls, code: str):
        Logger.warning(
            f"End calculate Fuquan Factor for {code}, nothing is found for calculation."
        )

    @classmethod
    def warning_data_error(cls, code: str, key: str, df: DataFrame):
        error_data = df[pd.isna(df[A])][key]
        span = DateSpan(error_data.min(), error_data.max())
        Logger.warning(f"Fuquan Factor for {code} is missed in {span}.")
