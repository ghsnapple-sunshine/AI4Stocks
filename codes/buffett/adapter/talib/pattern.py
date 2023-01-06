from typing import Union

from talib import (
    CDL2CROWS,
    CDL3BLACKCROWS,
    CDL3INSIDE,
    CDL3LINESTRIKE,
    CDL3OUTSIDE,
    CDL3STARSINSOUTH,
    CDL3WHITESOLDIERS,
    CDLABANDONEDBABY,
    CDLADVANCEBLOCK,
    CDLBELTHOLD,
    CDLBREAKAWAY,
    CDLCLOSINGMARUBOZU,
    CDLCONCEALBABYSWALL,
    CDLCOUNTERATTACK,
    CDLDARKCLOUDCOVER,
    CDLDOJI,
    CDLDOJISTAR,
    CDLDRAGONFLYDOJI,
    CDLENGULFING,
    CDLEVENINGDOJISTAR,
    CDLEVENINGSTAR,
    CDLGAPSIDESIDEWHITE,
    CDLGRAVESTONEDOJI,
    CDLHAMMER,
    CDLHANGINGMAN,
    CDLHARAMI,
    CDLHARAMICROSS,
    CDLHIGHWAVE,
    CDLHIKKAKE,
    CDLHIKKAKEMOD,
    CDLHOMINGPIGEON,
    CDLIDENTICAL3CROWS,
    CDLINNECK,
    CDLINVERTEDHAMMER,
    CDLKICKING,
    CDLKICKINGBYLENGTH,
    CDLLADDERBOTTOM,
    CDLLONGLEGGEDDOJI,
    CDLLONGLINE,
    CDLMARUBOZU,
    CDLMATCHINGLOW,
    CDLMATHOLD,
    CDLMORNINGDOJISTAR,
    CDLMORNINGSTAR,
    CDLONNECK,
    CDLPIERCING,
    CDLRICKSHAWMAN,
    CDLRISEFALL3METHODS,
    CDLSEPARATINGLINES,
    CDLSHOOTINGSTAR,
    CDLSHORTLINE,
    CDLSPINNINGTOP,
    CDLSTALLEDPATTERN,
    CDLSTICKSANDWICH,
    CDLTAKURI,
    CDLTASUKIGAP,
    CDLTHRUSTING,
    CDLTRISTAR,
    CDLUNIQUE3RIVER,
    CDLUPSIDEGAP2CROWS,
    CDLXSIDEGAP3METHODS,
)

from buffett.adapter.constants.pattern import (
    TWOCROWS,
    THREEBLACKCROWS,
    THREEINSIDE,
    THREELINESTRIKE,
    THREEOUTSIDE,
    THREESTARSINSOUTH,
    THREEWHITESOLDIERS,
    ABANDONEDBABY,
    ADVANCEBLOCK,
    BELTHOLD,
    BREAKAWAY,
    CLOSINGMARUBOZU,
    CONCEALBABYSWALL,
    COUNTERATTACK,
    DARKCLOUDCOVER,
    DOJI,
    DOJISTAR,
    DRAGONFLYDOJI,
    ENGULFING,
    EVENINGDOJISTAR,
    EVENINGSTAR,
    GAPSIDESIDEWHITE,
    GRAVESTONEDOJI,
    HAMMER,
    HANGINGMAN,
    HARAMI,
    HARAMICROSS,
    HIGHWAVE,
    HIKKAKE,
    HIKKAKEMOD,
    HOMINGPIGEON,
    IDENTICAL3CROWS,
    INNECK,
    INVERTEDHAMMER,
    KICKING,
    KICKINGBYLENGTH,
    LADDERBOTTOM,
    LONGLEGGEDDOJI,
    LONGLINE,
    MARUBOZU,
    MATCHINGLOW,
    MATHOLD,
    MORNINGDOJISTAR,
    MORNINGSTAR,
    ONNECK,
    PIERCING,
    RICKSHAWMAN,
    RISEFALL3METHODS,
    SEPARATINGLINES,
    SHOOTINGSTAR,
    SHORTLINE,
    SPINNINGTOP,
    STALLEDPATTERN,
    STICKSANDWICH,
    TAKURI,
    TASUKIGAP,
    THRUSTING,
    TRISTAR,
    UNIQUE3RIVER,
    UPSIDEGAP2CROWS,
    XSIDEGAP3METHODS,
)
from buffett.adapter.numpy import ndarray
from buffett.adapter.pandas import DataFrame, Series
from buffett.adapter.talib.tools import dataframe_to_ndarraydict, call_talib_cdl


class PatternRecognize:
    """
    talib结果为0表示未识别到此信号，100表示识别到买入信号，-100表示识别到卖出信号
    """

    @staticmethod
    def all(inputs: DataFrame) -> DataFrame:
        """
        一次性计算所有指标

        :param inputs:
        :return:
        """
        dic = dataframe_to_ndarraydict(inputs)
        datas = [
            PatternRecognize.two_crows(dic),
            PatternRecognize.three_black_crows(dic),
            PatternRecognize.three_inside_up_o_down(dic),
            PatternRecognize.three_line_strike(dic),
            PatternRecognize.three_outside_up_o_down(dic),
            PatternRecognize.three_stars_in_the_south(dic),
            PatternRecognize.three_advancing_white_soldiers(dic),
            PatternRecognize.abandoned_baby(dic),
            PatternRecognize.advance_block(dic),
            PatternRecognize.belt_hold(dic),
            PatternRecognize.break_away(dic),
            PatternRecognize.closing_marubozu(dic),
            PatternRecognize.concealing_baby_swallow(dic),
            PatternRecognize.counter_attack(dic),
            PatternRecognize.dark_cloud_cover(dic),
            PatternRecognize.doji(dic),
            PatternRecognize.doji_star(dic),
            PatternRecognize.dragonfily_doji_star(dic),
            PatternRecognize.engulfing_pattern(dic),
            PatternRecognize.evening_doji_star(dic),
            PatternRecognize.evening_star(dic),
            PatternRecognize.xside_by_side_white_lines(dic),
            PatternRecognize.gravestone_doji(dic),
            PatternRecognize.hammer(dic),
            PatternRecognize.hanging_man(dic),
            PatternRecognize.harami_pattern(dic),
            PatternRecognize.harami_cross_pattern(dic),
            PatternRecognize.high_wave_candle(dic),
            PatternRecognize.hikkake_pattern(dic),
            PatternRecognize.modified_hikkake_pattern(dic),
            PatternRecognize.homing_pigeon(dic),
            PatternRecognize.idential_three_crows(dic),
            PatternRecognize.in_neck_pattern(dic),
            PatternRecognize.inverted_hammer(dic),
            PatternRecognize.kicking(dic),
            PatternRecognize.kicking_bull_o_bear(dic),
            PatternRecognize.ladder_bottom(dic),
            PatternRecognize.long_legged_doji(dic),
            PatternRecognize.long_line_candle(dic),
            PatternRecognize.marubozu(dic),
            PatternRecognize.matching_low(dic),
            PatternRecognize.mat_hold(dic),
            PatternRecognize.moring_doji_star(dic),
            PatternRecognize.morning_star(dic),
            PatternRecognize.on_neck_pattern(dic),
            PatternRecognize.piercing_pattern(dic),
            PatternRecognize.rickshaw_man(dic),
            PatternRecognize.rising_o_falling_three_methods(dic),
            PatternRecognize.separating_lines(dic),
            PatternRecognize.shooting_star(dic),
            PatternRecognize.short_line_candle(dic),
            PatternRecognize.spinning_top(dic),
            PatternRecognize.stalled_pattern(dic),
            PatternRecognize.stick_sandwich(dic),
            PatternRecognize.takuri(dic),
            PatternRecognize.tasuki_gap(dic),
            PatternRecognize.thrusting_pattern(dic),
            PatternRecognize.tristar_pattern(dic),
            PatternRecognize.unique_three_river(dic),
            PatternRecognize.upside_gap_two_crows(dic),
            PatternRecognize.xside_gap_three_methods(dic),
        ]
        df = DataFrame(data=datas).T
        df.index = inputs.index
        return df

    @staticmethod
    def two_crows(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Two Crows 两只乌鸦
        简介：三日K线模式，第一天长阳，第二天高开收阴，第三天再次高开继续收阴， 收盘比前一日收盘价低，预示股价下跌。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDL2CROWS, TWOCROWS, inputs)

    @staticmethod
    def three_black_crows(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Three Black Crows 三只乌鸦
        简介：三日K线模式，连续三根阴线，每日收盘价都下跌且接近最低价， 每日开盘价都在上根K线实体内，预示股价下跌。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDL3BLACKCROWS, THREEBLACKCROWS, inputs)

    @staticmethod
    def three_inside_up_o_down(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称： Three Inside Up/Down 三内部上涨和下跌
        简介：三日K线模式，母子信号+长K线，以三内部上涨为例，K线为阴阳阳， 第三天收盘价高于第一天开盘价，第二天K线在第一天K线内部，预示着股价上涨。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDL3INSIDE, THREEINSIDE, inputs)

    @staticmethod
    def three_line_strike(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称： Three-Line Strike 三线打击
        简介：四日K线模式，前三根阳线，每日收盘价都比前一日高， 开盘价在前一日实体内，第四日市场高开，收盘价低于第一日开盘价，预示股价下跌。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDL3LINESTRIKE, THREELINESTRIKE, inputs)

    @staticmethod
    def three_outside_up_o_down(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Three Outside Up/Down 三外部上涨和下跌
        简介：三日K线模式，与三内部上涨和下跌类似，K线为阴阳阳，但第一日与第二日的K线形态相反， 以三外部上涨为例，第一日K线在第二日K线内部，预示着股价上涨。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDL3OUTSIDE, THREEOUTSIDE, inputs)

    @staticmethod
    def three_stars_in_the_south(
        inputs: Union[DataFrame, dict[str, ndarray]]
    ) -> Series:
        """
        名称：Three Stars In The South 南方三星
        简介：三日K线模式，与大敌当前相反，三日K线皆阴，第一日有长下影线， 第二日与第一日类似，K线整体小于第一日，第三日无下影线实体信号， 成交价格都在第一日振幅之内，预示下跌趋势反转，股价上升。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDL3STARSINSOUTH, THREESTARSINSOUTH, inputs)

    @staticmethod
    def three_advancing_white_soldiers(
        inputs: Union[DataFrame, dict[str, ndarray]]
    ) -> Series:
        """
        名称：Three Advancing White Soldiers 三个白兵
        简介：三日K线模式，三日K线皆阳， 每日收盘价变高且接近最高价，开盘价在前一日实体上半部，预示股价上升。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDL3WHITESOLDIERS, THREEWHITESOLDIERS, inputs)

    @staticmethod
    def abandoned_baby(
        inputs: Union[DataFrame, dict[str, ndarray]], penetration: float = 0.3
    ) -> Series:
        """
        名称：Abandoned Baby 弃婴
        简介：三日K线模式，第二日价格跳空且收十字星（开盘价与收盘价接近， 最高价最低价相差不大），预示趋势反转，发生在顶部下跌，底部上涨。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :param penetration
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(
            CDLABANDONEDBABY, ABANDONEDBABY, inputs, penetration=penetration
        )

    @staticmethod
    def advance_block(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Advance Block 大敌当前
        简介：三日K线模式，三日都收阳，每日收盘价都比前一日高， 开盘价都在前一日实体以内，实体变短，上影线变长。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLADVANCEBLOCK, ADVANCEBLOCK, inputs)

    @staticmethod
    def belt_hold(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Belt-hold 捉腰带线
        简介：两日K线模式，下跌趋势中，第一日阴线， 第二日开盘价为最低价，阳线，收盘价接近最高价，预示价格上涨。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLBELTHOLD, BELTHOLD, inputs)

    @staticmethod
    def break_away(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Breakaway 脱离
        简介：五日K线模式，以看涨脱离为例，下跌趋势中，第一日长阴线，第二日跳空阴线，延续趋势开始震荡， 第五日长阳线，收盘价在第一天收盘价与第二天开盘价之间，预示价格上涨。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLBREAKAWAY, BREAKAWAY, inputs)

    @staticmethod
    def closing_marubozu(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Closing Marubozu 收盘缺影线
        简介：一日K线模式，以阳线为例，最低价低于开盘价，收盘价等于最高价，预示着趋势持续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLCLOSINGMARUBOZU, CLOSINGMARUBOZU, inputs)

    @staticmethod
    def concealing_baby_swallow(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称： Concealing Baby Swallow 藏婴吞没
        简介：四日K线模式，下跌趋势中，前两日阴线无影线 ，第二日开盘、收盘价皆低于第二日，第三日倒锤头， 第四日开盘价高于前一日最高价，收盘价低于前一日最低价，预示着底部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLCONCEALBABYSWALL, CONCEALBABYSWALL, inputs)

    @staticmethod
    def counter_attack(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Counterattack 反击线
        简介：二日K线模式，与分离线类似。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLCOUNTERATTACK, COUNTERATTACK, inputs)

    @staticmethod
    def dark_cloud_cover(
        inputs: Union[DataFrame, dict[str, ndarray]], penetration: float = 0.3
    ) -> Series:
        """
        名称：Dark Cloud Cover 乌云压顶
        简介：二日K线模式，第一日长阳，第二日开盘价高于前一日最高价， 收盘价处于前一日实体中部以下，预示着股价下跌。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :param penetration
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(
            CDLDARKCLOUDCOVER, DARKCLOUDCOVER, inputs, penetration=penetration
        )

    @staticmethod
    def doji(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Doji 十字
        简介：一日K线模式，开盘价与收盘价基本相同。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLDOJI, DOJI, inputs)

    @staticmethod
    def doji_star(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Doji Star 十字星
        简介：一日K线模式，开盘价与收盘价基本相同，上下影线不会很长，预示着当前趋势反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLDOJISTAR, DOJISTAR, inputs)

    @staticmethod
    def dragonfily_doji_star(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Dragonfly Doji 蜻蜓十字/T形十字
        简介：一日K线模式，开盘后价格一路走低， 之后收复，收盘价与开盘价相同，预示趋势反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLDRAGONFLYDOJI, DRAGONFLYDOJI, inputs)

    @staticmethod
    def engulfing_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Engulfing Pattern 吞噬模式
        简介：两日K线模式，分多头吞噬和空头吞噬，以多头吞噬为例，第一日为阴线， 第二日阳线，第一日的开盘价和收盘价在第二日开盘价收盘价之内，但不能完全相同。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLENGULFING, ENGULFING, inputs)

    @staticmethod
    def evening_doji_star(
        inputs: Union[DataFrame, dict[str, ndarray]], penetration: float = 0.3
    ) -> Series:
        """
        名称：Evening Doji Star 十字暮星
        简介：三日K线模式，基本模式为暮星，第二日收盘价和开盘价相同，预示顶部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :param penetration
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(
            CDLEVENINGDOJISTAR, EVENINGDOJISTAR, inputs, penetration=penetration
        )

    @staticmethod
    def evening_star(
        inputs: Union[DataFrame, dict[str, ndarray]], penetration: float = 0.3
    ) -> Series:
        """
        名称：Evening Star 暮星
        简介：三日K线模式，与晨星相反，上升趋势中, 第一日阳线，第二日价格振幅较小，第三日阴线，预示顶部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :param penetration
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(
            CDLEVENINGSTAR, EVENINGSTAR, inputs, penetration=penetration
        )

    @staticmethod
    def xside_by_side_white_lines(
        inputs: Union[DataFrame, dict[str, ndarray]]
    ) -> Series:
        """
        名称：Up/Down-gap side-by-side white lines 向上/下跳空并列阳线
        简介：二日K线模式，上升趋势向上跳空，下跌趋势向下跳空, 第一日与第二日有相同开盘价，实体长度差不多，则趋势持续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLGAPSIDESIDEWHITE, GAPSIDESIDEWHITE, inputs)

    @staticmethod
    def gravestone_doji(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Gravestone Doji 墓碑十字/倒T十字
        简介：一日K线模式，开盘价与收盘价相同，上影线长，无下影线，预示底部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLGRAVESTONEDOJI, GRAVESTONEDOJI, inputs)

    @staticmethod
    def hammer(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Hammer 锤头
        简介：一日K线模式，实体较短，无上影线， 下影线大于实体长度两倍，处于下跌趋势底部，预示反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHAMMER, HAMMER, inputs)

    @staticmethod
    def hanging_man(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Hanging Man 上吊线
        简介：一日K线模式，形状与锤子类似，处于上升趋势的顶部，预示着趋势反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHANGINGMAN, HANGINGMAN, inputs)

    @staticmethod
    def harami_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Harami Pattern 母子线
        简介：二日K线模式，分多头母子与空头母子，两者相反，以多头母子为例，在下跌趋势中，第一日K线长阴， 第二日开盘价收盘价在第一日价格振幅之内，为阳线，预示趋势反转，股价上升。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHARAMI, HARAMI, inputs)

    @staticmethod
    def harami_cross_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Harami Cross Pattern 十字孕线
        简介：二日K线模式，与母子县类似，若第二日K线是十字线， 便称为十字孕线，预示着趋势反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHARAMICROSS, HARAMICROSS, inputs)

    @staticmethod
    def high_wave_candle(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：High-Wave Candle 风高浪大线
        简介：三日K线模式，具有极长的上/下影线与短的实体，预示着趋势反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHIGHWAVE, HIGHWAVE, inputs)

    @staticmethod
    def hikkake_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Hikkake Pattern 陷阱
        简介：三日K线模式，与母子类似，第二日价格在前一日实体范围内, 第三日收盘价高于前两日，反转失败，趋势继续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHIKKAKE, HIKKAKE, inputs)

    @staticmethod
    def modified_hikkake_pattern(
        inputs: Union[DataFrame, dict[str, ndarray]]
    ) -> Series:
        """
        名称：Modified Hikkake Pattern 修正陷阱
        简介：三日K线模式，与陷阱类似，上升趋势中，第三日跳空高开； 下跌趋势中，第三日跳空低开，反转失败，趋势继续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHIKKAKEMOD, HIKKAKEMOD, inputs)

    @staticmethod
    def homing_pigeon(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Homing Pigeon 家鸽
        简介：二日K线模式，与母子线类似，不同的的是二日K线颜色相同， 第二日最高价、最低价都在第一日实体之内，预示着趋势反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLHOMINGPIGEON, HOMINGPIGEON, inputs)

    @staticmethod
    def idential_three_crows(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Identical Three Crows 三胞胎乌鸦
        简介：三日K线模式，上涨趋势中，三日都为阴线，长度大致相等， 每日开盘价等于前一日收盘价，收盘价接近当日最低价，预示价格下跌。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLIDENTICAL3CROWS, IDENTICAL3CROWS, inputs)

    @staticmethod
    def in_neck_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：In-Neck Pattern 颈内线
        简介：二日K线模式，下跌趋势中，第一日长阴线， 第二日开盘价较低，收盘价略高于第一日收盘价，阳线，实体较短，预示着下跌继续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLINNECK, INNECK, inputs)

    @staticmethod
    def inverted_hammer(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Inverted Hammer 倒锤头
        简介：一日K线模式，上影线较长，长度为实体2倍以上， 无下影线，在下跌趋势底部，预示着趋势反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLINVERTEDHAMMER, INVERTEDHAMMER, inputs)

    @staticmethod
    def kicking(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Kicking 反冲形态
        简介：二日K线模式，与分离线类似，两日K线为秃线，颜色相反，存在跳空缺口。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLKICKING, KICKING, inputs)

    @staticmethod
    def kicking_bull_o_bear(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Kicking - bull/bear determined by the longer marubozu 由较长缺影线决定的反冲形态
        简介：二日K线模式，与反冲形态类似，较长缺影线决定价格的涨跌。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLKICKINGBYLENGTH, KICKINGBYLENGTH, inputs)

    @staticmethod
    def ladder_bottom(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Ladder Bottom 梯底
        简介：五日K线模式，下跌趋势中，前三日阴线， 开盘价与收盘价皆低于前一日开盘、收盘价，第四日倒锤头，第五日开盘价高于前一日开盘价， 阳线，收盘价高于前几日价格振幅，预示着底部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLLADDERBOTTOM, LADDERBOTTOM, inputs)

    @staticmethod
    def long_legged_doji(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Long Legged Doji 长脚十字
        简介：一日K线模式，开盘价与收盘价相同居当日价格中部，上下影线长， 表达市场不确定性。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLLONGLEGGEDDOJI, LONGLEGGEDDOJI, inputs)

    @staticmethod
    def long_line_candle(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Long Line Candle 长蜡烛
        简介：一日K线模式，K线实体长，无上下影线。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLLONGLINE, LONGLINE, inputs)

    @staticmethod
    def marubozu(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Marubozu 光头光脚/缺影线
        简介：一日K线模式，上下两头都没有影线的实体， 阴线预示着熊市持续或者牛市反转，阳线相反。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLMARUBOZU, MARUBOZU, inputs)

    @staticmethod
    def matching_low(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Matching Low 相同低价
        简介：二日K线模式，下跌趋势中，第一日长阴线， 第二日阴线，收盘价与前一日相同，预示底部确认，该价格为支撑位。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLMATCHINGLOW, MATCHINGLOW, inputs)

    @staticmethod
    def mat_hold(
        inputs: Union[DataFrame, dict[str, ndarray]], penetration: float = 0.3
    ) -> Series:
        """
        名称：Mat Hold 铺垫
        简介：五日K线模式，上涨趋势中，第一日阳线，第二日跳空高开影线， 第三、四日短实体影线，第五日阳线，收盘价高于前四日，预示趋势持续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :param penetration
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLMATHOLD, MATHOLD, inputs, penetration=penetration)

    @staticmethod
    def moring_doji_star(
        inputs: Union[DataFrame, dict[str, ndarray]], penetration: float = 0.3
    ) -> Series:
        """
        名称：Morning Doji Star 十字晨星
        简介：三日K线模式， 基本模式为晨星，第二日K线为十字星，预示底部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :param penetration
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(
            CDLMORNINGDOJISTAR, MORNINGDOJISTAR, inputs, penetration=penetration
        )

    @staticmethod
    def morning_star(
        inputs: Union[DataFrame, dict[str, ndarray]], penetration: float = 0.3
    ) -> Series:
        """
        名称：Morning Star 晨星
        简介：三日K线模式，下跌趋势，第一日阴线， 第二日价格振幅较小，第三天阳线，预示底部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :param penetration
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(
            CDLMORNINGSTAR, MORNINGSTAR, inputs, penetration=penetration
        )

    @staticmethod
    def on_neck_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：On-Neck Pattern 颈上线
        简介：二日K线模式，下跌趋势中，第一日长阴线，第二日开盘价较低， 收盘价与前一日最低价相同，阳线，实体较短，预示着延续下跌趋势。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLONNECK, ONNECK, inputs)

    @staticmethod
    def piercing_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Piercing Pattern 刺透形态
        简介：两日K线模式，下跌趋势中，第一日阴线，第二日收盘价低于前一日最低价， 收盘价处在第一日实体上部，预示着底部反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLPIERCING, PIERCING, inputs)

    @staticmethod
    def rickshaw_man(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Rickshaw Man 黄包车夫
        简介：一日K线模式，与长腿十字线类似， 若实体正好处于价格振幅中点，称为黄包车夫。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLRICKSHAWMAN, RICKSHAWMAN, inputs)

    @staticmethod
    def rising_o_falling_three_methods(
        inputs: Union[DataFrame, dict[str, ndarray]]
    ) -> Series:
        """
        名称：Rising/Falling Three Methods 上升/下降三法
        简介： 五日K线模式，以上升三法为例，上涨趋势中， 第一日长阳线，中间三日价格在第一日范围内小幅震荡， 第五日长阳线，收盘价高于第一日收盘价，预示股价上升。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLRISEFALL3METHODS, RISEFALL3METHODS, inputs)

    @staticmethod
    def separating_lines(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Separating Lines 分离线
        简介：二日K线模式，上涨趋势中，第一日阴线，第二日阳线， 第二日开盘价与第一日相同且为最低价，预示着趋势继续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLSEPARATINGLINES, SEPARATINGLINES, inputs)

    @staticmethod
    def shooting_star(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Shooting Star 射击之星
        简介：一日K线模式，上影线至少为实体长度两倍， 没有下影线，预示着股价下跌

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLSHOOTINGSTAR, SHOOTINGSTAR, inputs)

    @staticmethod
    def short_line_candle(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Short Line Candle 短蜡烛
        简介：一日K线模式，实体短，无上下影线

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLSHORTLINE, SHORTLINE, inputs)

    @staticmethod
    def spinning_top(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Spinning Top 纺锤
        简介：一日K线，实体小。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLSPINNINGTOP, SPINNINGTOP, inputs)

    @staticmethod
    def stalled_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Stalled Pattern 停顿形态
        简介：三日K线模式，上涨趋势中，第二日长阳线， 第三日开盘于前一日收盘价附近，短阳线，预示着上涨结束

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLSTALLEDPATTERN, STALLEDPATTERN, inputs)

    @staticmethod
    def stick_sandwich(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Stalled Pattern 停顿形态
        简介：三日K线模式，上涨趋势中，第二日长阳线， 第三日开盘于前一日收盘价附近，短阳线，预示着上涨结束

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLSTICKSANDWICH, STICKSANDWICH, inputs)

    @staticmethod
    def takuri(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Takuri (Dragonfly Doji with very long lower shadow) 探水竿
        简介：一日K线模式，大致与蜻蜓十字相同，下影线长度长。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLTAKURI, TAKURI, inputs)

    @staticmethod
    def tasuki_gap(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Tasuki Gap 跳空并列阴阳线
        简介：三日K线模式，分上涨和下跌，以上升为例， 前两日阳线，第二日跳空，第三日阴线，收盘价于缺口中，上升趋势持续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLTASUKIGAP, TASUKIGAP, inputs)

    @staticmethod
    def thrusting_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Thrusting Pattern 插入
        简介：二日K线模式，与颈上线类似，下跌趋势中，第一日长阴线，第二日开盘价跳空， 收盘价略低于前一日实体中部，与颈上线相比实体较长，预示着趋势持续。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLTHRUSTING, THRUSTING, inputs)

    @staticmethod
    def tristar_pattern(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Tristar Pattern 三星
        简介：三日K线模式，由三个十字组成， 第二日十字必须高于或者低于第一日和第三日，预示着反转。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLTRISTAR, TRISTAR, inputs)

    @staticmethod
    def unique_three_river(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Unique 3 River 奇特三河床
        简介：三日K线模式，下跌趋势中，第一日长阴线，第二日为锤头，最低价创新低，第三日开盘价低于第二日收盘价，收阳线， 收盘价不高于第二日收盘价，预示着反转，第二日下影线越长可能性越大。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLUNIQUE3RIVER, UNIQUE3RIVER, inputs)

    @staticmethod
    def upside_gap_two_crows(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Upside Gap Two Crows 向上跳空的两只乌鸦
        简介：三日K线模式，第一日阳线，第二日跳空以高于第一日最高价开盘， 收阴线，第三日开盘价高于第二日，收阴线，与第一日比仍有缺口。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLUPSIDEGAP2CROWS, UPSIDEGAP2CROWS, inputs)

    @staticmethod
    def xside_gap_three_methods(inputs: Union[DataFrame, dict[str, ndarray]]) -> Series:
        """
        名称：Upside/Downside Gap Three Methods 上升/下降跳空三法
        简介：五日K线模式，以上升跳空三法为例，上涨趋势中，第一日长阳线，第二日短阳线，第三日跳空阳线，第四日阴线，开盘价与收盘价于前两日实体内， 第五日长阳线，收盘价高于第一日收盘价，预示股价上升。

        :param inputs:      prices: ['open', 'high', 'low', 'close']
        :return:            integer (values are -100, 0 or 100)
        """
        return call_talib_cdl(CDLXSIDEGAP3METHODS, XSIDEGAP3METHODS, inputs)
