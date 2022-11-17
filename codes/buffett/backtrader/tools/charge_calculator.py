class ChargeCalculator:
    def __init__(
        self,
        yinhuashuilv: float = 0.001,  # 1‰
        yongjinlv: float = 0.00003,  # 0.03%（万三）
        zuidiyongjin: float = 5,  # 5
        guohufeilv: float = 0.000002,
    ):  # 0.002%（万零点二）
        self.__yinhuashuilv = yinhuashuilv
        self.__yongjinlv = yongjinlv
        self.__zuidiyongjin = zuidiyongjin
        self.__guohufeilv = guohufeilv

    # 成交额的单位是【分】
    def run(self, cje: int) -> int:
        if cje == 0:
            return 0
        elif cje > 0:  # 买入
            yongjin = max(cje * self.__yongjinlv, self.__zuidiyongjin * 100)
            guohufei = cje * self.__guohufeilv
            shui = int(round(yongjin + guohufei, 0))
            return shui
        elif cje < 0:
            yongjin = max(-cje * self.__yongjinlv, self.__zuidiyongjin * 100)
            guohufei = -cje * self.__guohufeilv
            yinhuashui = -cje * self.__yinhuashuilv
            shui = int(round(yongjin + guohufei + yinhuashui, 0))
            return shui
