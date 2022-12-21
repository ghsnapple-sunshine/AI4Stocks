from buffett.common import ComparableEnum


class FuquanType(ComparableEnum):
    """
    Fuquan类型
    baostock adjustFlag中后复权为1，前复权为2，不复权为3
    akshare adjust使用字符串
    """

    BFQ = 3
    QFQ = 2
    HFQ = 1

    def ak_format(self) -> str:
        FUQUAN_TYPE_DICT = {
            FuquanType.BFQ: "",
            FuquanType.QFQ: "qfq",
            FuquanType.HFQ: "hfq",
        }
        return FUQUAN_TYPE_DICT[self]

    def bs_format(self) -> str:
        return str(self.value)

    def ths_format(self) -> str:
        FUQUAN_TYPE_DICT = {
            FuquanType.BFQ: "00",
            FuquanType.QFQ: "01",
            FuquanType.HFQ: "02",
        }
        return FUQUAN_TYPE_DICT[self]

    def __str__(self):
        FUQIAN_TYPE_DICT = {
            FuquanType.BFQ: "bfq",
            FuquanType.QFQ: "qfq",
            FuquanType.HFQ: "hfq",
        }
        return FUQIAN_TYPE_DICT[self]
