"""
1、计算除息价:
除息价=股息登记日的收盘价-每股所分红利现金额

2、计算除权价:
送红股后的除权价=股权登记日的收盘价/(1+每股送红股数)
配股后的除权价=(股权登记日的收盘价+配股价*每股配股数)/(1+每股配股数)

3、计算除权除息价

除权除息价=(股权登记日的收盘价-每股所分红利现金额+配股价*每股配股数)/(1+每股送红股数+每股配股数)
“前收盘价”由交易所计算并公布。首发日的“前收盘价”等于“首发价格”

from buffett.adapter.pandas import DataFrame
from buffett.common.constants.col import CLOSE
from buffett.download import Para
from buffett.download.handler.stock import DcDailyHandler
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType
from buffett.download.types import FuquanType

if __name__ == "__main__":
    operator = Operator(RoleType.DbStock)
    daily_hdl = DcDailyHandler(operator=operator)
    para = Para().with_code("000001")
    bfq_data = DataFrame(
        {
            "close_bfq": (
                daily_hdl.select_data(para=para.with_fuquan(FuquanType.BFQ))[CLOSE]
            )
        }
    )
    hfq_data = DataFrame(
        {
            "close_hfq": (
                daily_hdl.select_data(para=para.with_fuquan(FuquanType.HFQ))[CLOSE]
            )
        }
    )
    qfq_data = DataFrame(
        {
            "close_qfq": (
                daily_hdl.select_data(para=para.with_fuquan(FuquanType.QFQ))[CLOSE]
            )
        }
    )
    merge_data = bfq_data.join(hfq_data, how="left")
    merge_data = merge_data.join(qfq_data, how="left")
    merge_data['ratio_hfq'] = merge_data['close_hfq'] / merge_data['close_bfq']
    merge_data['ratio_qfq'] = merge_data['close_qfq'] / merge_data['close_bfq']
    print(merge_data)
"""
