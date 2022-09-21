import unittest
import numpy
from codes.ak_download.akdl_stock_list import AkDlStockList
from codes.data_connect.mysql_common import MysqlRole, MysqlConstants
from codes.data_connect.mysql_operator import MysqlOperator


class TestAkshareDlStockList(unittest.TestCase):
    def test_all(self):
        res = AkDlStockList.DownloadAndSave(MysqlRole.DbTest)
        op = MysqlOperator(MysqlRole.DbTest)
        cnt = op.GetTableCnt(MysqlConstants.STOCK_LIST_TABLE)
        assert cnt == res
        tbl = op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        assert type(tbl) == numpy.ndarray

    def tearDown(self) -> None:
        op = MysqlOperator(MysqlRole.DbTest)
        op.DropTable(MysqlConstants.STOCK_LIST_TABLE)


if __name__ == '__main__':
    unittest.main()
