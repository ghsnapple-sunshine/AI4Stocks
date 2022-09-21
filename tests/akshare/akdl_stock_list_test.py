import unittest

from codes.akshare.akdl_stock_list import AkDlStockList
from codes.data_connect.mysql_common import MysqlRole, MysqlConstants
from codes.data_connect.mysql_operator import MysqlOperator


class AkshareDlStockListTest(unittest.TestCase):
    def test_all(self):
        res = AkDlStockList.DownloadAndSave(MysqlRole.DbTest)
        op = MysqlOperator(MysqlRole.DbTest)
        cnt = op.GetTableCnt(MysqlConstants.STOCK_LIST_TABLE)
        assert cnt == res
        tbl = op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        assert type(tbl) == list

if __name__ == '__main__':
    unittest.main()
