import unittest
from pandas import DataFrame
from codes.data_connect.mysql_common import MysqlColumnType, MysqlConstants, MysqlRole, MysqlColumnAddReq
from codes.data_connect.mysql_operator import MysqlOperator


class TestMysqlOperator(unittest.TestCase):
    def test_all(self):
        op = TestMysqlOperator.createTable()
        TestMysqlOperator.insertData(op)
        TestMysqlOperator.tryInsertData(op)

    def tearDown(self):
        op = MysqlOperator(MysqlRole.DbTest)
        op.DropTable(MysqlConstants.STOCK_LIST_TABLE)

    @staticmethod
    def createTable():
        data = [['code', MysqlColumnType.Varchar8, MysqlColumnAddReq.PRIMKEY],
                ['name', MysqlColumnType.Varchar4, MysqlColumnAddReq.NONE]]
        df = DataFrame(data=data, columns=MysqlConstants.COLUMN_INDEXS)
        op = MysqlOperator(MysqlRole.DbTest)
        op.CreateTable(MysqlConstants.STOCK_LIST_TABLE, df, False)
        return op

    @staticmethod
    def insertData(op: MysqlOperator):
        data = [['sz000001', '平安银行'],
                ['sh600000', '浦发银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        op.InsertData(MysqlConstants.STOCK_LIST_TABLE, df)

    @staticmethod
    def tryInsertData(op: MysqlOperator):
        data = [['sz000001', '狗狗银行']]
        df = DataFrame(data=data, columns=['code', 'name'])
        op.TryInsertData(MysqlConstants.STOCK_LIST_TABLE, df) # 不更新数据
        tbl = op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        for row in range(len(tbl)):
            if tbl[row, 0] == 'sz000001':
                assert tbl[row, 1] == '平安银行'

        op.TryInsertData(MysqlConstants.STOCK_LIST_TABLE, df, update=True)  # 更新数据
        tbl = op.GetTable(MysqlConstants.STOCK_LIST_TABLE)
        for row in range(len(tbl)):
            if tbl[row, 0] == 'sz000001':
                assert tbl[row, 1] == '狗狗银行'

if __name__ == '__main__':
    unittest.main()
