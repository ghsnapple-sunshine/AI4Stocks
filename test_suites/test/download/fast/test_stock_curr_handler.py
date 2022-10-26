'''
class TestStockCurrHandler(BaseTest):
    def test_run(self):
        hdl = StockCurrHandler(op=self.op)
        tbl = hdl.download_and_save()
        print(tbl)
'''