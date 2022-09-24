from pendulum import DateTime

from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.akshare.stock_daily_handler import StockDailyHandler
from ai4stocks.download.baostock.stock_minute_handler import StockMinuteHandler
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator

if __name__ == '__main__':
    op = MysqlOperator(MysqlRole.DbStock)
    start_date = DateTime(2000, 1, 1)
    end_date = DateTime.now()
    StockListHandler.DownloadAndSave(op=op)
    StockDailyHandler.DownloadAndSave(op=op)
    StockMinuteHandler.DownloadStockMinuteInfos(op=op)
