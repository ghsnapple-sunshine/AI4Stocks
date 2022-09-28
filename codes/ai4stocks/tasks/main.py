from pendulum import DateTime, Duration

from ai4stocks.download.akshare.stock_list_handler import StockListHandler
from ai4stocks.download.akshare.stock_daily_handler import StockDailyHandler
from ai4stocks.download.baostock.stock_minute_handler import StockMinuteHandler
from ai4stocks.download.connect.mysql_common import MysqlRole
from ai4stocks.download.connect.mysql_operator import MysqlOperator

if __name__ == '__main__':
    op = MysqlOperator(MysqlRole.DbStock)
    end_date = DateTime.now()
    end_date = DateTime(year=end_date.year, month=end_date.month, day=end_date.day,
                        hour=end_date.hour, minute=end_date.minute) - Duration(days=1)
    start_date = DateTime(2000, 1, 1)
    StockListHandler(op=op).DownloadAndSave()
    StockDailyHandler(op=op).DownloadAndSave(start_date=start_date, end_date=end_date)
    StockMinuteHandler(op=op).DownloadAndSave(start_date=start_date, end_date=end_date)
