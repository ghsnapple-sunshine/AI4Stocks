"""
class TestSerialize(Tester):
    
    def test_daily_task(self):
        task = StockDailyTask(operator=self.operator,
                              start_time=DateTime.now())
        bts = SerializeTool.magic(task)
        task2 = SerializeTool.deserialize(bts)
"""
