from abc import abstractmethod

from ai4stocks.download.mysql import Operator
from ai4stocks.download.para import Para


class Handler:
    def __init__(self, operator: Operator):
        self._operator = operator

    @abstractmethod
    def obtain_data(self, para: Para):
        pass

    @abstractmethod
    def get_data(self, para: Para):
        pass
