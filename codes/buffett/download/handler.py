from abc import abstractmethod

from buffett.download.mysql import Operator
from buffett.download.para import Para


class Handler:
    def __init__(self, operator: Operator):
        self._operator = operator

    @abstractmethod
    def obtain_data(self, para: Para):
        pass

    @abstractmethod
    def get_data(self, para: Para):
        pass
