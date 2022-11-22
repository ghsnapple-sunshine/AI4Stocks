from abc import abstractmethod

from buffett.download.mysql import Operator
from buffett.download.para import Para


class Handler:
    def __init__(self, operator: Operator):
        self._operator = operator

    @abstractmethod
    def obtain_data(self, *args, **kwargs):
        pass

    @abstractmethod
    def select_data(self, *args, **kwarg):
        pass
