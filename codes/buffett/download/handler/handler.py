from abc import abstractmethod

from buffett.download.mysql import Operator


class Handler:
    def __init__(self, operator: Operator, *args, **kwargs):
        self._operator = operator

    @abstractmethod
    def obtain_data(self, *args, **kwargs):
        pass

    @abstractmethod
    def select_data(self, *args, **kwargs):
        pass
