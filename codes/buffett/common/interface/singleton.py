from buffett.constants.magic import get_class


class Singleton:
    """
    所有单例的基类
    subcls继承cls后，subcls的类属性仍存在cls的__dict__中
    因此，Singleton中需要创建一个字典来存放这些实例

    """
    _instance = {}

    def __new__(cls, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        """
        if cls not in cls._instance:
            return super(Singleton, cls).__new__(cls)
        return cls._instance[cls]

    def __init__(self):
        cls = get_class(self)
        cls._instance[cls] = self
