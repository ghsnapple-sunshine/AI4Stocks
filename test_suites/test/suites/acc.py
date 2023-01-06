from typing import Callable

from buffett.adapter.os import os
from buffett.adapter.pandas import pd, DataFrame
from buffett.adapter.pickle import load, dump
from buffett.common.error import AttrTypeError
from buffett.common.logger import Logger
from buffett.common.magic import get_self, get_name, get_class
from buffett.common.magic.tools import get_func_params, get_func_full_name
from buffett.common.pendulum import Date
from buffett.common.tools import dataframe_is_valid

ROOT_PATH = "D:/BuffettData/testcase_cache/"
FOLDER_PATH = f"{ROOT_PATH}%s/"
INDEX_PATH = f"{ROOT_PATH}%s/index"
FILE_PATH = f"{ROOT_PATH}%s/%s"


class Accelerator:
    """
    Accelerator可以缓存func的运行结果，当(func, *args, **kwargs)检查结果一致时，

    """

    _cache = None
    _cls_inited = False
    _file_num = 0
    _mocked_func = None
    _folder_path = None
    _index_path = None
    _file_path = None

    def __init__(self, func: Callable):
        if not callable(func):
            raise AttrTypeError("func")
        restore = self.restore(func)
        self._func = func if restore is None else restore
        if not Accelerator._cls_inited:
            self._cls_init()

    def mock(self):
        """
        得到一个Accelerator的mock方法

        :return:
        """
        return self._calc_n_save

    @classmethod
    def _cls_init(cls):
        """
        初始化类的一致性对象，包括缓存、缓存路径等

        :return:
        """
        cls._cls_inited = True
        cls._mocked_func = get_name(cls._calc_n_save)  # Robust when function renaming
        today = Date.today().format("YYYYMMDD")
        cls._folder_path = FOLDER_PATH % (today,)
        cls._index_path = INDEX_PATH % (today,)
        cls._file_path = FILE_PATH % (today, "%s")
        if os.path.isfile(cls._index_path):  # index file exists
            with open(cls._index_path, "rb") as index_file:
                cls._cache = load(file=index_file)
            cls._file_num = sum([x[1] is not None for x in cls._cache.values()])
            return
        # if _cache is not successfully loaded...
        elif not os.path.isdir(Accelerator._folder_path):  # folder file not exists
            # rmtree(ROOT_PATH)
            os.makedirs(Accelerator._folder_path)
        Accelerator._cache = {}  # assign it to empty if not loaded

    @staticmethod
    def restore(func):
        """
        从一个Accelerator的mock方法还原得到本来的方法
        如果还原失败，返回None

        :param func:
        :return:
        """
        caller = get_self(func)
        if (
            isinstance(caller, Accelerator)
            and get_name(func) == Accelerator._mocked_func
        ):  # 避免套娃
            # if Accelerator._mocked_func == get_func_full_name(func):  # 避免套娃
            return caller._func
        return None

    @staticmethod
    def safe_io(func: Callable, *args, **kwargs) -> None:
        try:
            func(*args, **kwargs)
        except IOError as e:
            Logger.error(e)

    def _calc_n_save(self, *args, **kwargs):
        """
        根据传入的参数计算（或从换从中提取），并返回被包装函数的值

        :param args:
        :param kwargs:
        :return:
        """

        # 1. get signature
        si = self._get_signature(args, kwargs)
        # 2. if already loaded(or computed)
        if si in Accelerator._cache:
            obj, file_path = Accelerator._cache[si][0], Accelerator._cache[si][1]
            # 2.1 a type object indicated it has not been loaded(or computed)
            if isinstance(obj, type):
                if obj == DataFrame:
                    if file_path is not None:
                        obj = pd.read_feather(path=file_path)  # read from feather
                        Accelerator._cache[si][0] = obj
                    else:
                        obj = DataFrame()
                        Accelerator._cache[si][0] = obj
                # 2.1.2 Other(a type) indicates it need to be loaded.(Not defined)
                else:
                    Logger.warning(f"Unsupported format {obj} to save.")
            # 2.2 an instance object indicates it has been loaded(or computed)
            return obj
        # 3. if not loaded
        else:
            # 3.1 run the func
            result = self._func(*args, **kwargs)
            # 3.2 save to cache
            Accelerator._cache[si] = [result, None]
            # 3.3 save to disk
            # 3.3.1 save DataFrame to disk
            if isinstance(result, DataFrame):
                if dataframe_is_valid(result):
                    file_path = Accelerator._file_path % (Accelerator._file_num,)
                    result.to_feather(path=file_path)  # save to feather
                    Accelerator._cache[si][1] = file_path  # else path is still None
                    Accelerator._file_num += 1
                # Nevertheless dataframe is valid or not, it'll be recorded.
                dic = dict(
                    filter(
                        lambda x: (x[1][0] is not None) or (x[1][1] is not None),
                        Accelerator._cache.items(),
                    )
                )
                # do reform as follows:
                # key, [data, path]       --> key, [type(data), path]
                # key, [type(data), path] --> key, [type(data), path]
                dic = dict(
                    (k, [v[0] if isinstance(v[0], type) else get_class(v[0]), v[1]])
                    for k, v in dic.items()
                )
                with open(Accelerator._index_path, "wb") as index_file:
                    dump(dic, file=index_file)
            # 3.3.2 save other(type) to disk(not defined)
            else:
                Logger.warning(f"Unsupported format {type(result)} to save.")
            return result

    def _get_signature(self, args: tuple, kwargs: dict) -> tuple[str, ...]:
        params = get_func_params(self._func)
        for i in range(0, len(args)):
            params[i][1] = args[i]
        for i in range(len(args), len(params)):
            if params[i][0] in kwargs:
                params[i][1] = kwargs[params[i][0]]
        signature = [get_func_full_name(self._func)]
        signature.extend([x[1] for x in params])
        signature = tuple(signature)
        return signature

    def __str__(self):
        return f"Accelerator of {get_func_full_name(self._func)}"
