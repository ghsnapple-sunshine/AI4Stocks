from json import loads, JSONDecodeError
from random import Random
from typing import Optional

from random_user_agent.user_agent import UserAgent
from requests import Response, Session as req_Session, get as req_get
from requests.adapters import HTTPAdapter as req_HTTPAdapter, ProxyError

from buffett.adapter.numpy import np
from buffett.adapter.pandas import DataFrame, pd
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.magic import get_class_name
from buffett.common.tools import dataframe_is_valid

ADDRESS = "address"
HOST = "host"
IP = "ip"
PORT = "port"
DB_POOL = "pool"
TAG = "tag"


class Requests:
    _s = req_Session()
    _s.mount("http://", req_HTTPAdapter(max_retries=3))
    _s.mount("https://", req_HTTPAdapter(max_retries=3))

    @classmethod
    def get(
        cls,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        proxies: Optional[dict] = None,
        verify: bool = True,
        timeout: int = 60,
    ) -> Response:
        """
        get方式爬取url

        :param url:         链接
        :param params:      参数
        :param headers:     头
        :param proxies:     代理
        :param verify:      是否验证
        :param timeout:     超时时长
        :return:
        """
        return cls._s.get(
            url,
            params=params,
            headers=headers,
            verify=verify,
            proxies=proxies,
            timeout=timeout,
        )


class ProxyRequestsLogger(Logger):
    @classmethod
    def info_get_start(cls, source: str):
        cls.info(f"Get proxy from {source} start.")

    @classmethod
    def info_get_success(cls, source: str):
        cls.info(f"Get proxy from {source} success.")

    @classmethod
    def info_get_fail_with_error(cls, source: Optional[str], err: Exception):
        cls.info(f"Get proxy from {source} with {get_class_name(err)}")

    @classmethod
    def info_save_success(cls):
        cls.info(f"Save proxy success.")

    @classmethod
    def info_get_fail_with_code(cls, source: str, code: int):
        cls.info(f"Get proxy from {source} with code {code}.")

    @classmethod
    def info_test_success(cls, proxy: str):
        cls.info(f"Test {proxy} success.")

    @classmethod
    def info_test_fail_with_error(cls, proxy: str, err: Exception):
        cls.info(f"Test {proxy} failed with {get_class_name(err)}.")

    @classmethod
    def info_test_fail_with_code(cls, proxy: str, code: int):
        cls.info(f"Test {proxy} failed with code {code}.")


class WeightedRandom:
    INIT = 1024

    def __init__(self, items: list[str]):
        self._items = np.array(items)
        self._num = len(items)
        self._order = dict((v, k) for k, v in enumerate(items))
        self._weights = np.array([1] * self._num) * self.INIT
        self._w_sums = np.concatenate(
            [np.arange(1, self._num + 1) * self.INIT, np.arange(self._num)]
        ).reshape((self._num, 2), order="F")
        self._random = Random()

    def random_item(self):
        """
        随机获取一个对象

        :return:
        """
        total = sum(self._weights)
        rand = self._random.random() * total
        order = self._w_sums[self._w_sums[:, 0] >= rand][0, 1]
        return self._items[order]

    def get_weight(self, item: str):
        """
        获取weight

        :param item:
        :return:
        """
        order = self._order[item]
        return self._weights[order]

    def set_weight(self, item: str, value: int):
        """
        设置weight

        :param item:
        :param value:
        :return:
        """
        order = self._order[item]
        diff = value - self._weights[order]
        self._weights[order] = value
        self._w_sums[order:, 0] = self._w_sums[order:, 0] + diff

    def reset_weight(self, item: str):
        """
        重置weight

        :param item:
        :return:
        """
        return self.set_weight(item, self.INIT)


class ProxyRequests:
    _ua = UserAgent()
    _logger = LoggerBuilder.build(ProxyRequestsLogger)()
    _proxies = None
    _weights = None

    @classmethod
    def get_from_proxylist(cls):
        """
        下载proxylist

        :return:
        """
        SOURCE = "proxylist.fatezero.org"
        # 下载
        cls._logger.info_get_start(SOURCE)
        req = cls.get(url="http://proxylist.fatezero.org/proxy.list")
        if req.status_code != 200:
            cls._logger.info_get_fail(SOURCE, req.status_code)
            return
        # 处理
        lines = req.text.split("\n")
        dfs = []
        for line in lines:
            try:
                df = DataFrame(loads(line))
                dfs.append(df)
            except JSONDecodeError:
                continue
        dfs = pd.concat_safe(dfs).drop_duplicates()
        dfs[ADDRESS] = dfs.apply(lambda x: f"{x[HOST]}:{x[PORT]}", axis=1)
        # 验证和保存
        cls.combine(dfs[[ADDRESS]])

    @classmethod
    def get_from_kuaidaili(cls):
        """
        下载kuaidaili

        :return:
        """

        def save_dfs(dfs_: list[DataFrame]):
            # 拼接
            dfs_ = pd.concat_safe(dfs_).drop_duplicates()
            dfs_.columns = [x.lower() for x in dfs_.columns]
            dfs_[ADDRESS] = dfs_.apply(lambda x: f"{x[IP]}:{x[PORT]}", axis=1)
            # 验证和保存
            cls.combine(dfs_[[ADDRESS]])

        SOURCE = "www.kuaidaili.com"
        cls._logger.info_get_start(SOURCE)
        cls._load()
        # 下载
        i = 1
        dfs = []
        while True:
            # 如果获取失败则尝试更换代理
            url = f"https://www.kuaidaili.com/free/inha/{i}/"
            req = cls.get(url=url, proxy="random")
            if req is None:
                continue
            if req.status_code == 404:
                break
            elif req.status_code != 200:
                cls._logger.info_get_fail_with_code(url, req.status_code)
            try:
                df = pd.read_html(req.text)
                dfs.extend(df)
                cls._logger.info_get_success(url)
                if i % 20 == 0:
                    save_dfs(dfs)
                    dfs = []
                i += 1
            except ValueError as e:
                cls._logger.info_get_fail_with_error(url, e)
                pass
        save_dfs(dfs)

    @classmethod
    def combine(cls, proxies: DataFrame) -> None:
        """
        将现有proxy与新获proxy进行组合并保存

        :param proxies:
        :return:
        """
        cls._load()
        if dataframe_is_valid(cls._proxies):
            all_proxies = pd.concat([cls._proxies, proxies]).drop_duplicates()
            add_proxies = pd.subtract(all_proxies, cls._proxies)
            add_proxies = cls.verify(proxies=add_proxies)  # 增量验证
            all_proxies = pd.concat([cls._proxies, add_proxies]).reset_index(drop=True)
        else:
            all_proxies = proxies.drop_duplicates()
            all_proxies = cls.verify(proxies=all_proxies)
        cls._proxies = all_proxies
        cls._proxies.to_feather(DB_POOL)
        cls._logger.info_save_success()

    @classmethod
    def _load(cls, force: bool = False) -> None:
        """
        加载保存的proxy

        :param force:       强制加载
        :return:
        """
        if not force and cls._proxies is not None:
            cls._weights = WeightedRandom(cls._proxies[ADDRESS].tolist())
            return
        try:
            cls._proxies = pd.read_feather(DB_POOL)
            cls._weights = WeightedRandom(cls._proxies[ADDRESS].tolist())
        except IOError:
            pass

    @classmethod
    def get(
        cls,
        url: str,
        proxy: Optional[str] = None,
        timeout: int = 5,
        max_retries: int = 100,
        **kwargs,
    ) -> Optional[Response]:
        """

        :param url
        :param proxy:           None: 不使用proxy; random: 随机proxy; XXX.XXX.XXX.XXX:XXXX 具体proxy
        :param timeout:
        :param max_retries:     设置以避免死循环
        :return:
        """
        headers = {"User-Agent": cls._ua.get_random_user_agent()}
        item = None
        if proxy is None:
            proxies = None
        else:
            item = cls._weights.random_item() if proxy == "random" else proxy
            proxies = {"http": item, "https": item}
        #
        response = None
        punish = 4
        for i in range(max_retries):
            try:
                response = req_get(
                    url, headers=headers, proxies=proxies, timeout=timeout, **kwargs
                )
                break
            except ConnectionError as e:  # Sometimes the header is invalid...
                cls._logger.info_get_fail_with_error(url, e)
                continue
            except ProxyError as e:
                punish = 8
                cls._logger.info_get_fail_with_error(url, e)
                break
            except Exception as e:
                cls._logger.info_get_fail_with_error(url, e)
                break
        # 更新权重
        if proxy == "random":
            if response is None:
                cls._weights.set_weight(item, cls._weights.get_weight(item) / punish)
            else:
                cls._weights.reset_weight(item)
        return response

    @classmethod
    def verify(
        cls,
        url: str = "https://www.baidu.com/",
        proxies: Optional[DataFrame] = None,
    ) -> DataFrame:
        """
        通过百度验证，通过验证的IP才会被保存

        :param url:
        :param proxies:
        :return:
        """

        def verify_one(proxy: str) -> bool:
            r = req_get(url, proxies={"http": proxy, "https:": proxy}, timeout=5)
            if r.status_code == 200:
                cls._logger.info_test_success(proxy)
                return True
            else:
                cls._logger.info_test_fail_with_code(proxy, r.status_code)
                return False

        # 如未指定验证对象，则验证已保存的代理
        if proxies is None:
            cls._load()
            proxies = cls._proxies
        if proxies.empty:
            return DataFrame()
        #
        proxies = proxies[ADDRESS]
        tags = [verify_one(x) for x in proxies.values]
        filter_proxies = DataFrame(data={ADDRESS: proxies, TAG: tags})
        filter_proxies = filter_proxies[filter_proxies[TAG]]
        filter_proxies = filter_proxies[[ADDRESS]].reset_index(drop=True)
        return filter_proxies


if __name__ == "__main__":
    while True:
        print("1. add    ips to pool:")
        print("2. verify ips in pool:")
        print("3. exit              :")
        choice = input("\ninput your choice: ")
        if choice == "1":
            ProxyRequests.get_from_proxylist()
            ProxyRequests.get_from_kuaidaili()
        elif choice == "2":
            # ProxyRequests.verify("https://d.10jqka.com.cn/v6/line/hs_000001/02/all.js")
            ProxyRequests.verify("https://www.kuaidaili.com/free/inha/3/")
            # ProxyRequests.verify()
        elif choice == "3":
            break
        else:
            continue
