from pickle import load, dump
from typing import Optional

from random_user_agent.user_agent import UserAgent
from requests import Response, get as req_get
from requests.adapters import ProxyError

from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.request.wrand import Wrand
from buffett.common.constants import UINT64_MAX_VALUE
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.magic import get_class_name
from buffett.common.tools import dataframe_is_valid

ADDRESS = "address"
PATH = "E:/BuffettData/proxy_pool/"
DB_POOL = "E:/BuffettData/proxy_pool/pool"
TAG = "tag"


class ProxyRequests:
    def __init__(self, file_name: Optional[str] = None):
        self._ua = UserAgent()
        self._logger = LoggerBuilder.build(ProxyRequestsLogger)()
        self._wrand_name = None if file_name is None else PATH + file_name
        self._proxies = None
        self._wrand = None
        self._load()
        self._counter = 50

    def update(self, proxies: DataFrame) -> None:
        """
        将现有proxy与新获proxy进行组合并保存

        :param proxies:
        :return:
        """
        self._load()
        if dataframe_is_valid(self._proxies):
            all_proxies = pd.concat([self._proxies, proxies]).drop_duplicates()
            add_proxies = pd.subtract(all_proxies, self._proxies)
            add_proxies = self.verify(proxies=add_proxies)  # 增量验证
            all_proxies = pd.concat([self._proxies, add_proxies]).reset_index(drop=True)
        else:
            all_proxies = proxies.drop_duplicates()
            all_proxies = self.verify(proxies=all_proxies)
        self._proxies = all_proxies
        self._proxies.to_feather(DB_POOL)
        self._logger.info_save_proxy_success()

    def _load(self) -> None:
        """
        加载保存的proxy和weight

        :return:
        """
        if self._proxies is None:
            try:
                self._proxies = pd.read_feather(DB_POOL)
            except IOError as e:
                print(e)
        #
        if self._wrand_name is not None:
            try:
                with open(self._wrand_name, "rb") as weight_file:
                    self._wrand = load(file=weight_file)
            except IOError as e:
                print(e)
        if self._wrand is None:  # Contains situation when load weights failed and...
            self._wrand = Wrand(self._proxies[ADDRESS].values)

    def _save(self):
        """
        保存weight

        :return:
        """
        if self._wrand is None:
            return
        with open(self._wrand_name, "wb") as weight_file:
            dump(self._wrand, file=weight_file)

    def get(
        self,
        url: str,
        proxy: Optional[str] = None,
        timeout: int = 5,
        max_retries: int = UINT64_MAX_VALUE,
        **kwargs,
    ) -> Response:
        """
        获取链接内容

        :param url
        :param proxy:           None: 不使用proxy; random: 随机proxy; XXX.XXX.XXX.XXX:XXXX 具体proxy
        :param timeout:
        :param max_retries:     设置以避免死循环
        :return:
        """
        if proxy is not None and proxy.lower() == "random":
            return self._get_by_random_proxy(url, timeout, max_retries, **kwargs)

        proxies = None if proxy is None else {"http": proxy, "https": proxy}
        #
        response = None
        for i in range(4):
            try:
                headers = {"User-Agent": self._ua.get_random_user_agent()}
                response = req_get(
                    url, headers=headers, proxies=proxies, timeout=timeout, **kwargs
                )
                break
            except ConnectionError as e:  # Sometimes the header is invalid...
                self._logger.info_get_data_fail_with_error(url, e, proxy=proxy)
                continue
            except Exception as e:
                self._logger.info_get_data_fail_with_error(url, e, proxy=proxy)
                break
        return response

    def _get_by_random_proxy(
        self,
        url: str,
        timeout: int = 5,
        max_retries: int = UINT64_MAX_VALUE,
        **kwargs,
    ) -> Response:
        """
        通过随机代理获取链接内容

        :param url:
        :param timeout:
        :param max_retries:
        :param kwargs:
        :return:
        """
        #
        response = None
        for i in range(max_retries):
            punish = 1
            item = self._wrand.random_item()
            proxies = {"http": item, "https": item}
            try:
                headers = {"User-Agent": self._ua.get_random_user_agent()}
                response = req_get(
                    url, headers=headers, proxies=proxies, timeout=timeout, **kwargs
                )
                if response.status_code == 200:
                    self._logger.info_get_data_success(url, proxy=item)
                    break
                self._logger.info_get_data_fail_with_code(
                    url, response.status_code, proxy=item
                )
                if response.status_code == 404:
                    break
            except ConnectionError as e:  # Sometimes the header is invalid...
                self._logger.info_get_data_fail_with_error(url, e, proxy=item)
                continue
            except ProxyError as e:
                punish = 8
                self._logger.info_get_data_fail_with_error(url, e, proxy=item)
            except Exception as e:
                punish = 4
                self._logger.info_get_data_fail_with_error(url, e, proxy=item)
            # 更新权重
            if response is not None:
                self._wrand.reset_weight(item)
            elif punish > 1:
                self._wrand.set_weight(item, self._wrand.get_weight(item) / punish)
            # 保存weights
            self._counter -= 1
            if self._counter == 0:
                self._save()
                self._counter = 50
        return response

    def verify(
        self,
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
                self._logger.info_test_success(proxy)
                return True
            else:
                self._logger.info_test_fail_with_code(proxy, r.status_code)
                return False

        # 如未指定验证对象，则验证已保存的代理
        if proxies is None:
            self._load()
            proxies = self._proxies
        if proxies.empty:
            return DataFrame()
        #
        proxies = proxies[ADDRESS]
        tags = [verify_one(x) for x in proxies.values]
        filter_proxies = DataFrame(data={ADDRESS: proxies, TAG: tags})
        filter_proxies = filter_proxies[filter_proxies[TAG]]
        filter_proxies = filter_proxies[[ADDRESS]].reset_index(drop=True)
        return filter_proxies


class ProxyRequestsLogger(Logger):
    @classmethod
    def info_get_data_success(cls, source: str, proxy: str = ""):
        proxy = "" if proxy == "" else f" with {proxy}"
        cls.info(f"Get from {source}{proxy} success.")

    @classmethod
    def info_get_data_fail_with_error(
        cls, source: str, err: Exception, proxy: str = ""
    ):
        proxy = "" if proxy == "" else f" with {proxy}"
        cls.info(f"Get from {source}{proxy} {get_class_name(err)}")

    @classmethod
    def info_get_data_fail_with_code(cls, source: str, code: int, proxy: str = ""):
        proxy = "" if proxy == "" else f" with {proxy}"
        cls.info(f"Get from {source}{proxy} code:{code}.")

    @classmethod
    def info_test_success(cls, proxy: str):
        cls.info(f"Test {proxy} success.")

    @classmethod
    def info_test_fail_with_error(cls, proxy: str, err: Exception):
        cls.info(f"Test {proxy} failed with {get_class_name(err)}.")

    @classmethod
    def info_test_fail_with_code(cls, proxy: str, code: int):
        cls.info(f"Test {proxy} failed with code {code}.")

    @classmethod
    def info_save_proxy_success(cls):
        cls.info("Save proxy success.")
