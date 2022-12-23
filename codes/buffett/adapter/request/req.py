from typing import Optional

from requests import Response, Session as req_Session
from requests.adapters import HTTPAdapter as req_HTTPAdapter


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
