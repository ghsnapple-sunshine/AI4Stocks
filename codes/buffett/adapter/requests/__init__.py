from typing import Optional

from requests import Session as req_Session
from requests.adapters import HTTPAdapter as req_HTTPAdapter


class requests:
    _s = req_Session()
    _s.mount('http://', req_HTTPAdapter(max_retries=3))
    _s.mount('https://', req_HTTPAdapter(max_retries=3))

    @classmethod
    def get(cls, url: str, params: Optional[dict] = None, verify: bool = True):
        """
        if params is None:
            return cls._s.get(url, verify=verify, timeout=60)
        else:
            return cls._s.get(url, params=params, verify=verify, timeout=60)
        """
        return cls._s.get(url, params=params, verify=verify, timeout=60)
