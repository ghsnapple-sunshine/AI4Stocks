from json import loads, JSONDecodeError

from buffett.adapter.pandas import DataFrame, pd
from buffett.adapter.request.proxy_req import ProxyRequests
from buffett.common.logger import Logger, LoggerBuilder
from buffett.common.magic import get_class_name

ADDRESS = "address"
HOST = "host"
IP = "ip"
PORT = "port"
PATH = "E:/BuffettData/proxy_pool/"
DB_POOL = "E:/BuffettData/proxy_pool/pool"
TAG = "tag"


def get_from_proxylist():
    """
    下载proxylist

    :return:
    """
    logger = LoggerBuilder.build(ProxyGetterLogger)()
    SOURCE = "proxylist.fatezero.org"
    request = ProxyRequests()
    # 下载
    logger.info_get_proxy_start(SOURCE)
    response = request.get(url="http://proxylist.fatezero.org/proxy.list")
    if response.status_code != 200:
        logger.info_get_fail_with_code(SOURCE, response.status_code)
        return
    # 处理
    lines = response.text.split("\n")
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
    request.update(dfs[[ADDRESS]])


def get_from_kuaidaili():
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
        request.update(dfs_[[ADDRESS]])

    logger = LoggerBuilder.build(ProxyGetterLogger)()
    SOURCE = "www.kuaidaili.com"
    logger.info_get_proxy_start(SOURCE)
    request = ProxyRequests("kuaidaili")
    # 下载
    i = 1
    dfs = []
    while True:
        # 如果获取失败则尝试更换代理
        url = f"https://www.kuaidaili.com/free/inha/{i}/"
        response = request.get(url=url, proxy="random")
        if response is None:
            continue
        if response.status_code == 404:
            break
        elif response.status_code != 200:
            logger.info_get_fail_with_code(url, response.status_code)
        try:
            df = pd.read_html(response.text)
            dfs.extend(df)
            if i % 20 == 0:
                save_dfs(dfs)
                dfs = []
            i += 1
        except ValueError as e:
            logger.info_get_data_fail_with_error(url, e)
            pass
    save_dfs(dfs)


class ProxyGetterLogger(Logger):
    @classmethod
    def info_get_proxy_start(cls, source: str):
        cls.info(f"Get proxy from {source} start.")

    @classmethod
    def info_get_data_fail_with_code(cls, source: str, code: int):
        cls.info(f"Get from {source} {code}")

    @classmethod
    def info_get_data_fail_with_error(cls, source: str, err: Exception):
        cls.info(f"Get from {source} {get_class_name(err)}")
        print(err)


if __name__ == "__main__":
    get_from_kuaidaili()
