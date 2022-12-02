from typing import Optional

from buffett.adapter.wellknown import tqdm
from buffett.common.error import ParamTypeError
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType


def cleanup(like: Optional[str] = None) -> None:
    c1 = like is None
    c2 = isinstance(like, str)
    if not c1 and not c2:
        raise ParamTypeError("like", Optional[str])

    op = Operator(RoleType.ROOT)
    tbls = op.execute("show tables", fetch=True)
    tbls = tbls[tbls.columns[0]]
    to_delete_tbls = tbls[tbls.apply(lambda x: like in x)] if c2 else tbls
    if len(to_delete_tbls) == 0:
        print("没有表格待删除。")
        return

    print("待删除的表格为：")
    [print(f"{x}") for x in to_delete_tbls]
    confirm = input("确认删除这些表格？删除后不可恢复。（y/n）")
    if confirm == "y":
        with tqdm(total=len(to_delete_tbls)) as pbar:
            for table_name in to_delete_tbls:
                op.drop_table(name=table_name)
                pbar.update(1)
        print("删除完毕。")
    else:
        print("跳过删除。")
