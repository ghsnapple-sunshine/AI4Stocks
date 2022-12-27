from buffett.adapter.wellknown import tqdm
from buffett.common.tools import dataframe_not_valid
from buffett.download.mysql import Operator
from buffett.download.mysql.types import RoleType


def get_operator_root():
    """
    由于使用root权限需要输入密码，该方法可以避免在操作其他数据库时（前）需要输入root密码，改善体验。
    :return:
    """
    yield Operator(RoleType.ROOT)


operators = {
    "1": get_operator_root(),
    "2": Operator(RoleType.DbAnaly),
}


def cleanup() -> None:
    db = input("请选择要操作的数据库： 1.stocks, 2.analysis. (1/2)")
    op = operators.get(db)
    if op is None:
        print("输入错误。")
        return
    #
    if db == "1":
        op = next(op)
    tbls = op.execute("show tables", fetch=True)
    if dataframe_not_valid(tbls):
        print("数据库为空")
        return
    tbls = tbls[tbls.columns[0]]
    #
    like = input("请选择要匹配的表名：如'dc_stock_dayinfo':")
    to_delete_tbls = tbls[tbls.apply(lambda x: like in x)] if like != "" else tbls

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


if __name__ == "__main__":
    cleanup()
