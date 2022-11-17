from buffett.adapter.os import system
from buffett.adapter.pendulum import datetime
from buffett.adapter.threading import Thread


def execCmd(cmd):
    try:
        print("命令%s开始运行%s" % (cmd, datetime.now()))
        system(cmd)
        print("命令%s结束运行%s" % (cmd, datetime.now()))
    except:
        print("%s\t 运行失败" % (cmd,))


if __name__ == "__main__":
    # 是否需要并行运行
    if_parallel = True

    # 需要执行的命令列表
    model_list = ["yolo", "centernet"]
    cmds = ["python main.py --model " + i for i in model_list]

    if if_parallel:
        # 并行
        threads = []
        for cmd in cmds:
            th = Thread(target=execCmd, args=(cmd,))
            th.start()
            threads.append(th)

        # 等待线程运行完毕
        for th in threads:
            th.join()
    else:
        # 串行
        for cmd in cmds:
            try:
                print("命令%s开始运行%s" % (cmd, datetime.now()))
                system(cmd)
                print("命令%s结束运行%s" % (cmd, datetime.now()))
            except:
                print("%s\t 运行失败" % (cmd,))
