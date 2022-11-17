import subprocess
from multiprocessing import Process


def func():
    # os.system("python ./test2.py")
    # os.system("gnome-terminal -e 'bash -c \"ls; exec bash\"'")
    # subprocess.call_in_new_window('python ./test2.py', shell=True)
    # subprocess.call(['gnome-terminal', '-x', 'python ./test2.py'])
    subprocess.call("python ./test3.py", creationflags=subprocess.CREATE_NEW_CONSOLE)


if __name__ == "__main__":
    # t = Thread(target=func)
    # t.start()
    p = Process(target=func)
    p.start()
