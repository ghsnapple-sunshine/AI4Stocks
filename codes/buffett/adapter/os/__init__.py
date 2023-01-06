from os import (
    system as os_system,
    makedirs as os_makedirs,
    path as os_path,
    getpid as os_getpid,
)


class os:
    getpid = os_getpid
    makedirs = os_makedirs
    path = os_path
    system = os_system
