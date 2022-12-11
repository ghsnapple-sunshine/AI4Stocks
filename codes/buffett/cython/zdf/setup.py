from distutils.core import setup
from distutils.extension import Extension
from pathlib import Path

import numpy as np
from Cython.Build import cythonize

# cythonize：编译源代码为C或C++，返回一个distutils Extension对象列表
ext = [
    Extension(name="node", sources=["node.pyx"]),
    Extension(name="tree", sources=["tree.pyx"]),
    Extension(name="quantile", sources=["quantile.pyx"]),
    Extension(
        name="zdf_stat",
        sources=["zdf_stat.pyx"],
        include_dirs=[str(Path(np.__file__).parent / "core" / "include")],
    ),
]
setup(ext_modules=cythonize(ext, language_level=3))


# cd D:\twinkle-pc-files\documents\AI4Stocks\codes\buffett\cython\zdf_stat
# python setup.py build_ext --inplace
