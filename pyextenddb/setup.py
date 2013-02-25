"""
setup.py file for SWIG example
"""

from distutils.core import setup, Extension


setup (name = 'extenddbint',
       version = '0.1',
       author      = "Sridhar Valaguru",
       description = """Python bindings for extenddb.""",
       #ext_modules = ["_extenddbint.so"],
       py_modules = ["extenddbint.py","extenddb.py"],
       )
