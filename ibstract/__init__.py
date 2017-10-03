import sys

if sys.version_info < (3, 6, 0):
    raise RuntimeError("ibstract requires Python 3.6.0 or higher")

try:
    import ibapi
except ImportError:
    print('Interactive Brokers API >= 9.73 is required')
    sys.exit()

ibver = tuple(ibapi.VERSION.values())
if ibver < (9, 73, 2):
    print("\nInteractive Brokers API version: %s installed."
          "\nInteractive Brokers API Version >= 9.73.2 required."
          % ibapi.get_version_string())


from .brokers import *
from .marketdata import *
from .financedata import *
from .trading import *
from .ibglobals import *
from . import utils

__version__ = '1.0.0a2'

__all__ = ['utils']
for _m in (brokers, marketdata, financedata, trading, ibglobals):
    __all__ += _m.__all__
