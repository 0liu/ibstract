IBstract
========

IBstract is a high-level Pythonic interface to Interactive Brokers API,
wrapping historical and real-time market data access, as well as orders and
account management. The ultimate goal of Ibstract is to provide an easy-to-use
Pythonic IB interface for individual algorithmic back-testing and trading.


Features
----------
- Read and write historical and market data in pandas formats.
- Automatically merge and manage downloaded data by symbols, bar size and date/time.
- Store and cache historical data locally using sqlite.
- Real-time market data streaming and common technical signal generating. (To do)
- Order submission, status monitoring, and logging. (To do)
- Account management report and performance analysis. (To do)


Requirements
------------
- Python >= 3.6 (Recommend Anaconda 4.4.0)
- pandas >= 0.20.0
- sqlite >= 3.13
- SQLAlchemy >= 1.1.9


References
----------
- [Rob Carver's Blog](http://qoppac.blogspot.co.uk/2017/03/interactive-brokers-native-python-api.html)
- [Interactive Brokers TWS API v9.72+](http://interactivebrokers.github.io/tws-api/)
