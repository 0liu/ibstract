IBstract
========

IBstract is a high-level Python library wrapping Interactive Brokers API,
providing historical data download, real-time market data streaming, and orders
and account management. The goal of Ibstract is to provide an easy-to-use
Pythonic IB interface for algorithmic trading and back-testing.


Features
----------
- Read and write historical and market data in pandas formats.
- Automatically merge and manage downloaded data by symbols, bar size and date/time.


Requirements
------------
- Interactive Brokers API <= 9.72
- Interactive Brokers Gateway >= 959, <= 963.3h
- Python >= 3.6 (Recommend Anaconda 4.4.0)
- pandas >= 0.20.0


References
----------
- [Rob Carver: Using swigibpy so that Python will play nicely with Interactive Brokers API](https://qoppac.blogspot.com/2014/03/using-swigibpy-so-that-python-will-play.html)
- [Interactive Brokers API References v9.72](http://xavierib.github.io/twsapidocs/)
