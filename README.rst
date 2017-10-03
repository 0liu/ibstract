ibstract
========

|Pyversion| |PyPIVersion| |Status| |License|

**ibstract** is a Python 3 package for trading data acquiring and
management. Thanks to Python's asyncio_ library, it can accesses `Interactive
Brokers API`_ for concurrent remote data downloading, and a MySQL database as
local cache for concurrent data archiving and offline query. Classes in the
package also combine, transform, and maintain trading data, and provide
organized and aggregated data or signals for algorithmic trading. **ibstract**
users can focus on trading algorithms without worrying about the hassels of
handling a broker API or the tedious and error-prone trading data management.


Features
--------
* Concurrent data acquiring and processing with asynchronous access to remote
  IB API server and local MySQL database, powered by ``async/await`` syntax of
  `asyncio`_ module in Python_ 3.6+ and 3rd-party `aio-libs`_.
* Automatically analyze and split a user's historical data request, and
  dispatch data acquiring tasks to local MySQL database (preferred) or remote
  IB API server. In this way much downloading efforts could be saved for
  repeating requests for the same data pieces.
* MarketDataBlock class manages and merges historical data pieces with
  different symbols, types, durations and date/time in an organized and
  standardized way. Data time zone is region-based using pytz, and
  automatically converted and maintained.


Planned Features:
^^^^^^^^^^^^^^^^^
* Asynchronously generating technical signals from user-specified historical data.
* Concurrent real-time market data streaming and real-time trading signal generating.
* Background order submission, status monitoring, and logging.


Installation
------------

::

    pip3 install -U ibstract

Requirements
^^^^^^^^^^^^
* Python_ 3.6+ (Anaconda_ 4.4.0+)
* `Interactive Brokers API`_ 9.73.2+
* `IB gateway latest`_ 967+
* `ib_insync`_ 0.8.5+
* aiomysql_ 0.0.9+
* sqlalchemy_ 1.1.9+
* pandas_ 0.20.1+
* tzlocal_ 1.4+


Documentation
-------------
`Full Documentation <http://rawgit.com/jesseliu0/ibstract/master/docs/html/index.html>`_


Examples
--------
For **full explanation and detailed examples**, please take a look at the example notebooks:

* `Historical data operations example notebook`_
* `MarketDataBlock class example notebook`_
* `IB class example notebook`_

Example 1: Concurrently acquire data from local MySql database and remote IB API server.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A user coroutine requests wider range of historical data than those existing in
MySQL.  The data pieces existing in MySQL will not be downloaded, but will be
queried and combined with those downloaded. A request could be split into
multiple downloading tasks and perfored concurrently and asynchronously, as
well as inserting the downloaded data to MySQL in the background.

**Data pre-existing** in MySQL database:

::

                                                       opening    high     low  closing  volume  barcount average
    Symbol DataType BarSize TickerTime                                           
    GS     TRADES   1d      2017-08-31 00:00:00-04:00   223.25  224.49  222.58   223.74   15491     10053 223.764
                            2017-09-01 00:00:00-04:00   224.55  227.56  223.53   225.88   16940     11739 226.350
                            2017-09-05 00:00:00-04:00   223.85  224.00  217.30   217.78   45499     28392 218.901

**Request for wider range of data:**

.. code-block:: python

    async def user_coro(req, broker, mysql):
        blk_ret = await get_hist_data(req, broker, mysql)
        return blk_ret

    # Request daily data of 8 days, from 8/29 - 9/8.
    # Data from 8/31 - 9/5 exist in local database and will not be downloaded.
    req = HistDataReq('Stock', 'GS', '1d', '8d', dtest(2017, 9, 9))
    broker = IB('127.0.0.1', 4002)
    db_info = {'host': '127.0.0.1', 'user': 'root', 'password': 'ibstract',
               'db': 'ibstract_test'}
    
    loop = asyncio.get_event_loop()
    mysql={**db_info, 'loop': loop}
    blk_ret = loop.run_until_complete(user_coro(req, broker, mysql))
    blk_ret.df

**Output data** is the combination of those in database and downloaded:

::

                                                       opening    high     low  closing  volume  barcount     average
    Symbol DataType BarSize TickerTime                                           
    GS	   TRADES   1d      2017-08-29 00:00:00-04:00   217.27  220.14  215.75   219.96   18795     12617    218.7545
                            2017-08-30 00:00:00-04:00   220.25  224.22  220.09   222.42   18580     12085    222.7730
                            2017-08-31 00:00:00-04:00   223.25  224.49  222.58   223.74   15491	    10053    223.7635
                            2017-09-01 00:00:00-04:00   224.55  227.56  223.53   225.88   16940	    11739    226.3505
                            2017-09-05 00:00:00-04:00   223.85  224.00  217.30   217.78   45499	    28392    218.9010
                            2017-09-06 00:00:00-04:00   218.98  221.02  217.61   218.83   26158	    15960    219.5335
                            2017-09-07 00:00:00-04:00   218.73  218.81  214.64   215.84   27963	    17892    215.7020
                            2017-09-08 00:00:00-04:00   215.51  219.28  215.40   217.21   23250	    15562    217.5120
             
Example 2: Create, update and combine MarketDataBlock instances.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**Input pandas.DataFrames** having different columns, symbols, barsize, and dates/times:

.. code-block:: python

    print(df_gs1)
    print(df_gs2)
    print(df_fb5m)
    print(df_fb1m)
    print(df_amzn)

::

     symbol  barsize                        date   close
   0     GS    5 min   2016-07-12 10:35:00-07:00  140.05
   1     GS    5 min   2016-07-12 11:20:00-07:00  141.34

     symbol  barSize                    datetime   close   volume
   0     GS    5 min   2016-07-12 10:35:00-07:00  140.05   344428

                     time       c     vol
   0  2016-07-21 09:30:00  120.05  234242
   1  2016-07-21 09:35:00  120.32  410842

                     time       c     vol
   0  2016-07-25 09:40:00  120.47  579638
   1  2016-07-25 09:41:00  120.82  192476

      symb     bar         date   close   volume
   0  AMZN   1 day   2016-07-21  749.22    27917
   1  AMZN   1 day   2016-07-22  738.87    36662
   2  AMZN   1 day   2016-07-23  727.23     8766

**MarketDatablock organizes DataFrames together:**

.. code-block:: python

    import pytz
    from ibstract import MarketDataBlock

    east = pytz.timezone('US/Eastern')
    
    blk = MarketDataBlock(df_gs1, datatype='TRADES', tz=east)
    blk.update(df_gs2, datatype='TRADES', tz=east)
    blk.update(df_fb5m, symbol='FB', datatype='TRADES', barsize='5m', tz=east)
    blk.update(df_fb1m, symbol='FB', datatype='TRADES', barsize='1m', tz=east)
    blk_amzn = MarketDataBlock(df_amzn, datatype='TRADES', tz=east)
    blk.combine(blk_amzn)

**Output MarketDataBlock:** ::

                                                       closing  volume
    Symbol DataType BarSize TickerTime                                
    AMZN   TRADES   1d      2016-07-21 00:00:00-04:00   749.22   27917
                            2016-07-22 00:00:00-04:00   738.87   36662
                            2016-07-23 00:00:00-04:00   727.23    8766
    FB     TRADES   1m      2016-07-25 09:40:00-04:00   120.47  579638
                            2016-07-25 09:41:00-04:00   120.82  192476
                    5m      2016-07-21 09:30:00-04:00   120.05  234242
                            2016-07-21 09:35:00-04:00   120.32  410842
    GS     TRADES   5m      2016-07-12 13:35:00-04:00   140.05  344428
                            2016-07-12 14:20:00-04:00   141.34      -1          


References
----------
* `Interactive Brokers API Documentation`_
* `Interactive Brokers API User Group`_


Changelog
---------

Version 1.0.0
^^^^^^^^^^^^^^^
* Migrated to native Python IB API.
* Asynchronous operations based on asyncio and aio-libs.
* New structures and features.
* Added documentation and test cases.

Version 0.1.0 (Deprecated)
^^^^^^^^^^^^^^^^^^^^^^^^^^
* This experimental version was developed based on IB API v9.72 or older, using swigibpy v0.5.0.


.. |PyVersion| image:: https://img.shields.io/badge/python-3.6+-blue.svg
.. |PyPiVersion| image:: https://badge.fury.io/py/ibstract.svg
                         :target: https://badge.fury.io/py/ibstract
.. |License| image:: https://img.shields.io/github/license/mashape/apistatus.svg
                     :target: https://github.com/jesseliu0/ibstract/blob/master/LICENSE
.. |Status| image:: https://img.shields.io/badge/status-alpha-orange.svg

.. _`Historical data operations example notebook`: http://nbviewer.jupyter.org/github/jesseliu0/ibstract/blob/master/examples/example_histdata.ipynb
.. _`MarketDataBlock class example notebook`: http://nbviewer.jupyter.org/github/jesseliu0/ibstract/blob/master/examples/example_MarketDataBlock.ipynb
.. _`IB class example notebook`: http://nbviewer.jupyter.org/github/jesseliu0/ibstract/blob/master/examples/example_brokers.ipynb

.. _`Interactive Brokers API`: https://interactivebrokers.github.io
.. _`IB gateway latest`: https://www.interactivebrokers.com/en/index.php?f=16454
.. _`Interactive Brokers API Documentation`: http://interactivebrokers.github.io/tws-api/
.. _`Interactive Brokers API User Group`: https://groups.io/g/twsapi/topics

.. _Python: https://www.python.org
.. _Anaconda: https://www.anaconda.com/download/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _aio-libs: https://github.com/aio-libs
.. _pandas: http://pandas.pydata.org/
.. _`ib_insync`: https://github.com/erdewit/ib_insync
.. _sqlalchemy: http://www.sqlalchemy.org
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _pytz: https://github.com/newvem/pytz
.. _tzlocal: https://github.com/regebro/tzlocal
