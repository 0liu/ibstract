"""
Obtaining, storing and processing historical and real-time market data.
"""


import pandas as pd


class MarketData(object):
    """
    Market data class.
    Orgainzed based on Pandas dataframe.

    self._depot stores all the historical data in Pandas DataFrame format,
    with a pre-defined MultiIndex ['Ticker', 'DateTime',].
    """

    def __init__(self, data=None):
        """
        Costruct HistData object and initialize data pool.
        data: a list of named tuples, whose field_names are data names
                   (ticker, date, price, etc.), which then merge to DataFrame
                   column names.
            Example:
                   Price = namedtuple('Price', 'ticker date price')
                   init_data = [
                                Price('GE', '2010-01-01', 30.00)
                                Price('GE', '2010-01-02', 31.00)
                   ]
        If init_data is empty(None), pool is initialized as an empty DataFrame.
        """

        if data is None:
            self._depot = pd.DataFrame()
        else:
            try:
                self._depot = pd.DataFrame(data, columns=data[0]._fields)
            except AttributeError:
                print('HistData must be initialized either by empty data,\
                or by a list of named tuples!')

    @property
    def depot(self, index_list=['ticker', 'date_time']):
        """
        Implement data storage as a property.
        Return a copy of data pool with index set to the specified list.
        Keyword Arguments:
        """
        # Filter out all valid index names in the specified list.
        indices = [x for x in index_list if x in self._depot.columns]

        # If any valid index except "ticker" and "date_time" specified,
        # make a copy of data frame, set its index, and return the copy.
        if indices:
            depot_copy = self._depot.copy()
            depot_copy.set_index(indices, inplace=True)
            return depot_copy
        else:
            return self._depot

    def add_data(self, data):
        """
        New data is merged with self.pool in the SQL "outer" (union) style.
        Note 1: Merging remove duplicate data rows.
        Note 2: The index of self._depot is a series of integers, which has
        no actual meaning (not time series) & may be changed by merging.

        Keyword Arguments:
        data: market data in DataFrame format.
        """
        # Re-index new data to conform with depot format

        # Merge with depot in SQL "outer"(union) style. Remove duplicate rows.
        self._depot.merge(data, how='outer')


# import pandas as pd
# from collections import namedtuple


# HistData = namedtuple('Price', 'Ticker DateTime Price')
# a = Price('GE', pd.to_datetime('2010-01-11').tz_localize('US/Pacific'), 35.00)
# b = Price('GE', pd.to_datetime('2010-01-12').tz_localize('US/Pacific'), 37.00)
# l = [a, b]
# df5 = pd.DataFrame(l, columns=l[0]._fields)


# idx=pd.MultiIndex(levels=[[],[]],labels=[[],[]],names=['symbol','date'])

# check if df6.index.names==idx.names
