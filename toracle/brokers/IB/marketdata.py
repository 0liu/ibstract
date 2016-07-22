"""
Obtaining, storing and processing historical and real-time market data.
"""


import pandas as pd
from collections import namedtuple


# check namedtuple instance
def isnamedtuple(x):
    """Check if x is a namedtuple.
    :rtype: Boolean.
    """
    return all((type(x).__bases__[0] is tuple,
                len(type(x).__bases__) == 1,
                isinstance(getattr(x, '_fields', None), tuple)))


class MarketData(object):
    """
    Market data cache, for either historical or real-time data.

    There could be multiple MarketData objects for different purposes, such
    as monitoring real-time prices, retrieving historical data, research,
    modeling, etc. These cache objects can connect to a common database.

    self._depot stores data in Pandas DataFrame format, with pre-defined
    MultiIndex format:
        ['Ticker', 'Interval', 'DateTime']
        'Ticker': string.
        'Interval': pandas Timedelta.
        'DateTime': pandas DatetimeIndex.
    """

    def __init__(self, data=None):
        """
        Initialize private data _depot.
        data: a list of namedtuples, whose _fields are data names, for example,
        ('Ticker', 'DateTime', 'Price').
        
        Example:
        Price = namedtuple('Price', 'Ticker DateTime Price Volume')
        data = [
                Price('FB', '2016-07-21', 30.00)
                Price('FB', '2016-07-22', 31.00)
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


def main():
    """Unit test main function.
    :returns: None
    :rtype: None
    """
    DataItem = namedtuple('DataItem', 'Ticker Interval DateTime Price Volume')
    # intvl = pd.Timedelta('5 min')
    mkt_data = [
        DataItem('FB', '5 min', '2016-07-21 09:30:00', 120.05, 234242),
        DataItem('FB', '5 min', '2016-07-21 09:35:00', 120.32, 410842),
        DataItem('FB', '1 min', '2016-07-25 09:40:00', 120.47, 579638),
        DataItem('FB', '1 min', '2016-07-25 09:41:00', 120.82, 192476),
        DataItem('GS', '5 min', '2016-07-12 10:35:00', 140.05, 39832),
        DataItem('GS', '5 min', '2016-07-12 11:20:00', 141.34, 19468),
        DataItem('AMZN', '1 day', '2016-07-21', 749.22, 27917),
        DataItem('AMZN', '1 day', '2016-07-22', 738.87, 36662),
        DataItem('AMZN', '1 day', '2016-07-23', 727.23, 8766),
    ]

    global mkt_data_df
    mkt_data_df = pd.DataFrame(mkt_data, columns=mkt_data[0]._fields)
    mkt_data_df['Interval'] = pd.TimedeltaIndex(mkt_data_df['Interval'])
    mkt_data_df['DateTime'] = pd.DatetimeIndex(mkt_data_df['DateTime'])
# mkt_data_df.set_index(['Ticker', 'Interval', 'DateTime'],
                          # inplace=True)


if __name__ == '__main__':
    main()
