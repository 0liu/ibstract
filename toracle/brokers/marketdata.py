#!/usr/bin/env python3

"""
Obtaining, storing and processing historical and real-time market data.
"""


import pandas as pd
from collections import namedtuple


# check namedtuple instance
def is_namedtuple(x):
    """Check if x is a namedtuple.
    :rtype: Boolean.
    """
    return all((type(x).__bases__[0] is tuple,
                len(type(x).__bases__) == 1,
                isinstance(getattr(x, '_fields', None), tuple)))


def is_list_namedtuple(l):
    """Check if l is a list/tuple with all items are namedtuples.
    :rtype: Boolean.
    """
    return (isinstance(l, list) or isinstance(l, tuple))\
        and all(is_namedtuple(x) for x in l)


class MarketData(object):
    """
    Market data cache, for either historical or real-time data.

    There could be multiple MarketData objects for different purposes, such
    as monitoring real-time prices, retrieving historical data, research,
    modeling, etc. These cache objects can connect to a common database.

    self._depot stores data in pandas DataFrame format, with pre-defined
    MultiIndex format:
        ['Ticker', 'Interval', 'DateTime']
        'Ticker': string.
        'Interval': pandas Timedelta.
        'DateTime': pandas DatetimeIndex.
    """

    def __init__(self, data=None):
        """
        :param data: List of namedtuples, or dataframe. Initial market data.

        """

        # Pre-defined depot index.
        self.depot_index = ['Ticker', 'Interval', 'DateTime']

        # Intialize depot.
        self._depot = pd.DataFrame()
        if data:
            self.update(data)

    @property
    def depot(self, index=None):
        """
        Implement data storage as a property.
        Return a copy of data depot with index set to the specified list.

        :param index: List of wanted index. E.g. ['Ticker','DateTime']

        """
        # Filter out all valid index names in the specified list.
        if index:
            valid = [x for x in index
                     if ((x in self._depot.columns) or
                         (x in self._depot.index.names))]
            if valid:
                return self._depot.reset_index().set_index(valid, inplace=True)
        else:
            return self._depot

    def update(self, data):
        """
        New data is combined with self._depot and overwritten with non-null
        values of input data. Indexes and Columns will be unioned.

        :param data: List of namedtuples, or dataframe. Market data slices.

        """
        # Check input data type
        if not((isinstance(data, pd.DataFrame)) or is_list_namedtuple(data)):
            print('''Input data must be a dataframe or list of namedtuples.
            Input data is not combined. Abort!''')
            return

        # Check empty data.
        if (isinstance(data, pd.DataFrame) and data.empty) or not data:
            print("Input data is empty. Kidding me?\n")
            return

        # Reset dataframe index, or convert namedtuples to dataframe.
        if isinstance(data, pd.DataFrame):
            data.reset_index(inplace=True)
        else:  # data is a list of namedtuple
            data_df = pd.DataFrame(data, columns=data[0]._fields)

        # Now set index to pre-defined, convert time index, and combine values.
        try:
            data_df['Interval'] = pd.TimedeltaIndex(data_df['Interval'])
            data_df['DateTime'] = pd.DatetimeIndex(data_df['DateTime'])
            data.set_index(self.depot_index, inplace=True)
        except KeyError:
            print('''Input data fields must comply with pre-defined index:
            ['Ticker','Interval','DateTime']
            Input data is not combined. Abort!''')
            return
        if self._depot.empty:  # initializing depot
            self._depot = data_df
        else:
            self._depot = data_df.combine_first(self._depot)


def main():
    """Unit test main function.
    :returns: None
    :rtype: None
    """

    # depot_index = pd.MultiIndex(levels=[[], [], []],
    #                             labels=[[], [], []],
    #                             names=['Ticker', 'Interval', 'DateTime'])
    # depot_columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    DataItem1 = namedtuple('DataItem', 'Ticker Interval DateTime Close')
    # intvl = pd.Timedelta('5 min')
    mkt_data1 = [
        DataItem1('GS', '5 min', '2016-07-12 10:35:00', 140.05),
        DataItem1('GS', '5 min', '2016-07-12 11:20:00', 141.34),
    ]
    DataItem = namedtuple('DataItem', 'Ticker Interval DateTime Close Volume')
    mkt_data = [
        DataItem('FB', '5 min', '2016-07-21 09:30:00', 120.05, 234242),
        DataItem('FB', '5 min', '2016-07-21 09:35:00', 120.32, 410842),
        DataItem('FB', '1 min', '2016-07-25 09:40:00', 120.47, 579638),
        DataItem('FB', '1 min', '2016-07-25 09:41:00', 120.82, 192476),
        DataItem('AMZN', '1 day', '2016-07-21', 749.22, 27917),
        DataItem('AMZN', '1 day', '2016-07-22', 738.87, 36662),
        DataItem('AMZN', '1 day', '2016-07-23', 727.23, 8766),
    ]
    mkt_data2 = [
        DataItem('GS', '5 min', '2016-07-12 10:35:00', 140.05, 344428),
    ]

    global mkt_data1_df
    mkt_data1_df = pd.DataFrame(mkt_data1, columns=mkt_data1[0]._fields)
    mkt_data1_df['Interval'] = pd.TimedeltaIndex(mkt_data1_df['Interval'])
    mkt_data1_df['DateTime'] = pd.DatetimeIndex(mkt_data1_df['DateTime'])
    print(mkt_data1_df)

    global mkt_data2_df
    mkt_data2_df = pd.DataFrame(mkt_data2, columns=mkt_data2[0]._fields)
    mkt_data2_df['Interval'] = pd.TimedeltaIndex(mkt_data2_df['Interval'])
    mkt_data2_df['DateTime'] = pd.DatetimeIndex(mkt_data2_df['DateTime'])
    print(mkt_data2_df)

    global mkt_data_df
    mkt_data_df = pd.DataFrame(mkt_data, columns=mkt_data[0]._fields)
    mkt_data_df['Interval'] = pd.TimedeltaIndex(mkt_data_df['Interval'])
    mkt_data_df['DateTime'] = pd.DatetimeIndex(mkt_data_df['DateTime'])
    mkt_data_df.set_index(MarketData.depot_index)
    print(mkt_data_df)

    global depot
    depot = pd.DataFrame()

    if depot.empty:
        depot = mkt_data1_df.set_index(MarketData.depot_index)
        depot.sortlevel(inplace=True)
    print('\n\n', depot)

    # merge
    col_inter = list(depot.columns.intersection(mkt_data_df.columns))
    depot = pd.merge(depot, mkt_data_df, how='outer', left_index=True,
                     right_index=True, on=col_inter)
    # depot.sortlevel(inplace=True)
    # print('\n\n\n', depot)

if __name__ == '__main__':
    main()
