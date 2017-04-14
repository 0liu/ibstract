#!/usr/bin/env python3

"""
Market data object and its methods, storing and processing
historical/real-time market data.
"""


import re
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


def timedur_to_std(time_dur_str):
    """Convert bar size to standard abbreviations:
       1. No space.
       2. One letter represents unit.
       3. s,m,h,d,W,M for seconds, minutes, hours, days, weeks and months.

    :param time_dur_str: A time duration string, like '1 min', '5days',etc.
    :returns: standardized bar size.
    :rtype: string.

    """

    # find all digits
    bs_num = re.findall('\d+', time_dur_str)[0]

    # find all letters
    bs_strs = re.findall('[a-zA-Z]', time_dur_str)

    if len(bs_strs) == 1:
        # If only one letter, lower/upper case "m"/"M" to diff min and month
        bs_unit = bs_strs[0].lower()
        if bs_unit not in ('s', 'm', 'h', 'd', 'w', 'y'):
            raise Exception('Invalid bar size unit: {}!'.format(time_dur_str))
        if bs_strs[0] in ('w', 'W', 'M'):  # Upper case for week/month
            bs_unit = bs_unit.upper()
    else:
        unit_map = {
            'sec': 's',
            'min': 'm',
            'hour': 'h',
            'hr': 'h',
            'day': 'd',
            'wk': 'W',
            'week': 'W',
            'mo': 'M',
            'mon': 'M',
            'yr': 'Y',
            'year': 'Y'
        }

        bs_unit = ''
        for k in unit_map.keys():
            bs_strs = re.findall(k, time_dur_str)
            if bs_strs:
                bs_unit = unit_map[k]
                break
        if not bs_unit:
            raise Exception("Invalid bar size unit: {}".format(time_dur_str))

    return bs_num + bs_unit


class MarketData(object):
    """
    Market data cache, for either historical or real-time data.

    There could be multiple MarketData objects for different purposes, such
    as monitoring real-time prices, retrieving historical data, research,
    modeling, etc. These cache objects can connect to a common database.

    self._depot stores data in pandas DataFrame format, with pre-defined
    MultiIndex format:
        ['Symbol', 'BarSize', 'DateTime']
        'Symbol': string.
        'BarSize': pandas Timedelta.
        'DateTime': pandas DatetimeIndex.
    """

    def __init__(self, data=None):
        """
        :param data: List of namedtuples, or dataframe. Initial market data.

        """

        # Pre-defined depot index.
        self.depot_index = ['Symbol', 'BarSize', 'DateTime']

        # Intialize depot.
        self._depot = pd.DataFrame()
        if data:
            self.update(data)

    @property
    def depot(self, index=None):
        """
        Implement data storage as a property.
        Return a copy of data depot with index set to the specified list.

        :param index: List of wanted index. E.g. ['Symbol','DateTime']

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

        :param data: List of namedtuples, or dataframe, with market data rows.
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
            data_df = data.reset_index()
        else:  # data is a list of namedtuple
            data_df = pd.DataFrame(data, columns=data[0]._fields)

        # Now set index to pre-defined, convert time index, and combine values.
        try:
            data_df['BarSize'] = data_df['BarSize'].map(timedur_to_std)
            data_df['DateTime'] = pd.DatetimeIndex(data_df['DateTime'])
            data_df.set_index(self.depot_index, inplace=True)
        except KeyError:
            print('''Input data fields must comply with pre-defined index:
            ['Symbol','BarSize','DateTime']
            Input data is not combined. Abort!''')
            return
        if self._depot.empty:  # initializing depot
            self._depot = data_df
        else:
            self._depot = data_df.combine_first(self._depot)

    def combine(self, another_market_data):
        """Combine the data in depot with another MarketData object.

        :param another_market_data: Another MarketData() object.
        :returns: No return. Side effect: self._depot is updated.
        """
        if another_market_data is MarketData:
            self.update(another_market_data.depot)
        else:
            print("Input object is not a MarketData class object. Abort!")


if __name__ == '__main__':
    DataRow_simple = namedtuple('DataRow_simple',
                                'Symbol BarSize DateTime Close')
    DataRow = namedtuple('DataRow', 'Symbol BarSize DateTime Close Volume')

    mkt_data_simple = [
        DataRow_simple('GS', '5 min', '2016-07-12 10:35:00', 140.05),
        DataRow_simple('GS', '5 min', '2016-07-12 11:20:00', 141.34),
    ]

    mkt_data_full = [
        DataRow('FB', '5 min', '2016-07-21 09:30:00', 120.05, 234242),
        DataRow('FB', '5 min', '2016-07-21 09:35:00', 120.32, 410842),
        DataRow('FB', '1 min', '2016-07-25 09:40:00', 120.47, 579638),
        DataRow('FB', '1 min', '2016-07-25 09:41:00', 120.82, 192476),
        DataRow('AMZN', '1 day', '2016-07-21', 749.22, 27917),
        DataRow('AMZN', '1 day', '2016-07-22', 738.87, 36662),
        DataRow('AMZN', '1 day', '2016-07-23', 727.23, 8766),
    ]

    mkt_data_oneline = [
        DataRow('GS', '5 min', '2016-07-12 10:35:00', 140.05, 344428),
    ]

    mktdata = MarketData(mkt_data_simple)
    mktdata.update(mkt_data_oneline)
    mktdata.update(mkt_data_full)
    print(mktdata.depot)
