"""
Obtaining, storing and processing historical and real-time market data.
"""


import pandas as pd


# Globals holding data
# All historical data stored on disk
# hist_pool to be implemented with a database/storage (NoSql, HDF5)
# Historical data cache stored in memory for current session
# hist_cache = pd.DataFrame()


class HistData(object):
    """
    Historical market data cache.
    Orgainzed based on Pandas dataframe.

    self.pool stores all the historical data in Pandas DataFrame format,
    and its has no meaningful index (just integers by default).
    """

    def __init__(self, init_data=None):
        """
        Costruct HistData object and initialize data pool.
        init_data: a list of named tuples, whose field_names are data names
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

        global hist_cache
        if init_data is None:
            self.pool = pd.DataFrame()
        else:
            try:
                self.pool = pd.DataFrame(init_data,
                                         columns=init_data[0]._fields)
            except AttributeError:
                print('HistData must be initialized either by empty data,\
                or by a list of named tuples!')

    def data_by_index(self, index_list):
        """
        Return a copy of data pool with index set to the specified list.
        """
        pool_copy = self.pool.copy()

        # filter out the data names that do not exist.
        exist_indices = [x for x in index_list if x in pool_copy.columns]
        # Set indices
        pool_copy.set_index(exist_indices, inplace=True)
        return pool_copy

    def add_data(self, data):
        """
        Keyword Arguments:
        data: list of named tuples.
        New data is merged with self.pool in the SQL "outer" (union) style.
        Note 1: Merging remove duplicate data rows.
        Note 2: The index of self.pool is a series of integers, which has
        no actual meaning (not time series) & may be changed by merging.
        """
        try:
            df_data = pd.DataFrame(data, columns=data[0]._fields)
        except AttributeError:
            print('HistData must be initialized either by empty data,\
            or by a list of named tuples!')

        self.pool.merge(df_data, how='outer')
