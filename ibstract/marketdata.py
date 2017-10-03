"""
Market data management.
- Acquring, processing, caching historical data blocks, and read/write the
  database.
- Streaming market data in real time.
"""
import logging
from datetime import datetime, timezone
import pytz
from tzlocal import get_localzone
import numpy as np
import pandas as pd
import asyncio
from aiomysql.sa import create_engine as aio_create_engine
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import String, Float, DateTime
from sqlalchemy.dialects.mysql import INTEGER as mysqlINTEGER
from sqlalchemy.sql import and_

from .utils import SEC_TYPES, HIST_DATA_TYPES
from .utils import MarketDataBlock_col_rename as col_rename
from .utils import tzcomb, tzmax, tzmin
from .utils import timedur_standardize
from .utils import timedur_to_reldelta
from .utils import trading_days


_logger = logging.getLogger('ibstract.marketdata')
__all__ = ['MarketDataBlock', 'HistDataReq', 'init_db', 'query_hist_data',
           'insert_hist_data', 'hist_data_req_start_end', 'get_hist_data',
           'download_insert_hist_data', 'query_hist_data_split_req']


class MarketDataBlock:
    """
    A data block contains a bunch of time series market data, such as OHLC and
    volume, indexed by symbol, date/time, and bar size.

    Composition design pattern is used instead of subclassing pandas.DataFrame.
    Class methods are customized for maintaining market data integrity, besides
    all the features of pandas.DataFrame.

    Data block is stored in the internal pandas.DataFrame self.data with fixed
    MultiIndex: ['Symbol', 'DataType', 'BarSize', 'TickerTime'].
        - 'DataType': string
        - 'Symbol': string
        - 'BarSize': pandas Timedelta
        - 'TickerTime': pandas DatetimeIndex
    """

    data_index = ['Symbol', 'DataType', 'BarSize', 'TickerTime']
    dtlevel = data_index.index('TickerTime')

    def __init__(self, df: pd.DataFrame, symbol: str=None, datatype: str=None,
                 barsize: str=None, tz: str=None):
        self.df = pd.DataFrame()
        if df is not None:
            self.update(
                df, symbol=symbol, datatype=datatype, barsize=barsize, tz=tz)

    def __len__(self):
        return len(self.df)

    def __eq__(self, mkt_data_block):
        return self.df == mkt_data_block.data

    def __repr__(self):
        return repr(self.df)

    def __str__(self):
        return str(self.df)

    def tz_convert(self, tzinfo):
        if not self.df.empty:
            self.df = self.df.tz_convert(tzinfo, level=self.__class__.dtlevel)

    @property
    def tzinfo(self):
        if self.df.empty:
            return None
        return self.df.index.levels[self.__class__.dtlevel].tzinfo

    @tzinfo.setter
    def tzinfo(self, tzinfo):
        self.tz_convert(tzinfo)

    @property
    def tz(self):
        if self.df.empty:
            return None
        return self.df.index.levels[self.__class__.dtlevel].tz

    @tz.setter
    def tz(self, tzinfo):
        self.tz_convert(tzinfo)

    def _standardize_index(
            self, df_in: pd.DataFrame, symbol: str=None, datatype: str=None,
            barsize: str=None, tz: str=None):
        """Normalize input DataFrame index to MarketDataBlock standard.
        """
        # Add or starndardize index names in the input.
        if isinstance(df_in.index, pd.MultiIndex):
            df_in.reset_index(inplace=True)

        # Rename ambiguous column names.
        df_in.columns = [
            col_rename.get(col.strip().lower(), col.strip().lower())
            for col in df_in.columns]

        # Insert Symbol, DataType, Barsize columns from arguments if not
        # found in the input dataframe.
        for col in MarketDataBlock.data_index:
            if col not in df_in.columns:
                if locals().get(col.lower(), None) is None:
                    raise KeyError(
                        'No {0} argument and no {0} column in the DataFrame.'
                        .format(col))
                df_in.insert(0, col, locals()[col.lower()])

        # Convert datetime strings to pandas DatetimeIndex
        df_in['TickerTime'] = pd.DatetimeIndex(
            df_in['TickerTime'].apply(pd.Timestamp))

        # Standardize BarSize strings
        df_in['BarSize'] = df_in['BarSize'].map(timedur_standardize)

        # Set index to class-defined MultiIndex
        df_in.set_index(MarketDataBlock.data_index, inplace=True)

        # Set time zone so all DatetimeIndex are tz-aware
        df_in_tz = df_in.index.levels[self.__class__.dtlevel].tz
        if df_in_tz is None or isinstance(df_in_tz, timezone) or \
           isinstance(df_in_tz, pytz._FixedOffset):
            # Input df has naive time index, or tzinfo is not pytz.timezone()
            if tz is None:
                raise ValueError(
                    'Argument tz=None, and TickerTime.tzinfo is None(naive),'
                    'datetime.timezone, or pytz._FixedOffset.')
            if df_in_tz is None:
                df_in = df_in.tz_localize(tz, level=self.__class__.dtlevel)
            else:
                df_in = df_in.tz_convert(tz, level=self.__class__.dtlevel)

        return df_in

    def update(self, df_in: pd.DataFrame, symbol: str=None, datatype: str=None,
               barsize: str=None, tz: str=None, standardize_index=True):
        """
        Input data is combined with self.df. Overlapped data will be
        overwritten by non-null values of input data. Indexes and Columns
        will be unioned.
        """
        # Check input data type
        if not (isinstance(df_in, pd.DataFrame)):
            raise TypeError('Input data must be a pandas.DataFrame.')

        # Check empty data
        if df_in.empty:
            return self

        # Standardize index
        if standardize_index:
            df_in = self._standardize_index(
                df_in.copy(), symbol=symbol, datatype=datatype,
                barsize=barsize, tz=tz)

        # Combine input DataFrame with internal self.df
        if self.df.empty:  # Initialize self.df
            self.df = df_in.sort_index()
        else:
            df_in = df_in.tz_convert(self.tzinfo, level=self.__class__.dtlevel)
            self.df = df_in.combine_first(self.df).sort_index()

        # Post-combination processing
        # Fill NaN, and enforce barcount and volume columns dtype to int64
        self.df.fillna(-1, inplace=True)
        for col in self.df.columns:
            if col.lower() in ('barcount', 'volume'):
                self.df[col] = self.df[col].astype(np.int64)

    def combine(self, blk):
        """Combine with another MarketDataBlock object.
        """
        if not isinstance(blk, MarketDataBlock):
            raise TypeError("Parameter is not a MarketDataBlock instance.")
        self.update(blk.df, standardize_index=False)


class HistDataReq:
    """
    User request for historical data.

    If BarSize in d/W/M and TimeDur in h/m/s, TimeDur will be converted by
    24hours/day, not actual trading hours.
    A user should use h/m/s time_dur only for intraday data (BarSize in h/m/s).
    TimeEnd should be in DateTime format.
    """
    __slots__ = ('_sectype', '_symbol', '_barsize', '_timedur', '_timeend',
                 '_datatype', '_exchange', '_currency')

    def __init__(self, sectype, symbol, barsize, timedur, timeend=None,
                 datatype='TRADES', exchange='SMART', currency='USD'):
        self.SecType = sectype
        self.Symbol = symbol
        self.BarSize = barsize
        self.TimeDur = timedur
        self.TimeEnd = timeend
        self.DataType = datatype
        self.Exchange = exchange
        self.Currency = currency

    def __repr__(self):
        return ("{}({}, {}, {}, {}, {}, {}, {}, {})".format(
            self.__class__.__name__,
            self.SecType, self.Symbol, self.BarSize, self.TimeDur,
            self.TimeEnd, self.DataType, self.Exchange, self.Currency))

    def __eq__(self, req):
        return all(getattr(self, attr) == getattr(req, attr)
                   for attr in HistDataReq.__slots__)

    @property
    def SecType(self):
        return self._sectype

    @SecType.setter
    def SecType(self, sectype):
        if sectype.upper() == 'CFD':
            sectype = 'CFD'
        elif sectype.upper() == 'FUTURESOPTION':
            sectype = 'FuturesOption'
        elif sectype.upper() == 'MUTUALFUND':
            sectype = 'MutualFund'
        else:
            sectype = sectype.title()
        if sectype not in SEC_TYPES:
            raise TypeError('Invalid req.SecType.')
        self._sectype = sectype

    @property
    def Symbol(self):
        return self._symbol

    @Symbol.setter
    def Symbol(self, symbol):
        self._symbol = symbol.upper()

    @property
    def BarSize(self):
        return self._barsize

    @BarSize.setter
    def BarSize(self, barsize):
        self._barsize = timedur_standardize(barsize)

    @property
    def TimeDur(self):
        return self._timedur

    @TimeDur.setter
    def TimeDur(self, timedur):
        self._timedur = timedur_standardize(timedur)

    @property
    def TimeEnd(self):
        return self._timeend

    @TimeEnd.setter
    def TimeEnd(self, timeend):
        if timeend is None:
            self._timeend = datetime.now(tz=pytz.utc)
        elif not isinstance(timeend, datetime):
            raise TypeError("req.TimeEnd must be a datetime.datetime object.")
        else:
            # Always use timezone-aware datetime.
            if timeend.tzinfo is None:
                _logger.warning('Naive HistDataReq.TimeEnd. '
                                'Assumeing system local time zone.')
                tz_system = get_localzone()
                timeend = tz_system.localize(timeend)
            self._timeend = timeend

    @property
    def DataType(self):
        return self._datatype

    @DataType.setter
    def DataType(self, datatype):
        if datatype not in HIST_DATA_TYPES:
            raise TypeError('Invalid req.DataType.')
        self._datatype = datatype.upper()

    @property
    def Exchange(self):
        return self._exchange

    @Exchange.setter
    def Exchange(self, exchange):
        self._exchange = exchange.upper()

    @property
    def Currency(self):
        return self._currency

    @Currency.setter
    def Currency(self, currency):
        self._currency = currency.upper()


def _gen_sa_table(sectype, metadata=None):
    """Generate SQLAlchemy Table object by sectype.
    """
    if metadata is None:
        metadata = MetaData()
    table = Table(
        sectype, metadata,
        Column('Symbol', String(20), primary_key=True),
        Column('DataType', String(20), primary_key=True),
        Column('BarSize', String(10), primary_key=True),
        Column('TickerTime', DateTime(), primary_key=True),
        Column('opening', Float(10, 2)),
        Column('high', Float(10, 2)),
        Column('low', Float(10, 2)),
        Column('closing', Float(10, 2)),
        Column('volume', mysqlINTEGER(unsigned=True)),
        Column('barcount', mysqlINTEGER(unsigned=True)),
        Column('average', Float(10, 2))
    )
    return table


def init_db(db_info):
    db_conn = "mysql+pymysql://{0}:{1}@{2}/{3}".format(
        db_info['user'], db_info['password'], db_info['host'], db_info['db'])
    engine = create_engine(db_conn, echo=False)
    metadata = MetaData(engine, reflect=True)

    sec_type_list = ['Index', 'Stock', 'Option', 'Future', 'Commodity',
                     'FuturesOption', 'Forex', 'Bond', 'MutualFund',
                     'CFD', 'Warrant']
    for sectype in sec_type_list:
        if sectype not in metadata.tables.keys():
            table = _gen_sa_table(sectype, metadata=metadata)
            table.create(engine, checkfirst=True)
    engine.dispose()


async def query_hist_data(
        engine: object, sectype: str, symbol: str, datatype: str, barsize: str,
        start: datetime=None, end: datetime=None) -> MarketDataBlock:
    """Query database on conditions.
    """
    if start is None:
        start = pytz.UTC.localize(datetime(1, 1, 1))
    if end is None:
        end = pytz.UTC.localize(datetime(9999, 12, 31, 23, 59, 59))
    table = _gen_sa_table(sectype)
    stmt = table.select().where(
        and_(
            table.c.Symbol == symbol,
            table.c.DataType == datatype,
            table.c.BarSize == barsize,
            table.c.TickerTime.between(
                start.astimezone(pytz.UTC), end.astimezone(pytz.UTC))
        )
    )
    async with engine.acquire() as conn:
        result = await conn.execute(stmt)
    df = pd.DataFrame(list(result), columns=table.columns.keys())
    blk = MarketDataBlock(df, tz='UTC')
    blk.tz_convert(start.tzinfo)
    return blk


async def insert_hist_data(engine: object, sectype: str, blk: MarketDataBlock):
    records = blk.df.reset_index().to_dict('records')
    for r in records:
        r['TickerTime'] = r['TickerTime'].to_pydatetime().astimezone(pytz.UTC)
    table = _gen_sa_table(sectype)
    async with engine.acquire() as conn:
        await conn.execute(
            table.insert().prefix_with('IGNORE').values(records))
        await conn.execute('commit')  # github.com/aio-libs/aiomysql/issues/70


async def download_insert_hist_data(
        req: HistDataReq, broker: object, engine: object,
        insert_limit: tuple=None) -> MarketDataBlock:
    """
    Download historical data for a single request, and insert data to database.
    """
    blk_list = await broker.req_hist_data_async(req)
    blk = MarketDataBlock(blk_list[0].df.copy())
    if insert_limit is not None:
        start = insert_limit[0].astimezone(pytz.UTC)
        end = insert_limit[1].astimezone(pytz.UTC)
        blk.df = blk.df.loc(axis=0)[:, :, :, start:end]
    await insert_hist_data(engine, req.SecType, blk)
    return blk_list[0]


def hist_data_req_start_end(req: HistDataReq, xchg_tz: pytz.tzinfo):
    """
    Calculate start and end datetime for a historical data request.
    If req.TimeDur and BarSize both are in h/m/s, data range is limited to
    the intraday data of req.TimeEnd.date().
    :param xchg_tz: Time zone info of the security exchange for req.
    """
    time_dur = req.TimeDur
    end_dt = xchg_tz.normalize(req.TimeEnd)
    if time_dur[-1] in ('W', 'M', 'Y'):
        start_dt = xchg_tz.normalize(end_dt - timedur_to_reldelta(time_dur))
        trd_days = trading_days(end_dt, time_start=start_dt)
    elif time_dur[-1] is 'd':
        # trd_days is a DateTimeIndex, with consecutive integer index.
        trd_days = trading_days(end_dt, time_dur)
        _logger.debug('trd_days: \n%s', trd_days)
        start_date = trd_days.iloc[0].to_pydatetime()
        start_dt = tzcomb(start_date, end_dt.time(), xchg_tz)
    else:  # TimeDur in h/m/s.
        trd_days = trading_days(end_dt, time_dur)
        _logger.debug('trd_days: \n%s', trd_days)
        if req.BarSize[-1] is 'd':
            # BarSize in d. Start time set to 00:00:00 of start date.
            start_date = trd_days.iloc[0].to_pydatetime()
            start_dt = tzmin(start_date, tz=xchg_tz)
        else:
            # BarSize in h/m/s; Limit to intraday data.
            _logger.warning(
                'req.TimeDur and req.BarSize are both in h/m/s.'
                'Time range limit to intraday.')
            start_date = trd_days.iloc[-1].to_pydatetime()
            start_dt = max(tzmin(start_date, tz=xchg_tz),
                           xchg_tz.normalize(
                               end_dt-timedur_to_reldelta(req.TimeDur)))
    return start_dt, end_dt, trd_days


async def query_hist_data_split_req(
        req: HistDataReq, xchg_tz: pytz.tzinfo, engine: object):
    """
    Query historical data from database, based on which downloading requests
    are generated.
    For req.BarSize < 1 day, download step is 1 day; otherwise 1 year.
    Consecutive trading days are grouped to one request.
    :param xchg_tz: Time zone info of the security exchange for req.
    """
    # Support BarSize in 'd', 'h', 'm' so far.
    if timedur_standardize(req.BarSize)[-1] in ('s', 'W', 'M'):
        raise NotImplementedError

    start_dt, end_dt, trd_days = hist_data_req_start_end(req, xchg_tz)

    # Query from database between start_dt and end_dt
    blk_db = await query_hist_data(
        engine, req.SecType, req.Symbol, req.DataType, req.BarSize,
        start_dt, end_dt)
    if not blk_db.df.empty:
        blk_db.tz = xchg_tz
        blk_db_dates = blk_db.df.index.levels[blk_db.__class__.dtlevel].date
    else:
        blk_db_dates = []

    # Logic to find datetime gaps in database for the request.
    # Convention: Download step = 1 year for BarSize >= 1 day.
    #             Download step = 1 day  for BarSize 'h' or 'm'.
    if req.BarSize[-1] in ('d', 'W', 'M', 'Y'):
        if req.TimeDur[-1] is 'd':
            # count back by trading days, if req.TimeDur in days
            trd_years = set(dt.year for dt in trd_days)
        else:
            # count back by calendar weeks, months, or years
            trd_years = set(range(start_dt.year, end_dt.year+1))
        _logger.debug('trd_years: %s', trd_years)
        blk_db_years = set(dt.year for dt in blk_db_dates)
        if datetime.now(tz=xchg_tz).year in blk_db_years:
            # always download current year
            blk_db_years.remove(datetime.now(tz=xchg_tz).year)
        trd_year_gap = sorted(trd_years ^ blk_db_years)
        _logger.debug('trd_year_gap: %s', trd_year_gap)
        timedur_timeend_download = [
            ('1y', tzmax(datetime(yr, 12, 31), tz=xchg_tz))
            for yr in trd_year_gap]
        insert_limit = [
            (
                xchg_tz.localize(datetime(yr, 1, 1)),
                tzmax(datetime(yr+1, 12, 31), tz=xchg_tz)
            ) for yr in trd_year_gap]
    else:  # Download step is 1 day for BarSize < 1 day (1min~8hours).
        # Find trading day gaps in the data from database
        trd_dates = [tday.date() for tday in trd_days]
        _logger.debug('trd_dates: %s', trd_dates)
        trd_day_gap = sorted(set(trd_dates) ^ set(blk_db_dates))
        trd_day_gap_idx = [trd_days.index[trd_dates.index(d)]
                           for d in trd_day_gap]
        _logger.debug('trd_day_gap: %s', trd_day_gap)
        _logger.debug('trd_day_gap_idx: %s', trd_day_gap_idx)
        # Group consecutive trading days in the gaps
        timedur_timeend_download, insert_limit = [], []
        dur_days = 1
        for i, idx in enumerate(trd_day_gap_idx):
            if i == len(trd_day_gap_idx) - 1 or trd_day_gap_idx[i+1] > idx + 1:
                dl_start = tzmin(trd_days[idx-dur_days+1], tz=xchg_tz)
                dl_end = tzmax(trd_days[idx], tz=xchg_tz)
                timedur_timeend_download.append((str(dur_days) + 'd', dl_end))
                insert_limit.append((dl_start, dl_end))
                dur_days = 1
            else:
                dur_days += 1
    _logger.debug('timedur_timeend_download: %s', timedur_timeend_download)
    # Build HistDataReq list
    download_reqs = []
    for dur_days, end_day in timedur_timeend_download:
        download_reqs.append(HistDataReq(
            req.SecType, req.Symbol, req.BarSize,
            dur_days, end_day,
            req.DataType, req.Exchange, req.Currency
        ))

    return download_reqs, insert_limit, blk_db, start_dt, end_dt


async def get_hist_data(
        req: HistDataReq, broker: object, mysql: dict=None) -> MarketDataBlock:
    """
    Return a MarketDataBlock object containing historical market data for a
    user request. All the involved operations are asynchronously
    concurrent, including downloading data, merging data in memory, and query
    and saving data with a MySQL database.

    The function will first determine which parts of the requested data exist
    in the MySQL database. The parts of requested data not in the database will
    be automatically downloaded asynchronously, provided a broker API service
    is available. If a database is unavailable, all requested data will be
    downloaded.

    The downloaded data for any single request will be immediately combined to
    a MarketDataBlock object, while other requested data are still being
    downloaded. The downloaded data will also be asynchronously inserted to the
    database.

    :param mysql: {'host': str, 'user': str, 'password': str, 'db': str,
                   'loop': asyncio.BaseEventLoop}
    """
    xchg_tz = await broker.hist_data_req_timezone(req)

    # All data will be downloaded from broker if database is unavailable
    # or requested BarSize not in database.
    if mysql is None or timedur_standardize(req.BarSize)[-1] is 's':
        blk_list = await broker.req_hist_data_async(req)
        blk = blk_list[0]
        blk.tz_convert(xchg_tz)
        return blk

    # init database
    engine = await aio_create_engine(
        host=mysql['host'], user=mysql['user'], password=mysql['password'],
        db=mysql['db'], loop=mysql['loop'])

    # Query database first, and split req for downloading
    (dl_reqs, insert_limit, blk_ret,
     start_dt, end_dt) = await query_hist_data_split_req(req, xchg_tz, engine)
    _logger.debug('blk_ret head:\n%s', blk_ret.df.iloc[:3])
    _logger.debug('start_dt: %s', start_dt)
    _logger.debug('end_dt: %s', end_dt)

    # Download data and insert to db concurrently
    if dl_reqs is not None:
        blk_dl_list = await asyncio.gather(*(
            download_insert_hist_data(req_i, broker, engine, inslim)
            for req_i, inslim in zip(dl_reqs, insert_limit)))
        for blk_dl in blk_dl_list:
            _logger.debug('blk_dl head:\n%s', blk_dl.df.iloc[:3])
            blk_ret.combine(blk_dl)
            _logger.debug('Combined blk_ret head:\n%s', blk_ret.df.iloc[:3])
        # Limit time range according to req
        blk_ret.df = blk_ret.df.loc(axis=0)[:, :, :, start_dt:end_dt]

    # wrap up
    engine.close()
    await engine.wait_closed()
    return blk_ret
