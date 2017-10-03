"""
Utilities.
"""


import os
import re
import bisect
import pytz
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from functools import partial


NYSE_CAL = pd.to_datetime(
    pd.read_csv(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'nyse_dates.csv'))['NYSE'])

SEC_TYPES = ('Stock', 'Option', 'Future', 'Forex', 'Index', 'CFD', 'Commodity',
             'Bond', 'FuturesOption', 'MutualFund', 'Warrant')
HIST_DATA_TYPES = (
    'TRADES', 'MIDPOINT', 'BID', 'ASK', 'BID_ASK', 'ADJUSTED_LAST',
    'HISTORICAL_VOLATILITY', 'OPTION_IMPLIED_VOLATILITY',
    'REBATE_RATE', 'FEE_RATE',
    'YIELD_BID', 'YIELD_ASK', 'YIELD_BID_ASK', 'YIELD_LAST',)

MarketDataBlock_col_rename = {
    'symbol': 'Symbol',
    'symb': 'Symbol',
    'sym': 'Symbol',
    'datatype': 'DataType',
    'barsize': 'BarSize',
    'bar': 'BarSize',
    'tickertime': 'TickerTime',
    'ticktime': 'TickerTime',
    'date': 'TickerTime',
    'time': 'TickerTime',
    'datetime': 'TickerTime',  # avoid SQL keyword
    'open': 'opening',  # avoid SQL keyword
    'o': 'opening',
    'h': 'high',
    'l': 'low',
    'close': 'closing',  # avoid SQL keyword
    'c': 'closing',
    'vol': 'volume',
    'v': 'volume',
    'barcnt': 'barcount',
    'avg': 'average',
}

timezone_abbrv = {
    'AST': 'America/Halifax',
    'EST': 'US/Eastern',
    'EDT': 'US/Eastern',
    'CST': 'US/Central',
    'CDT': 'US/Central',
    'MST': 'US/Mountain',
    'MDT': 'US/Mountain',
    'PST': 'US/Pacific',
    'PDT': 'US/Pacific',
    'AKST': 'US/Alaska',
    'AKDT': 'US/Alaska',
    'HST': 'US/Hawaii',
    'HAST': 'US/Hawaii',
    'HADT': 'US/Hawaii',
    'SST': 'US/Samoa',
    'SDT': 'US/Samoa',
}


def datetime_tz(*args, tz=None):
    return tz.localize(datetime(*args, tzinfo=None))


dtutc = partial(datetime_tz, tz=pytz.utc)
dtest = partial(datetime_tz, tz=pytz.timezone('US/Eastern'))
dtcst = partial(datetime_tz, tz=pytz.timezone('US/Central'))
dtmst = partial(datetime_tz, tz=pytz.timezone('US/Mountain'))
dtpst = partial(datetime_tz, tz=pytz.timezone('US/Pacific'))


def tzcomb(dt1: datetime, dt2: datetime.time, tz: datetime.tzinfo):
    return tz.localize(datetime.combine(dt1, dt2))


tzmax = partial(tzcomb, dt2=datetime.max.time())
tzmin = partial(tzcomb, dt2=datetime.min.time())
utcomb = partial(tzcomb, tz=pytz.utc)
utcmax = partial(tzcomb, dt2=datetime.max.time(), tz=pytz.utc)
utcmin = partial(tzcomb, dt2=datetime.min.time(), tz=pytz.utc)
estcomb = partial(tzcomb, tz=pytz.timezone('US/Eastern'))
estmax = partial(tzcomb, dt2=datetime.max.time(),
                 tz=pytz.timezone('US/Eastern'))
estmin = partial(tzcomb, dt2=datetime.min.time(),
                 tz=pytz.timezone('US/Eastern'))


def is_namedtuple(x):
    """
    Check if x is a namedtuple.

    :rtype: Boolean.
    """
    return all((type(x).__bases__[0] is tuple,
                len(type(x).__bases__) == 1,
                isinstance(getattr(x, '_fields', None), tuple)))


def is_list_namedtuple(l):
    """
    Check if l is a list/tuple with all items are namedtuples.

    :rtype: Boolean.
    """
    return (isinstance(l, list) or isinstance(l, tuple))\
        and all(is_namedtuple(x) for x in l)


def timedur_standardize(timedur: str) -> str:
    """
    Convert a user-input ambiguous time duration string to standard
    abbreviations, following the rules:
       1. No space.
       2. One letter represents unit.
       3. s,m,h,d,W,M for seconds, minutes, hours, days, weeks and months.

    :param timedur: A user-input ambiguous time duration string,
                    like '1 min', '5days',etc.
    :returns: standardized time duration string.

    """
    timedur_num = re.findall('\d+', timedur)[0]  # find all digits
    timedur_strs = re.findall('[a-zA-Z]', timedur)  # find all letters

    if len(timedur_strs) == 1:
        # If only one letter, lower/upper case "m"/"M" to diff min and month
        timedur_unit = timedur_strs[0].lower()
        if timedur_unit not in ('s', 'm', 'h', 'd', 'w', 'y'):
            raise Exception(
                'Invalid input time duration unit: {}!'.format(timedur))
        if timedur_strs[0] in ('w', 'W', 'M', 'y', 'Y'):  # Upper case for week/month/year
            timedur_unit = timedur_unit.upper()
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
        timedur_unit = ''
        for k in unit_map.keys():
            timedur_strs = re.findall(k, timedur)
            if timedur_strs:
                timedur_unit = unit_map[k]
                break
        if not timedur_unit:
            raise TypeError(
                "Invalid input time duration unit: {}!".format(timedur))
    return timedur_num + timedur_unit


def timedur_to_reldelta(timedur: str) -> relativedelta:
    tdur = timedur_standardize(timedur)
    t_num = re.findall('\d+', tdur)[0]
    t_unit = tdur[-1]
    unit_delta = {
        's': relativedelta(seconds=int(t_num)),
        'm': relativedelta(minutes=int(t_num)),
        'h': relativedelta(hours=int(t_num)),
        'd': relativedelta(days=int(t_num)),
        'W': relativedelta(weeks=int(t_num)),
        'M': relativedelta(months=int(t_num)),
        'Y': relativedelta(years=int(t_num)),
    }
    try:
        reldelta = unit_delta[t_unit]
        return reldelta
    except KeyError:
        raise KeyError('Support only s, m, h, d, W, M, Y.')


def timedur_to_timedelta(timedur: str):
    reldelta = timedur_to_reldelta(timedur)
    t = datetime.now()
    return t + reldelta - t


def timedur_to_IB(timedur: str) -> str:
    """
    Convert a user-input ambiguous time duration string to IB-style
    durationString and check validility.

    :param timedur: A user-input ambiguous time duration string,
                    like '1 min', '5days',etc.
    :returns: IB-style durationString

    """
    tdur = timedur_standardize(timedur)
    t_num = re.findall('\d+', tdur)[0]
    t_unit = tdur[-1]
    if t_unit in ['m', 'h']:
        multip = {'m': 60, 'h': 3600}[t_unit]
        t_num = str(multip * int(t_num))
        t_unit = 's'
    if t_unit in ['s', 'd', 'W', 'M', 'Y']:
        return t_num + ' ' + t_unit.upper()
    else:
        raise TypeError(
            "Invalid input time duration string: {}!".format(timedur))


def barsize_to_IB(barsize: str) -> str:
    """
    Convert a user-input ambiguous bar size string to IB-style barSizeSetting
    and check validility.

    :param barsize: A user-input ambiguous time duration string,
                    like '1 min', '5days',etc.
    :returns: IB-style barSizeSetting

    """
    timedur = timedur_standardize(barsize)
    IB_barsize_map = {
        '1s': '1 secs',
        '5s': '5 secs',
        '10s': '10 secs',
        '15s': '15 secs',
        '30s': '30 secs',
        '1m': '1 min',
        '2m': '2 mins',
        '3m': '3 mins',
        '5m': '5 mins',
        '10m': '10 mins',
        '15m': '15 mins',
        '20m': '20 mins',
        '30m': '30 mins',
        '1h': '1 hour',
        '2h': '2 hours',
        '3h': '3 hours',
        '4h': '4 hours',
        '8h': '8 hours',
        '1d': '1 day',
        '1W': '1W',
        '1M': '1M'
    }
    try:
        barSizeSetting = IB_barsize_map[timedur]
    except KeyError:
        raise KeyError("Invalid input barsize string: {}!".format(barsize))
    return barSizeSetting


def trading_days(
        time_end: datetime, time_dur: str=None, time_start: datetime=None):
    """determine start and end trading days covering time_dur or time_start.
       So far use NYSE trading days calendar for all exchanges.
    """
    xchg_tz = time_end.tzinfo
    end_idx = bisect.bisect_left(NYSE_CAL, time_end.replace(tzinfo=None))
    if time_start is not None:
        # ignore time_dur, use time_start, time_end as boundary.
        start_idx = bisect.bisect_left(
            NYSE_CAL, time_start.replace(tzinfo=None))
        trading_days = NYSE_CAL[start_idx:end_idx]
    else:
        tdur = timedur_to_timedelta(time_dur)
        # If tdur remainding h/m/s > 0, round it up to 1 more day.
        n_trading_days = tdur.days
        if xchg_tz.normalize(
                time_end - relativedelta(seconds=tdur.seconds)
        ) < tzmin(time_end, tz=xchg_tz):
            n_trading_days += 1
        # time_dur in days, and time_end is not beginning of day.
        if time_end.time() != datetime.min.time():
            n_trading_days += 1
        # Slicing from trading day calendar.
        trading_days = NYSE_CAL[end_idx-n_trading_days:end_idx]
    return trading_days
