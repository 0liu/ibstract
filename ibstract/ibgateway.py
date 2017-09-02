"""
Interactive Brokers API client and callbacks.
"""

import time
import datetime as dt
import re
import random
from configparser import ConfigParser
from collections import namedtuple, OrderedDict

# Import shared utilities
from marketdata import timedur_to_std

# Import swigibpy classes
from swigibpy import EWrapper, EPosixClientSocket
from swigibpy import Contract as IBContract

# Import IB settings and constants
from ibconfig import GATEWAY_HOST, GATEWAY_PORT
from ibconfig import ERRORS_TO_TRIGGER, MAX_WAIT_SECONDS
from ibconfig import REQ_TICK_TYPES, TICK_TYPES
from ibconfig import IBInvalidReqTickTypeName


# Utlities
def barsize_to_IB(barsize):
    """Convert bar size string to IB style and check validility.

    :param barsize: bar size in market data.
    :returns: IB-style bar size string.
    :rtype: string

    """
    bs = timedur_to_std(barsize)
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
        bs_IB = IB_barsize_map[bs]
    except KeyError:
        print("Error, barsize_to_IB(): Invalid input barsize string!")

    return bs_IB


def timedur_to_IB(time_dur_str):
    tdur = timedur_to_std(time_dur_str)
    tdur_num = re.findall('\d+', tdur)[0]
    tdur_unit = tdur[-1]
    if tdur_unit in ['s', 'd', 'W', 'M', 'Y']:
        return tdur_num + ' ' + tdur_unit.upper()
    else:
        raise Exception(("Error, timedur_to_IB():"
                         "Invalid time duration string for IB: {} !").
                        format(time_dur_str))


def ticktype_name2id(tick_type_name):
    try:
        return REQ_TICK_TYPES['Id'][tick_type_name.lower()]
    except KeyError:
        raise IBInvalidReqTickTypeName


class IBWrapper(EWrapper):
    """

    EWrapper implementation, which are callbacks passed to IBClient.

    """
    def __init__(self):
        super().__init__()
        self.init_error()

    def error(self, errorid, errorCode, errorString):
        """
        Error handling.
        """

        # Any error not in the TRIGGER list would be treated as information
        if errorCode in ERRORS_TO_TRIGGER:
            errormsg = "IB error id %d errorcode %d string %s"\
                       % (errorid, errorCode, errorString)
            self.flag_iserror = True
            self.error_msg = errormsg

    def init_error(self):
        self.flag_iserror = False
        self.error_msg = ""

        # init historical data related attributes
        self.req_hist_data_done = False

    def init_time(self):
        self.date_time_now = None

    def init_hist_data(self):
        self.req_hist_data_done = False
        self.hist_data_buf = []

    def init_tick_data(self):
        self.tick_data_streaming_end = False
        self.tick_data_buf = []


    # ##########################################################
    # Following virtual functions are defined/declared in IB_API

    def currentTime(self, time_from_server):
        self.date_time_now = time_from_server

    def historicalData(self, reqId, date, open_price, high, low, close,
                       volume, barCount, WAP, hasGaps):
        if date[:8] == 'finished':
            self.req_hist_data_done = True
        else:
            self.hist_data_buf.append((date, open_price, high, low, close,
                                       volume, barCount, WAP, hasGaps))

    def tickString(self, tickerId, field, value):
        tick_type_name = TICK_TYPES['Name'].get(int(field), 'NotDefined')
        print("tickString():", tick_type_name, value, sep=' ')

    def tickGeneric(self, tickerId, tickType, value):
        tick_type_name = TICK_TYPES['Name'].get(int(tickType), 'NotDefined')
        print("tickGeneric():", tick_type_name, value, sep=' ')

    def tickSize(self, tickerId, tickType, size):
        tick_type_name = TICK_TYPES['Name'].get(int(tickType), 'NotDefined')
        print("tickSize():", tick_type_name, size, sep=' ')

    def tickPrice(self, tickerId, tickType, price, canAutoExecute):
        tick_type_name = TICK_TYPES['Name'].get(int(tickType), 'NotDefined')
        print("tickPrice():", tick_type_name, price, sep=' ')

    def marketDataType(self, reqId, marketDataType):
        print("marketDataType():", reqId, marketDataType, sep = ' ')

    def nextValidId(self, orderId):
        pass

    def managedAccounts(self, openOrderEnd):
        pass


class IBClient(object):
    def __init__(self, callback, clientid=999):
        eclient = EPosixClientSocket(callback)
        conn_success = eclient.eConnect(GATEWAY_HOST, GATEWAY_PORT, clientid)
        if conn_success:
            print("Successfully Connected to IB Gateway {0}:{1}."
                  .format(GATEWAY_HOST, GATEWAY_PORT))
        else:
            raise Exception("Connecting to IB Gateway {0}:{1} failed!".
                            format(GATEWAY_HOST, GATEWAY_PORT))

        self.ec = eclient
        self.cb = callback

    def speaking_clock(self):
        """
        This function is only for test 1 in testib.py.
        """
        print("Getting the time... ")

        self.ec.reqCurrentTime()

        start_time = time.time()

        self.cb.init_error()
        self.cb.init_time()

        iserror = False
        finished = False

        while not (finished or iserror):
            finished = self.cb.date_time_now is not None
            iserror = self.cb.flag_iserror

            if (time.time() - start_time) > MAX_WAIT_SECONDS:
                iserror = True

            if iserror:
                print("Error happened")
                print(self.cb.error_msg)

        return self.cb.date_time_now

    def make_contract(self, security_type, symbol,
                      put_call='CALL', strike=0.0,
                      expiry='20160721', multiplier='1',
                      exchange='SMART', currency='USD'):
        """Make IB contract from input security parameters.

        :param security_type: 'STK','OPT','FUT','IND','FOP','CASH','BAG','NEWS'
        :param symbol: ticker symbol.
        :param put_call: 'CALL','PUT','C','P'
        :param strike: double. Option strike price.
        :param expiry: 'YYYYMM'. Futures expiration date.
        :param multiplier: Futures or options multipler. Only necessary when
                           multiple possibilities exist.
        :param exchange: 'SMART',etc.
        :param currency: 'USD',etc.
        :returns: An IB contract object for query contract details or
                  historical data.
        :rtype: swigibpy::Contract class.

        """

        contract = IBContract()
        contract.secType = security_type
        contract.symbol = symbol
        contract.right = put_call
        contract.strike = strike
        contract.expiry = expiry
        # contract.multiplier = multiplier  # not working with options
        contract.exchange = exchange
        contract.currency = currency
        # contract.conId = random.randint(1001, 2000)
        return contract

    def req_hist_data(self, req_contract, req_endtime, req_len='1 w',
                      req_barsize='1 day', req_datatype='TRADES', useRTH=1):
        """Request historical data with IB API EClientSocket::reqHistoricalData().
        :param req_contract: An IB contract describing the requested security.
        :param req_endtime: endDateTime in IB API. String.
                            The end of requested time duration.
                            Format: "yyyymmdd HH:mm:ss ttt".
                            ttt, opt. time zone: GMT,EST,PST,MST,AST,JST,AET.
        :param req_len: durationStr in IB API. Time duration of data.
        :param req_barsize: barSizeSetting in IB API. String. Time resolution.
        :param req_datatype: whatToShow in IB API. Requested data type.
                             "TRADES","MIDPOINT","BID", "ASK","BID_ASK",
                             "HISTORICAL_VOLATILITY",
                             "OPTION_IMPLIED_VOLATILITY".
        :param useRTH: Use regular trading hours. Whether to return all data
                       available during the requested time span, or only data
                       that falls within regular trading hours.
        :returns: Historical data in a single request.
        :rtype: A list of namedtuples defined in this function.
        """

        self.cb.init_error()
        self.cb.init_hist_data()

        # Generate a random request Id in the range of [100,1000]
        req_id = random.randint(2000, 3000)

        # Convert input strings to IB style
        IB_barsize = barsize_to_IB(req_barsize)
        IB_duration = timedur_to_IB(req_len)
        IB_whattoshow = req_datatype.upper()
        if IB_whattoshow not in ("TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK",
                                 "HISTORICAL_VOLATILITY",
                                 "OPTION_IMPLIED_VOLATILITY"):
            raise Exception("Invalid requested IB data type: {} !".
                            format(req_datatype))

        # call EClientSocket function to request historical data
        self.ec.reqHistoricalData(req_id, req_contract, req_endtime,
                                  IB_duration, IB_barsize, IB_whattoshow,
                                  useRTH, 1, None)

        # Loop to check if request finished
        start_time = time.time()
        finished = False
        iserror = False
        while not (finished or iserror):
            finished = self.cb.req_hist_data_done
            iserror = self.cb.flag_iserror
            if (time.time() - start_time) > MAX_WAIT_SECONDS:
                iserror = True
        if iserror:
            print(self.cb.error_msg)
            raise Exception("Error in requesting historic data!")

        # Add symbol and BarSize, and convert data to a list of namedtuples
        DataRow = namedtuple('HistDataTuple',
                             'Symbol, BarSize,'
                             'DateTime, Open, High, Low, Close,'
                             'Volume, BarCount, WAP, HasGaps')
        hist_data = []
        for data in self.cb.hist_data_buf:
            hist_data.append(DataRow(
                *((req_contract.symbol, req_barsize) + data)))

        return hist_data

    def req_mkt_data(self, contract, end_time=None, duration='00:01:00:00',
                     tick_type_names=['MKT_PRICE', ]):
        """Request streaming data with IB API EClientSocket::reqMktData().
        :param contract: An IB contract describing the requested security.
        :param endtime: A specified time to cancel data streming. String.
                        Note: if not None, it overrides next arg "duration".
                        strptime format: "%Y%m%d %H:%M:%S %z".
                        time zone: GMT,EST,PST,MST,AST,JST,AET.
        :param duration: Time duration of streaming. String.
                         Not effective if end_time is not None.
        :param tick_types: A tick type string to be converted to integer ID.
                https://www.interactivebrokers.com/en/software/api/apiguide/tables/generic_tick_types.htm
                https://www.interactivebrokers.com/en/software/api/apiguide/tables/tick_types.htm
        :returns: TBD.
        :rtype: A namedtuple defined in IBWrapper::historicalData().
        """

        self.cb.init_error()
        self.cb.init_tick_data()

        # Translate streaming stop condition, and define finished flag
        if end_time:  # use end_time to stop streaming
            end_dt = dt.datetime.strptime('end_time', "%Y%m%d %H:%M:%S %z")

            def finished_flag(start_time):
                return dt.datetime.now() > end_dt

        else:  # use time duration to stop streaming
            dur = [int(x) for x in duration.split(':')]
            dur_sec = dt.timedelta(days=dur[0], seconds=dur[3], minutes=dur[2],
                                   hours=dur[1]).total_seconds()

            def finished_flag(start_time):
                return (time.time() - start_time) > dur_sec

        # Generate a random request Id in the range of [100,1000]
        ticker_id = random.randint(2000, 3000)

        # Convert tick type name string to tick type integer id
        tick_type_ids = ','.join([str(ticktype_name2id(s)) for s in tick_type_names])

        # Request a market data stream
        self.ec.reqMktData(ticker_id, contract, tick_type_ids, snapshot=False,
                           mktDataOptions = None)
        start_time = time.time()

        # Loop to clocking the streaming
        finished = False
        iserror = False
        while not (finished or iserror):
            iserror = self.cb.flag_iserror
            if finished_flag(start_time):
                finished = True
                self.cb.tick_data_streaming_end = True
        self.ec.cancelMktData(ticker_id)

        if iserror:
            print(self.cb.error_msg)
            raise Exception("Error in streaming market data!")

        return


if __name__ == "__main__":
    import sys
    from marketdata import MarketData
    IB_wrapper = IBWrapper()
    ibc = IBClient(IB_wrapper)

    # test 1
    if (not sys.argv[1:]) or (int(sys.argv[1]) == 1):
        print(ibc.speaking_clock())

    # test 2
    if (not sys.argv[1:]) or (int(sys.argv[1]) == 2):
        security_type = 'STK'
        symbol = 'FB'
        # put_call = 'CALL'
        # strike = 170
        # expiry = '20170915'
        req_endtime = '20170831 13:00:00'
        req_len = '1 day'
        req_barsize = '5 min'
        req_datatype = 'TRADES'

        req_contract = ibc.make_contract(
            security_type, symbol)  # , put_call, strike, expiry)
        hist_data_list = ibc.req_hist_data(
            req_contract, req_endtime, req_len, req_barsize, req_datatype)
        hist_data = MarketData(hist_data_list)
        print(hist_data.depot)

    # test 3
    if (not sys.argv[1:]) or (int(sys.argv[1]) == 3):
        security_type = 'STK'
        symbol = 'FB'
        # put_call = 'CALL'
        # strike = 121
        # expiry = '20161021'
        # endtime = '20160920 13:00:00'
        duration = '00:00:00:15'
        ticktype_names = ['mkt_price', ]

        contract = ibc.make_contract(security_type, symbol)
        mkt_data_list = ibc.req_mkt_data(contract, duration=duration)
        mkt_data = MarketData(mkt_data_list)
        print(mkt_data.depot)

    # close connection
    ibc.ec.eDisconnect()
