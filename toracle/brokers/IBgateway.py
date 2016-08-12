"""
Interactive Brokers API wrapper.
"""

import time
import datetime
import random
from configparser import ConfigParser
from collections import namedtuple

from swigibpy import EWrapper, EPosixClientSocket
from swigibpy import Contract as IBContract

from marketdata import MarketData


# Max wait time
MAX_WAIT = 30


def read_gateway_config(configfile='IBconfig.ini'):
    """
    :param
             configfile: Configuration file containing IB Gateway IP, port.
    :return:
             IB Gateway IP and port.
    :note:
             Client ID is not specified in config file, instead at IBClient
             instantialization.
    """

    gwconfig = ConfigParser()
    gwconfig.read(configfile)
    gwdefault = gwconfig['DEFAULT']
    return gwdefault['host'], int(gwdefault['port'])


class IBWrapper(EWrapper):
    """

    EWrapper implementation, which are callbacks passed to IBClient.

    """
    # def __init__(self):
    #     super().__init__()
    #     self.init_error()

    def error(self, errorid, errorCode, errorString):
        """
        error handling, simple for now

        Here are some typical IB errors
        INFO: 2107, 2106
        WARNING 326 - can't connect as already connected
        CRITICAL: 502, 504 can't connect to TWS.
            200 no security definition found
            162 no trades

        """

        # Any errors not on this list we just treat as information
        ERRORS_TO_TRIGGER = [201, 103, 502, 504, 509, 200, 162, 420, 2105,
                             1100, 478, 201, 399]

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

    def init_hist_data(self, HistDataTuple):
        self.req_hist_data_done = False
        self.hist_data_buf = MarketData()
        self.HistDataTuple = HistDataTuple

    # ##########################################################
    # Following virtual functions are defined/declared in IB_API

    def currentTime(self, time_from_server):
        self.data_time_now = time_from_server

    def historicalData(self, reqId, date, open_price, high, low, close,
                       volume, barCount, WAP, hasGaps):
        if date[:8] == 'finished':
            self.req_hist_data_done = True
        else:
            self.hist_data_buf.update(
                self.HistDataTuple(date, open_price, high, low, close, volume,
                                   barCount, WAP, hasGaps))

    def nextValidId(self, orderId):
        pass

    def managedAccounts(self, openOrderEnd):
        pass


class IBClient(object):
    def __init__(self, callback, clientid=999):
        eclient = EPosixClientSocket(callback)
        host, port = read_gateway_config()

        conn_success = eclient.eConnect(host, port, clientid)
        if conn_success:
            print("Successfully Connected to IB Gateway {0}:{1}."
                  .format(host, port))
        else:
            raise Exception("Connecting to IB Gateway {0}:{1} failed!".
                            format(host, port))

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
            finished = self.cb.data_time_now is not None
            iserror = self.cb.flag_iserror

            if (time.time() - start_time) > MAX_WAIT:
                iserror = True

            if iserror:
                print("Error happened")
                print(self.cb.error_msg)

        return self.cb.data_time_now

    def make_contract(self, security_type, symbol,
                      put_call='CALL', strike=0.0,
                      expiry='20160721', multiplier='10',
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
        contract.exchange = exchange
        contract.currency = currency
        contract.right = put_call
        contract.expiry = expiry
        contract.multiplier = multiplier
        contract.conId = random.randint(1001, 2000)
        return contract

    def req_hist_data(self, req_contract, req_endtime, req_len='1 w',
                      req_barsize='1 day', req_datatype='TRADES', useRTH=1):
        """Wrapper of IB API EClientSocket::reqHistoricalData().
        :param req_contract: An IB contract describing the requested security.
        :param req_endtime: endDateTime in IB API. String.
                            The end of requested time duration.
                            Format: "yyyymmdd HH:mm:ss ttt".
                            ttt, opt. time zone: GMT,EST,PST,MST,AST,JST,AET.
        :param req_len: durationStr in IB API. Time duration of data.
        :param req_barsize: barSizeSetting in IB API. String. Time resolution.
                            1/5/15/30 secs, 1/2/3/5/15/30 mins, 1 hour, 1 day.
        :param req_datatype: whatToShow in IB API. Requested data type.
                             "TRADES","MIDPOINT","BID", "ASK","BID_ASK",
                             "HISTORICAL_VOLATILITY",
                             "OPTION_IMPLIED_VOLATILITY".
        :param useRTH: Use regular trading hours. Whether to return all data
                       available during the requested time span, or only data
                       that falls within regular trading hours.
        :returns: Historical data in a single request.
        :rtype: A namedtuple defined in IBWrapper::historicalData().
        """

        self.cb.init_error()

        # Initalize data buffer and a namedtuple type for receiving data
        if req_datatype.upper() == 'TRADES':
            HistDataTuple = namedtuple('HistDataTuple',
                                       'DateTime, Open, High, Low, Close,'
                                       'Volume, BarCount, WAP, HasGaps')
        else:
            HistDataTuple = namedtuple('HistDataTuple',
                                       'DateTime, Open, High, Low, Close,'
                                       'Volume, BarCount, WAP, HasGaps')
        self.cb.init_hist_data(HistDataTuple)

        # Generate a random request Id in the range of [100,1000]
        req_id = random.randint(2000, 3000)

        # call EClientSocket function to request historical data
        self.ec.reqHistoricalData(req_id, req_contract, req_endtime,
                                       req_len, req_barsize, req_datatype,
                                       useRTH, 1, None)

        # Loop to check if request finished
        start_time = time.time()
        finished = False
        iserror = False
        while not (finished or iserror):
            finished = self.cb.req_hist_data_done
            iserror = self.cb.flag_iserror
            if(time.time() - start_time) > MAX_WAIT:
                iserror = True
            pass

        if iserror:
            print(self.cb.error_msg)
            raise Exception("Error requesting historic data.")

        return self.cb.hist_data


if __name__ == "__main__":
    import pdb
    import sys
    # pdb.set_trace()
    IB_wrapper = IBWrapper()
    IB_client_test = IBClient(IB_wrapper)

    # test 1
    if (not sys.argv[1:]) or (int(sys.argv[1]) == 1):
        print(IB_client_test.speaking_clock())

    # test 2
    if (not sys.argv[1:]) or (int(sys.argv[1]) == 2):
        security_type = 'STK'
        symbol = 'FB'
        req_endtime = '20160812 16:00:00 EST'
        req_len = '1 w'
        req_barsize = '1 day'
        req_datatype = 'TRADES'

        req_contract = IB_client_test.make_contract(
            security_type, symbol)
        hist_data = IB_client_test.req_hist_data(
            req_contract, req_endtime, req_len, req_barsize, req_datatype)
        print(hist_data)

    # close connection
    IB_client_test.ec.eDisconnect()
