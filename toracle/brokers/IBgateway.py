"""
Interactive Brokers API wrapper.
"""

import time
import datetime
from configparser import ConfigParser
from collections import namedtuple

from swigibpy import EWrapper, EPosixClientSocket
from marketdata import MarketData

# Max wait time
MAX_WAIT = 30


def read_gateway_config(configfile='gwconfig.ini'):
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
    def __init__(self):
        self.init_error()
        self.init_time()
        self.init_hist_data()

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

    def init_hist_data(self, req_id):
        self.req_hist_data_done = False
        self.hist_data_buf = None

    # ##########################################################
    # Following virtual functions are defined/declared in IB_API

    def currentTime(self, time_from_server):
        self.data_time_now = time_from_server

    def historicalData(self, reqId, date, open_price, high, low, close,
                       volume, barCount, WAP, hasGaps):
        if date[:8] == 'finished':
            self.req_hist_data_done = True
        else:
            DataTuple = namedtuple('DataTuple',
                                   'DateTime, Open, High, Low, Close, Volume,\
                                   BarCount, WAP, HasGaps')
            self.hist_data = DataTuple(date, open_price, high, low,
                                       close, volume, barCount, WAP, hasGaps)
            # hist_data.add_data([data_rcved, ])

    def nextValidId(self, orderId):
        pass

    def managedAccounts(self, openOrderEnd):
        pass


class IBClient(object):
    def __init__(self, callback, clientid):
        eclient = EPosixClientSocket(callback)
        host, port = read_gateway_config()
        eclient.eConnect(host, port, clientid)

        self.eclient = eclient
        self.cb = callback

    def speaking_clock(self):
        """
        This function is only for test 1 in testib.py.
        """
        print("Getting the time... ")

        self.eclient.reqCurrentTime()

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

    def req_hist_data(self, contract, durationStr='1 w',
                      barSizeSetting='1 day', req_id=9999):
        """
        Request historical data for a contract, up to today.

        Keyword Arguments:
        contract -- IB contract defined in API.
        duration -- (default '1 Y')
        bar_size -- (default '1 day')
        req_id -- (default MEANINGLESS_NUMBER)
        """

        self.cb.init_error()
        self.cb.init_hist_data(req_id)

        # time_now = datetime.datetime.now()
        # time_now_str = time_now.strftime("%Y%m%d %H:%M:%S %Z")
        time_now_str = "20000725 13:00:00"

        # call EClientSocket function to request historical data
        self.eclient.reqHistoricalData(req_id, contract, time_now_str,
                                       durationStr, barSizeSetting,
                                       'Trades', 1, 1, None)

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
