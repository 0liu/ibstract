"""
Interactive Brokers API wrapper.
"""

import time
from configparser import ConfigParser
from swigibpy import EWrapper
from swigibpy import EPosixClientSocket

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

    def init_error(self):
        setattr(self, "flag_iserror", False)
        setattr(self, "error_msg", "")

    def error(self, id, errorCode, errorString):
        """
        error handling, simple for now

        Here are some typical IB errors
        INFO: 2107, 2106
        WARNING 326 - can't connect as already connected
        CRITICAL: 502, 504 can't connect to TWS.
            200 no security definition found
            162 no trades

        """

        ## Any errors not on this list we just treat as information
        ERRORS_TO_TRIGGER = [201, 103, 502, 504, 509, 200, 162, 420, 2105, 1100, 478, 201, 399]

        if errorCode in ERRORS_TO_TRIGGER:
            errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
            print(errormsg)
            setattr(self, "flag_iserror", True)
            setattr(self, "error_msg", True)

    ## time handling
    def init_time(self):
        setattr(self, "data_time_now", None)

    def currentTime(self, time_from_server):
        setattr(self, "data_time_now", time_from_server)

    def nextValidId(self, orderId):
        pass

    def managedAccounts(self, openOrderEnd):
        pass


class IBClient(object):
    def __init__(self, callback, clientid):
        ecskt = EPosixClientSocket(callback)
        host, port = read_gateway_config()
        ecskt.eConnect(host, port, clientid)

        self.ecskt = ecskt
        self.cb = callback

    def speaking_clock(self):
        print("Getting the time... ")

        self.ecskt.reqCurrentTime()

        start_time = time.time()

        self.cb.init_error()
        self.cb.init_time()

        iserror = False
        finished = False

        while not (finished or iserror):
            finished = self.cb.data_time_now is not None
            iserror = self.cb.flag_iserror

            if (time.time() - start_time) > MAX_WAIT:
                finished = True

            if iserror:
                print("Error happened")
                print(self.cb.error_msg)
                finished = True

        return self.cb.data_time_now


if __name__ == "__main__":
    """
    This simple example returns the time
    """

    ibwrapper = IBWrapper()
    ibclient_test = IBClient(ibwrapper, 99)
    print(ibclient_test.speaking_clock())
    ibclient_test.ecskt.eDisconnect()

    
