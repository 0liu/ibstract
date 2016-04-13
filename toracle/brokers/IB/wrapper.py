"""
Interactive Brokers API wrapper.
"""


import time
from configparser import ConfigParser
from swigibpy import EWrapper
from swigibpy import EPosixClientSocket


def read_gateway_config(configfile='gwconfig.ini'):
    """
    :param
    configfile: Configuration file containing IB Gateway IP, port and client ID.
    :return: IB Gateway IP, port and client ID.
    """

    gwconfig = ConfigParser()
    gwconfig.read(configfile)
    gwdefault = gwconfig['DEFAULT']
    return gwdefault['host'], gwdefault['port'], gwdefault['clientid']

