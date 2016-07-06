"""
IB modules test scripts.
"""

import sys
from gateway import IBWrapper, IBClient
from swigibpy import Contract as IBContract
import pdb

if __name__ == "__main__":
    pdb.set_trace()
    IB_wrapper = IBWrapper()
    IB_client_test = IBClient(IB_wrapper, 99)

    # test 1
    if (not sys.argv[1:]) or (int(sys.argv[1]) == 1):
        print(IB_client_test.speaking_clock())

    # test 2
    if (not sys.argv[1:]) or (int(sys.argv[1]) == 2):
        IB_contract = IBContract()
        IB_contract.secType = 'STK'
        IB_contract.symbol = 'F'
        IB_contract.exchange = 'SMART'
        IB_contract.currency = 'USD'
        hist_data = IB_client_test.req_hist_data(IB_contract)
        print(hist_data)

    # close connection
    IB_client_test.eclient.eDisconnect()
