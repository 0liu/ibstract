"""
IB modules test scripts.
"""

from IB.gateway import IBWrapper, IBClient
from swigibpy import Contract as IBContract


if __name__ == "__main__":
    IB_wrapper = IBWrapper()
    IB_client_test = IBClient(IB_wrapper, 99)

    # test 1
    print(IB_client_test.speaking_clock())

    # test 2
    IB_contract = IBContract()
    IB_contract.secType = 'FUT'
    IB_contract.expiry = "201609"
    IB_contract.symbol = "GE"
    IB_contract.exchange = "GLOBEX"
    print(IB_client_test.get_hist_data(IB_contract))

    # close connection
    IB_client_test.eclient.eDisconnect()
