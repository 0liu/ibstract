"""
Interactive Brokers API constants, settings and exceptions.
"""


from io import StringIO
import pandas as pd
from datetime import timedelta


__all__ = ['IB_DEFAULT_HOST', 'IB_DEFAULT_PORT', 'IB_HIST_DATA_TYPES',
           'IB_ERRORS', 'IB_REQ_TICK_TYPES', 'IB_TICK_TYPES',
           'IBInvalidReqTickTypeName']


# --- Global settings for IB API ---

# Default IB Gateway IP and port.
IB_DEFAULT_HOST = '127.0.0.1'
IB_DEFAULT_PORT = 4001


# --- IB API constants ---

IB_HIST_DATA_TYPES = (
    'TRADES', 'MIDPOINT', 'BID', 'ASK', 'BID_ASK', 'ADJUSTED_LAST',
    'HISTORICAL_VOLATILITY', 'OPTION_IMPLIED_VOLATILITY',
    'REBATE_RATE', 'FEE_RATE',
    'YIELD_BID', 'YIELD_ASK', 'YIELD_BID_ASK', 'YIELD_LAST',)


IB_HIST_DATA_STEPS = {  # BarSize: MaxAllowedDuration
    '1s': '30m',
    '5s': '1h',
    '10s': '4h',
    '15s': '4h',
    '30s': '8h',
    '1m': '1d',
    '2m': '2d',
    '3m': '1W',
    '5m': '1W',
    '10m': '1W',
    '15m': '1W',
    '20m': '1W',
    '30m': '1M',
    '1h': '1M',
    '2h': '1M',
    '3h': '1M',
    '4h': '1M',
    '8h': '1M',
    '1d': '1Y',
    '1W': '1Y',
    '1M': '1Y',
}

# Typical IB errors
#   INFO - 2107, 2106
#   WARNING - 326: can't connect as already connected
#   CRITICAL - 502, 504: can't connect to TWS.
#   200: no security definition found
#   162: no trades
IB_ERRORS = [201, 103, 502, 504, 509, 200, 162, 420, 2105,
             1100, 478, 201, 399]


# Generic tick type names and IDs to request market data
# Note: names are defined here from the descriptions in IB API reference.
req_tick_types_csv = StringIO("""
Name            Id
opt_vol         100
opt_open_int    101
hist_volat      104
opt_iv          106
idx_fut_prm     162
mkt_price       221
auc_vals        225
rtvolume        233
shortable       236
inventory       256
fun_ratios      258
news            292
rt_hist_volat   411
div             456
""")
IB_REQ_TICK_TYPES = pd.read_csv(
    req_tick_types_csv, delim_whitespace=True, index_col=0)


# Tick Types
tick_types_csv = StringIO("""
Value   Name                         Function
-1      NA                           None
0       BID_SIZE                     tickSize()
1       BID_PRICE                    tickPrice()
2       ASK_PRICE                    tickPrice()
3       ASK_SIZE                     tickSize()
4       LAST_PRICE                   tickPrice()
5       LAST_SIZE                    tickSize()
6       HIGH                         tickPrice()
7       LOW                          tickPrice()
8       VOLUME                       tickSize()
9       CLOSE_PRICE                  tickPrice()
10      BID_OPTION_COMPUTATION       tickOptionComputation()
11      ASK_OPTION_COMPUTATION       tickOptionComputation()
12      LAST_OPTION_COMPUTATION      tickOptionComputation()
13      MODEL_OPTION_COMPUTATION     tickOptionComputation()
14      OPEN_TICK                    tickPrice()
15      LOW_13_WEEK                  tickPrice()
16      HIGH_13_WEEK                 tickPrice()
17      LOW_26_WEEK                  tickPrice()
18      HIGH_26_WEEK                 tickPrice()
19      LOW_52_WEEK                  tickPrice()
20      HIGH_52_WEEK                 tickPrice()
21      AVG_VOLUME                   tickSize()
22      OPEN_INTEREST                tickSize()
23      OPTION_HISTORICAL_VOL        tickGeneric()
24      OPTION_IMPLIED_VOL           tickGeneric()
25      OPTION_BID_EXCH              None
26      OPTION_ASK_EXCH              None
27      OPTION_CALL_OPEN_INTEREST    tickSize()
28      OPTION_PUT_OPEN_INTEREST     tickSize()
29      OPTION_CALL_VOLUME           tickSize()
30      OPTION_PUT_VOLUME            tickSize()
31      INDEX_FUTURE_PREMIUM         tickGeneric()
32      BID_EXCH                     tickString()
33      ASK_EXCH                     tickString()
34      AUCTION_VOLUME               tickSize()
35      AUCTION_PRICE                tickPrice()
36      AUCTION_IMBALANCE            tickSize()
37      MARK_PRICE                   tickPrice()
38      BID_EFP_COMPUTATION          tickEFP()
39      ASK_EFP_COMPUTATION          tickEFP()
40      LAST_EFP_COMPUTATION         tickEFP()
41      OPEN_EFP_COMPUTATION         tickEFP()
42      HIGH_EFP_COMPUTATION         tickEFP()
43      LOW_EFP_COMPUTATION          tickEFP()
44      CLOSE_EFP_COMPUTATION        tickEFP()
45      LAST_TIMESTAMP               tickString()
46      SHORTABLE                    tickString()
47      FUNDAMENTAL_RATIOS           tickString()
48      RT_VOLUME                    tickString()
49      HALTED                       None
50      BIDYIELD                     tickPrice()
51      ASKYIELD                     tickPrice()
52      LASTYIELD                    tickPrice()
53      CUST_OPTION_COMPUTATION      tickOptionComputation()
54      TRADE_COUNT                  tickGeneric()
55      TRADE_RATE                   tickGeneric()
56      VOLUME_RATE                  tickGeneric()
""")
IB_TICK_TYPES = pd.read_csv(tick_types_csv, delim_whitespace=True, index_col=0)


# --- Exception definitions ---

class IBInvalidReqTickTypeName(Exception):
    def __init__(self):
        req_tick_types_all = '  ' + '\n  '.join(IB_REQ_TICK_TYPES.index)
        msg = ("Invalid IB tick type name.\n" +
               "Valid tick types names are:\n" + req_tick_types_all)
        super().__init__(msg)
