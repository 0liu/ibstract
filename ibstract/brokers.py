"""
Async concurrent operations on brokers API.
"""
import logging
import pytz
import abc
import asyncio
import ib_insync

from .utils import timedur_to_IB, barsize_to_IB
from .utils import timezone_abbrv
from .ibglobals import IB_HIST_DATA_TYPES
from .ibglobals import IB_HIST_DATA_STEPS
from .marketdata import MarketDataBlock


_logger = logging.getLogger('ibstract.broker')
__all__ = ['IB']


class Broker(abc.ABC):
    """Common interface for broker objects.
    """
    @abc.abstractmethod
    def connect(self, host, port, timeout):
        raise NotImplementedError

    @abc.abstractmethod
    def disconnect(self):
        raise NotImplementedError

    @abc.abstractmethod
    def hist_data_req_contract_details(self, req: tuple):
        raise NotImplementedError

    @abc.abstractmethod
    def hist_data_req_timezone(self, req: tuple):
        raise NotImplementedError

    @abc.abstractmethod
    async def req_hist_data_async(self, *req_parms: [tuple]):
        raise NotImplementedError

    @abc.abstractmethod
    def req_hist_data(self, *req_parms: [tuple]):
        raise NotImplementedError


class IB(ib_insync.IB, Broker):
    """
    Coroutine methods support async operations with Interactive Brokers API.
    """
    clientid_baskets = set(range(21, 53))
    sem = asyncio.BoundedSemaphore(30)  # IB max allowed client = 32

    contract_makers = {
        'STOCK': ib_insync.Stock, 'OPTION': ib_insync.Option,
        'FUTURE': ib_insync.Future, 'FOREX': ib_insync.Forex,
        'INDEX': ib_insync.Index, 'CFD': ib_insync.CFD,
        'COMMODITY': ib_insync.Commodity, 'BOND': ib_insync.Bond,
        'FUTURESOPTION': ib_insync.FuturesOption,
        'MUTUALFUND': ib_insync.MutualFund,
        'WARRANT': ib_insync.Warrant
    }
    hist_data_steps = IB_HIST_DATA_STEPS

    def __init__(self, host: str=None, port: int=None, timeout: int=2):
        super().__init__()
        if host and port and host.strip():
            self.connect(host.strip(), port, timeout)

    async def connect_async(self, host: str, port: int, timeout: int=2):
        if not self.client.isConnected():
            await IB.sem.acquire()
            self.host = host
            self.port = port
            self.clientid = IB.clientid_baskets.pop()
            await self.connectAsync(
                self.host, self.port, self.clientid, timeout)
            self._offline_cleanup()

    def connect(self, host: str, port: int, timeout: int=2):
        """Override super().connect() to include IB.clientid and IB.semaphore.
        """
        self.run(self.connect_async(host, port, timeout))

    @property
    def connected(self):
        return self.client.isConnected()

    def _hist_data_req_to_contract(self, req: object):
        """Convert marketdata.HistDataReq to IB contract.
        """
        contract_maker = IB.contract_makers[req.SecType.upper()]
        contract = contract_maker(
            req.Symbol.upper(), req.Exchange.upper(), req.Currency.upper())
        return contract

    def _hist_data_req_to_args(self, req: object):
        """Convert marketdata.HistDataReq to IB arguments.
        """
        assert req.DataType.upper() in IB_HIST_DATA_TYPES,\
            'Invalid IB data type requested: %s' % req.DataType
        contract = self._hist_data_req_to_contract(req)
        endDateTime = pytz.UTC.normalize(
            req.TimeEnd).strftime('%Y%m%d %H:%M:%S %Z')
        durationStr = timedur_to_IB(req.TimeDur)
        barSizeSetting = barsize_to_IB(req.BarSize)
        whatToShow = req.DataType.upper()
        useRTH = True if barSizeSetting in ('1 day', '1W', '1M') else False
        formatDate = 2  # enforce UTC output
        keepUpToDate = False
        chartOptions = None
        return (contract, endDateTime, durationStr, barSizeSetting, whatToShow,
                useRTH, formatDate, keepUpToDate, chartOptions)

    async def hist_data_req_contract_details(self, req: object):
        """Download contract details for a HistDataReq.
        """
        contract = self._hist_data_req_to_contract(req)
        details_list = await self.reqContractDetailsAsync(contract)
        return details_list

    async def hist_data_req_timezone(self, req: object):
        """Download contract details and retrieve timezone for a HistDataReq.
        """
        details_list = await self.hist_data_req_contract_details(req)
        timezone_id = details_list[0].timeZoneId
        for abbrv, zone in timezone_abbrv.items():
            if abbrv in timezone_id:
                return pytz.timezone(zone)

    async def req_hist_data_async(self, *req_list: [object]):
        """
        Concurrently downloads historical market data for multiple requests.
        """
        ibparms_list = (self._hist_data_req_to_args(req) for req in req_list)
        bars_list = await asyncio.gather(*(
            self.reqHistoricalDataAsync(*ibparms)
            for ibparms in ibparms_list))
        df_list = [ib_insync.util.df(bars) for bars in bars_list]
        xchg_tz_list = await asyncio.gather(*(
            self.hist_data_req_timezone(req) for req in req_list))
        blk_list = []
        for req, df, xchg_tz in zip(req_list, df_list, xchg_tz_list):
            _logger.debug(df.iloc[:3])
            if req.BarSize[-1] in ('d', 'W', 'M'):  # not intraday
                dl_tz = xchg_tz  # dates without timezone, init with xchg_tz.
            else:
                dl_tz = pytz.UTC
            blk = MarketDataBlock(df, symbol=req.Symbol, datatype=req.DataType,
                                  barsize=req.BarSize, tz=dl_tz)
            blk.tz_convert(xchg_tz)
            blk_list.append(blk)
        return blk_list

    def req_hist_data(self, *req_list: [object]):
        """
        Blocking version of get_hist_data_async().
        """
        return self.run(self.req_hist_data_async(*req_list))

    def disconnect(self):
        if self.client.isConnected():
            super().disconnect()
            self._offline_cleanup()

    def _offline_cleanup(self):
        if not self.client.isConnected():
            IB.sem.release()
            IB.clientid_baskets.add(self.clientid)
            del self.clientid
            del self.port
            del self.host
