"""
Test cases for brokers.
"""

import logging
import unittest
import pytz

from ibstract import IB
from .testdata import testdata_ib_connect
from .testdata import testdata_ib_req_hist_data


__all__ = ['IBTests']


_logger = logging.getLogger('ibstract.broker')
_logger.setLevel(level=logging.DEBUG)


class IBTests(unittest.TestCase):
    """
    Test cases for Interactive Brokers API.
    """
    def test_ib_connect(self):
        """Test async connect() and IB.semaphore.
        """
        def assert_ib_sem(sem, status, value):
            s = repr(sem).split()[-1][1:].strip(']>').split(',')
            self.assertEqual(s[0], status)
            self.assertEqual(sem._value, value)

        sem_init_val = IB.sem._value

        ib21 = IB(*testdata_ib_connect)
        self.assertTrue(ib21.client.isConnected())
        assert_ib_sem(IB.sem, 'unlocked', sem_init_val-1)

        ib22 = IB(*testdata_ib_connect)
        self.assertTrue(ib22.client.isConnected())
        assert_ib_sem(IB.sem, 'unlocked', sem_init_val-2)

        ib21.disconnect()
        self.assertFalse(ib21.client.isConnected())
        assert_ib_sem(IB.sem, 'unlocked', sem_init_val-1)

        ib22.disconnect()
        self.assertFalse(ib22.client.isConnected())
        assert_ib_sem(IB.sem, 'unlocked', sem_init_val)

        self.assertFalse(hasattr(ib21, 'clientid'))
        self.assertFalse(hasattr(ib22, 'clientid'))

    def test_req_hist_data_single(self, Broker=IB):
        """Test download historical data for a single request.
        """
        data = testdata_ib_req_hist_data
        broker = Broker(*data['login'])
        req, xchg_tz, datalen = data['reqs_tz_datalen'][0]
        blk = broker.req_hist_data(req)[0]
        self.assertGreaterEqual(len(blk), datalen)
        self.assertEqual(blk.tzinfo, xchg_tz)
        broker.disconnect()
        return blk

    def test_req_hist_data_many(self, Broker=IB):
        """Test download historical data concurrently for multiple requests.
        """
        data = testdata_ib_req_hist_data
        broker = Broker(*data['login'])
        reqs_tz_datalen = list(zip(*data['reqs_tz_datalen']))
        req_list = reqs_tz_datalen[0]
        tz_list = reqs_tz_datalen[1]
        datalen_list = reqs_tz_datalen[2]
        blk_list = broker.req_hist_data(*req_list)
        _logger.info([len(blk) for blk in blk_list])
        for blk, xchg_tz, datalen in zip(blk_list, tz_list, datalen_list):
            self.assertGreaterEqual(len(blk), datalen)
            self.assertEqual(blk.tzinfo, xchg_tz)
        broker.disconnect()
        return blk_list
