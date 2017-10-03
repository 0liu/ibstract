"""
Test cases for data downloading, storing, access and management.
"""

import warnings
import logging
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import asyncio
import aiomysql.sa as aiosa
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.sql import select

from ibstract import MarketDataBlock
from ibstract import init_db
from ibstract import query_hist_data
from ibstract import insert_hist_data
from ibstract import download_insert_hist_data
from ibstract import hist_data_req_start_end
from ibstract import query_hist_data_split_req
from ibstract import get_hist_data
from .testdata import testdata_market_data_block_merge
from .testdata import testdata_db_info
from .testdata import testdata_insert_hist_data
from .testdata import testdata_query_hist_data
from .testdata import testdata_download_insert_hist_data
from .testdata import testdata_req_start_end
from .testdata import testdata_query_hist_data_split_req
from .testdata import testdata_get_hist_data


__all__ = ['MarketDataBlockTests', 'HistDataTests']


warnings.filterwarnings("ignore")
root_logger = logging.getLogger()
root_logger.disabled = True
for name, logger in logging.Logger.manager.loggerDict.items():
    if 'ib_insync' in name:
        logger.disabled = True
logging.getLogger('aiomysql').disabled = True
logging.getLogger('sqlalchemy.engine.base.Engine').disabled = True
logging.getLogger('ibstract.marketdata').setLevel(level=logging.DEBUG)
_logger = logging.getLogger('ibstract.test_marketdata')
_logger.setLevel(level=logging.DEBUG)


class MarketDataBlockTests(unittest.TestCase):
    """
    Test cases for MarketDataBlock methods.
    """
    def test_market_data_block_merge(self):
        testdata = testdata_market_data_block_merge
        blk = MarketDataBlock(pd.DataFrame(testdata[0]),
                              datatype='TRADES', tz='US/Pacific')
        _logger.info('\n\nBlockTests:merge: Starting blk:\n%s', blk.df)
        for data in testdata[1:]:
            blk.update(pd.DataFrame(data[0]),
                       datatype='TRADES', tz='US/Pacific')
            blk_direct = MarketDataBlock(
                pd.DataFrame(data[1]), datatype='TRADES', tz='US/Pacific')
            _logger.debug('\n\nBlockTests:merge: blk.df\n%s', blk.df[:3])
            _logger.debug('\n\nBlockTests:merge: blk_direct.df\n%s',
                          blk_direct.df[:3])
            assert_frame_equal(blk.df, blk_direct.df)
            self.assertEqual(list(blk.df.index.names),
                             blk.__class__.data_index)
            self.assertEqual(list(blk_direct.df.index.names),
                             blk.__class__.data_index)


class HistDataTests(unittest.TestCase):
    """
    Test cases: Download, save and load historical market data.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_info = testdata_db_info
        self.db_conn = "mysql+pymysql://{}:{}@{}/{}".format(
            self.db_info['user'], self.db_info['password'],
            self.db_info['host'], self.db_info['db'])

    def _clear_db(self):
        # delete all exsiting tables and create new tables
        engine = create_engine(self.db_conn, echo=False)
        metadata = MetaData(engine, reflect=True)
        sec_type_list = ['Index', 'Stock', 'Option', 'Future', 'Commodity',
                         'FuturesOption', 'Forex', 'Bond', 'MutualFund',
                         'CFD', 'Warrant']
        for sectype in sec_type_list:
            if sectype in metadata.tables.keys():
                metadata.tables[sectype].drop()
        engine.dispose()

    def test_init_db(self):
        self._clear_db()
        init_db(self.db_info)
        engine = create_engine(self.db_conn, echo=False)
        metadata = MetaData(engine, reflect=True)
        sec_type_list = ['Index', 'Stock', 'Option', 'Future', 'Commodity',
                         'FuturesOption', 'Forex', 'Bond', 'MutualFund',
                         'CFD', 'Warrant']
        for sectype in sec_type_list:
            self.assertIn(sectype, metadata.tables.keys())
        engine.dispose()

    def test_insert_hist_data(self):
        self._clear_db()
        init_db(self.db_info)

        # Insert two time-overlapped MarketDataBlocks
        async def run(loop, data):
            engine = await aiosa.create_engine(
                user=self.db_info['user'], db=self.db_info['db'],
                host=self.db_info['host'], password=self.db_info['password'],
                loop=loop, echo=False)
            await insert_hist_data(engine, 'Stock', data[0])
            await insert_hist_data(engine, 'Stock', data[1])
            engine.close()
            await engine.wait_closed()

        # Execute insertion
        blk0 = MarketDataBlock(testdata_insert_hist_data[0])
        blk1 = MarketDataBlock(testdata_insert_hist_data[1])
        data = [blk0, blk1]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run(loop, data))

        # Verify insertion
        df_source = testdata_insert_hist_data[2]
        engine = create_engine(self.db_conn)
        conn = engine.connect()
        metadata = MetaData(engine, reflect=True)
        table = metadata.tables['Stock']
        result = conn.execute(select([table]))
        # self.assertEqual(result.keys(), list(df_source.columns))
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        _logger.debug(df.TickerTime[0])
        df.TickerTime = pd.DatetimeIndex(df.TickerTime).tz_localize('UTC')
        df_source.TickerTime = df_source.TickerTime.apply(pd.Timestamp)
        _logger.debug(df.iloc[0])
        assert_frame_equal(df, df_source)

    def test_query_hist_data(self):
        async def run(loop, query_parms, blk):
            engine = await aiosa.create_engine(
                user=self.db_info['user'], db=self.db_info['db'],
                host=self.db_info['host'], password=self.db_info['password'],
                loop=loop)
            # Insert and Query
            await insert_hist_data(engine, query_parms[0], blk)
            blk = await query_hist_data(engine, *query_parms)
            engine.close()
            await engine.wait_closed()
            return blk

        # Execute and verify query
        self._clear_db()
        init_db(self.db_info)
        blk_source = MarketDataBlock(testdata_query_hist_data[0])
        query_parms = testdata_query_hist_data[1]
        loop = asyncio.get_event_loop()
        blk = loop.run_until_complete(run(loop, query_parms, blk_source))
        assert_frame_equal(blk.df, blk_source.df.loc(axis=0)[
            :, :, :, query_parms[-2]:query_parms[-1]])

    def test_download_insert_hist_data(self):
        async def run(loop, req, broker, insert_limit):
            engine = await aiosa.create_engine(
                user=self.db_info['user'], db=self.db_info['db'],
                host=self.db_info['host'], password=self.db_info['password'],
                loop=loop)
            # Download, Insert and Query
            dl_blk = await download_insert_hist_data(
                req, broker, engine, insert_limit)
            db_blk = await query_hist_data(
                engine, req.SecType, req.Symbol, req.DataType, req.BarSize,
                *insert_limit)
            engine.close()
            await engine.wait_closed()
            return dl_blk, db_blk

        # Execute
        self._clear_db()
        init_db(self.db_info)
        req = testdata_download_insert_hist_data['req']
        broker, login = testdata_download_insert_hist_data['broker']
        insert_limit = testdata_download_insert_hist_data['insert_limit']
        broker.connect(*login)
        loop = asyncio.get_event_loop()
        dl_blk, db_blk = loop.run_until_complete(
            run(loop, req, broker, insert_limit))
        broker_blk = broker.req_hist_data(req)[0]

        # Verify
        lim0 = insert_limit[0]
        lim1 = insert_limit[1]
        assert_frame_equal(dl_blk.df, broker_blk.df)
        assert_frame_equal(db_blk.df,
                           broker_blk.df.loc(axis=0)[:, :, :, lim0:lim1])

    def test_hist_data_req_start_end(self):
        for req, s, e in testdata_req_start_end:
            xchg_tz = req.TimeEnd.tzinfo
            start, end, _ = hist_data_req_start_end(req, xchg_tz)
            _logger.info('req_start_end: %s', req)
            _logger.info('req_start_end: %s - %s', start, end)
            self.assertEqual(start, s)
            self.assertEqual(end, e)

    def test_query_hist_data_split_req(self):
        async def run(loop, blk, req, xchg_tz):
            engine = await aiosa.create_engine(
                user=self.db_info['user'], db=self.db_info['db'],
                host=self.db_info['host'], password=self.db_info['password'],
                loop=loop)
            # Populate database
            await insert_hist_data(engine, req.SecType, blk)
            # Filter req
            results = await query_hist_data_split_req(req, xchg_tz, engine)
            engine.close()
            await engine.wait_closed()
            return results

        for data in testdata_query_hist_data_split_req:
            # Prepare database
            self._clear_db()
            init_db(self.db_info)
            # Run target
            xchg_tz = data['start_dt'].tzinfo
            blk_db = MarketDataBlock(data['df_db'])
            loop = asyncio.get_event_loop()
            (dl_reqs, insert_limit, blk_db_ret,
             start_dt, end_dt) = loop.run_until_complete(
                 run(loop, blk_db, data['req'], xchg_tz))
            _logger.debug('\ndl_reqs: %s', dl_reqs)
            _logger.debug('\ninsert_limit: %s', insert_limit)
            _logger.debug('\nstart_dt: %s', start_dt)
            _logger.debug('\nend_dt: %s', end_dt)
            # Verify
            self.assertEqual(dl_reqs, data['dl_reqs'])
            self.assertEqual(insert_limit, data['insert_limit'])
            blk_db.tz = xchg_tz
            assert_frame_equal(blk_db_ret.df, blk_db.df)
            self.assertEqual(start_dt, data['start_dt'])
            self.assertEqual(end_dt, data['end_dt'])

    def test_get_hist_data(self):
        async def run(loop, req, blk_db, broker):
            # Populate database
            engine = await aiosa.create_engine(
                user=self.db_info['user'], db=self.db_info['db'],
                host=self.db_info['host'], password=self.db_info['password'],
                loop=loop, echo=False)
            await insert_hist_data(engine, 'Stock', blk_db)
            engine.close()
            await engine.wait_closed()
            # Get hist data
            blk_db = await get_hist_data(
                req, broker, mysql={**self.db_info, 'loop': loop})
            return blk_db

        from time import sleep
        for data in testdata_get_hist_data:
            sleep(1.5)  # Avoid IB pacing violation
            _logger.debug("\n======= get_hist_data_async: %s ======\n",
                          data['testcase'])
            self._clear_db()
            init_db(self.db_info)
            blk_db = MarketDataBlock(data['df_db'])
            broker = data['broker'][0](*data['broker'][1])
            blk_exp = MarketDataBlock(data['blk_exp.df'])
            blk_exp.tz = data['xchg_tz']
            loop = asyncio.get_event_loop()
            blk_ret = loop.run_until_complete(
                run(loop, data['req'], blk_db, broker))
            assert_frame_equal(blk_ret.df, blk_exp.df)


class RealTimeDataStreamingTests(unittest.TestCase):
    """
    Test cases for real-time market data streaming.
    """
    def test_stream_stk(self):
        pass


if __name__ == '__main__':
    unittest.main()
