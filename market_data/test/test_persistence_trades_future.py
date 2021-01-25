#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import os
import unittest

from market_data.src.persistence import create_ws_subscription_kdb_persister_debug

DEBUG_TIME = 2


# TODO Add should Have Successful connection
class TestShouldPersistAllMarketstradesForXBTUSDFuture210129WithoutError(unittest.TestCase):

    def test_persistence_trades_kraken_xbtusd_future(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="trades",
                                                              sym="XBTUSD/Future/210129",
                                                              market="KRAKEN",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_trades_binance_xbtusd_future(self):
        # hangs forever
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="trades",
                                                              sym="XBTUSD/Future/210326",
                                                              market="BINANCE",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_trades_bitmex_xbtusd_future(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="trades",
                                                              sym="XBTUSD/Future/210326",
                                                              market="BITMEX",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_trades_huobi_xbtusd_future(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="trades",
                                                              sym="XBTUSD/Future/210326",
                                                              market="HUOBI",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)


if __name__ == "__main__":
    unittest.main()
    print("Everything passed")
