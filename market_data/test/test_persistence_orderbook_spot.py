#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import os
import unittest

from market_data.src.persistence import create_ws_subscription_kdb_persister_debug

DEBUG_TIME = 1


# TODO Add should Have Successful connection
class TestShouldPersistAllMarketsOrderbooksForXBTUSDWithoutError(unittest.TestCase):

    def test_persistence_orderbooks_kraken_xbtusd(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="orderbooks",
                                                              sym="XBTUSD",
                                                              market="KRAKEN",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_orderbooks_binance_xbtusdt(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="orderbooks",
                                                              sym="XBTUSDT",
                                                              market="BINANCE",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_orderbooks_coinbase_xbtusd(self):
        # 2021-01-08 23:04:55,511 - ERROR - create_ws_subscription_kdb_persister_debug - (140) - WS orderbooks subcscription for XBTUSD on COINBASE - Caught this error: OSError(10048, 'Only one usage of each socket address (protocol/network address/port) is normally permitted', None, 10048, None)
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="orderbooks",
                                                              sym="XBTUSD",
                                                              market="COINBASE",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_orderbooks_bitmex_xbtusd(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="orderbooks",
                                                              sym="XBTUSD",
                                                              market="BITMEX",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_orderbooks_bitfinex_xbtusd(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="orderbooks",
                                                              sym="XBTUSD",
                                                              market="BITFINEX",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)

    def test_persistence_orderbooks_huobi_xbtusdt(self):
        log_file = create_ws_subscription_kdb_persister_debug(subscription_type="orderbooks",
                                                              sym="XBTUSDT",
                                                              market="HUOBI",
                                                              debug_time=DEBUG_TIME)
        f = open(os.path.join(log_file), "r")
        self.assertTrue(" - ERROR - " not in f.read(), "Persistence failed, look at log:" + log_file)


if __name__ == "__main__":
    unittest.main()
    print("Everything passed")
