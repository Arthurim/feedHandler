#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import unittest

from market_data.src.rest_api_traded_pairs import persist_traded_pairs

DEBUG_TIME = 1


class TestShouldGetAllTradedTickersFromRESTAPI(unittest.TestCase):

    def test_persistence_tickers(self):
        tickers = persist_traded_pairs(["KRAKEN"])
        self.assertTrue(len(tickers) > 0)


if __name__ == "__main__":
    unittest.main()
    print("Everything passed")
