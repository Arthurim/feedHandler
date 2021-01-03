#!/usr/bin/env python3
"""
@author: Arthurim
@Description: information relative to markets
"""

# TODO Add COINBASE
SUPPORTED_MARKETS = ["KRAKEN", "BINANCE", "BITMEX", "BITFINEX", "COINBASE"]


def is_supported_market(market):
    return market in SUPPORTED_MARKETS
