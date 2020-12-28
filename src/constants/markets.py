#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""

SUPPORTED_MARKETS = ["KRAKEN", "BINANCE", "BITMEX", "BITFINEX"]


def is_supported_market(market):
    return market in SUPPORTED_MARKETS
