#!/usr/bin/env python3
"""
@author: Arthurim
@Description: information relative to markets
"""

SUPPORTED_MARKETS = ["KRAKEN", "BINANCE", "BITMEX", "BITFINEX", "COINBASE", "HUOBI"]

# TODO add supported pairs per market?
supported_pairs: {
    "KRAKEN": ["XBTUSD", "ETHUSD", "FI_XBTUSD", "FI_ETHUSD"],
    "BiNANCE": ["XBTUSD", "ETHUSD", "FI_XBTUSD", "FI_ETHUSD"]

}

def is_supported_market(market):
    return market in SUPPORTED_MARKETS
