#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
from .ohlcs import persist_ohlc_to_kdb
from .orderbooks import persist_orderbook_to_kdb
from .spreads import persist_spread_to_kdb
from .trades import persist_trades_to_kdb


def persist_subscription_result_to_kdb(result, subscription_type, arg=""):
    if subscription_type == "orderbooks":
        arg = persist_orderbook_to_kdb(arg, result)
    elif subscription_type == "trades":
        persist_trades_to_kdb(result)
    elif subscription_type == "ohlcs":
        persist_ohlc_to_kdb(result)
    elif subscription_type == "spreads":
        persist_spread_to_kdb(result)
    else:
        raise ValueError("This subscription_type is not yet supported: " + str(subscription_type))
    return arg
