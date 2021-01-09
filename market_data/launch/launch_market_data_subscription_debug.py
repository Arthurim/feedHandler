#!/usr/bin/env python3
# @author: Arthurim
# @Description: Launches the market data subscription for X minutes
#               This writes to the TP (5000)
#               subscription_type is one of the following: orderbooks, ohlcs, trades, spreads
#               market is one of the following: KRAKEN, BINANCE, BITMEX, BITFINEX

import sys

from market_data.src.persistence import create_ws_subscription_kdb_persister_debug

create_ws_subscription_kdb_persister_debug(subscription_type=sys.argv[1],
                                           sym=sys.argv[2],
                                           market=sys.argv[3],
                                           debug_time=int(sys.argv[4]))
