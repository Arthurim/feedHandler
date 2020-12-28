#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import datetime
import logging

import pandas as pd

from .constants.kdb_hosts import MARKET_DATA_KDB_HOST, MARKET_DATA_TP
from .utils.persistence_utils import persist_row_to_table
from .utils.python_to_kdb_conversion import convert_spread_to_kdb_row


def persist_spread_to_kdb(result):
    spread = result[1]
    app_log = logging.getLogger('root')
    app_log.info("Persisting spread to kdb")
    new_row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                         "sym": result[3],
                         "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                         "marketTimestamp": datetime.datetime.fromtimestamp(float(spread[2])).strftime(
                             "%Y.%m.%dD%H:%M:%S.%f"),
                         "market": "KRAKEN",
                         "bidPrice": float(spread[0]),
                         "bidSize": float(spread[3]),
                         "offerPrice": float(spread[1]),
                         "offerSize": float(spread[4])
                         })
    kdb_row = convert_spread_to_kdb_row(new_row)
    persist_row_to_table(kdb_row, "spreads", MARKET_DATA_KDB_HOST, MARKET_DATA_TP)
