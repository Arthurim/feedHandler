#!/usr/bin/env python3
"""
@author: Arthurim
@Description: functions to handle the creation of a spread table from the ECNs APIs and to persist it to kdb
"""
import datetime
import logging

import pandas as pd

from market_data.src.constants.kdb_hosts import MARKET_DATA_KDB_HOST, MARKET_DATA_TP
from market_data.src.utils.persistence_utils import persist_row_to_table
from market_data.src.utils.python_to_kdb_conversion import convert_spread_to_kdb_row


def persist_spread_to_kdb(result):
    """
    Persists the spread result of the Webscoket API to Kdb

    :param result: a dictionary containing the spread result from API call
    :return:
    """
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
