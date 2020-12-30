#!/usr/bin/env python3
"""
@author: Arthurim
@Description: functions to handle the creation of a ohlc data from the ECNs APIs and to persist it to kdb
"""
import datetime
import logging

import pandas as pd

from .constants.kdb_hosts import MARKET_DATA_KDB_HOST, MARKET_DATA_TP
from .utils.persistence_utils import persist_row_to_table
from .utils.python_to_kdb_conversion import convert_ohlc_series_to_kdb_row


def persist_ohlc_to_kdb(result):
    """
    Persists the API result to the ohlcs table

    :param result: dictionary, the ohlc result from the API
    """
    ohlc = result[1]
    app_log = logging.getLogger('root')
    app_log.info("Persisting OHLC to kdb")
    new_row = pd.Series({"time":datetime.datetime.now().strftime("%H:%M:%S.%f"),
                         "sym":result[3],
                         "gatewayTimestamp":datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                         "marketTimestamp":datetime.datetime.fromtimestamp(float(ohlc[0])).strftime("%Y.%m.%dD%H:%M:%S.%f"),
                         "market":"KRAKEN",
                         "endTime":datetime.datetime.fromtimestamp(float(ohlc[1])).strftime("%Y.%m.%dD%H:%M:%S.%f"),
                         "interval":int(result[2].split("-")[1]),
                         "open":float(ohlc[2]),
                         "high":float(ohlc[3]),
                         "low":float(ohlc[4]),
                         "close":float(ohlc[5]),
                         "vwap":float(ohlc[6]),
                         "totalLhsFlow":float(ohlc[7]),
                         "tradeCount":int(ohlc[8])
                        })
    kdb_row = convert_ohlc_series_to_kdb_row(new_row)
    persist_row_to_table(kdb_row, "ohlcs", MARKET_DATA_KDB_HOST, MARKET_DATA_TP)
