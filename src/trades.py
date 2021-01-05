#!/usr/bin/env python3
"""
@author: Arthurim
@Description: functions to handle the creation of a trade table from the ECNs APIs and to persist it to kdb
"""
import datetime
import logging

import pandas as pd

from .constants.kdb_hosts import MARKET_DATA_KDB_HOST, MARKET_DATA_TP
from .utils.persistence_utils import persist_row_to_table
from .utils.python_to_kdb_conversion import convert_trades_series_to_kdb_row


def get_data_from_trades_result(result, market):
    rows = []
    if market == "KRAKEN":
        for trade in result[1]:
            row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                             "sym": result[3],
                             "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                             "tradeTimestamp": datetime.datetime.fromtimestamp(float(trade[2])).strftime(
                                 "%Y.%m.%dD%H:%M:%S.%f"),
                             "market": market,
                             "tradeId": "",
                             "side": str(trade[3]),
                             "price": float(trade[0]),
                             "lhsFlow": float(trade[1]),
                             "rhsFlow": float(trade[0]) * float(trade[1]),
                             "orderType": str(trade[4]),
                             "misc": str('""' if trade[5] == '' else trade[5])
                             })
            rows.append(row)
    elif market == "BINANCE":
        row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                         "sym": result["s"],
                         "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                         "tradeTimestamp": datetime.datetime.fromtimestamp(float(result["T"]) / 1e3).strftime(
                             "%Y.%m.%dD%H:%M:%S.%f"),
                         "market": market,
                         "tradeId": result["t"],
                         "side": "sell" if result["m"] else "buy",
                         "price": float(result["p"]),
                         "lhsFlow": float(result["q"]),
                         "rhsFlow": float(result["p"]) * float(result["q"]),
                         "orderType": "",
                         "misc": ""
                         })
        rows.append(row)
    return rows


def persist_trades_to_kdb(result, market):
    """
    Persists the trades result of the Webscoket API to Kdb

    :param market: string
    :param result: a dictionary containing the trades result from API call
    :return:
    """
    app_log = logging.getLogger('root')
    # app_log.info("Persisting #" + str(len(result[1])) + " trades to kdb")
    new_rows = get_data_from_trades_result(result, market)
    for new_row in new_rows:
        kdb_row = convert_trades_series_to_kdb_row(new_row)
        persist_row_to_table(kdb_row, "trades", MARKET_DATA_KDB_HOST, MARKET_DATA_TP)
