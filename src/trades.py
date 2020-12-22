#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import pandas as pd
import datetime

from persistence import persist_row_to_table
from pythonToKdbConversion import convert_trades_series_to_kdb_row


def persist_trades_to_kdb(result):
    app_log.info("Persisting #" + str(len(result[1])) + " trades to kdb")
    for trade in result[1]:
        new_row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                             "sym": result[3],
                             "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                             "tradeTimestamp": datetime.datetime.fromtimestamp(float(trade[2])).strftime(
                                 "%Y.%m.%dD%H:%M:%S.%f"),
                             "market": "KRAKEN",
                             "tradeId": "",
                             "side": str(trade[3]),
                             "price": float(trade[0]),
                             "lhsFlow": float(trade[1]),
                             "rhsFlow": float(trade[0]) * float(trade[1]),
                             "orderType": str(trade[4]),
                             "misc": str('""' if trade[5] == '' else trade[5])
                             })
        kdb_row = convert_trades_series_to_kdb_row(new_row)
        persist_row_to_table(kdb_row, "trades", "localhost", 5000)
