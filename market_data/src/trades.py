#!/usr/bin/env python3
"""
@author: Arthurim
@Description: functions to handle the creation of a trade table from the ECNs APIs and to persist it to kdb
"""
import datetime

import pandas as pd

from market_data.src.constants.kdb_hosts import MARKET_DATA_KDB_HOST, MARKET_DATA_TP
from market_data.src.utils.persistence_utils import persist_row_to_table
from market_data.src.utils.python_to_kdb_conversion import convert_trades_series_to_kdb_row
from market_data.src.utils.sym_handler import is_spot, is_future


def hormonise_side(s):
    if s in ["sell", "s", "ask", "offer"]:
        return "s"
    elif s in ["buy", "bid", "b"]:
        return "b"
    else:
        raise ValueError("Unknown side for trade:", str(s))


def get_data_from_trades_result(result, sym, market):
    rows = []
    if market == "KRAKEN":
        if is_spot(sym):
            for trade in result[1]:
                row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                                 "sym": sym,
                                 "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                                 "tradeTimestamp": datetime.datetime.fromtimestamp(float(trade[2])).strftime(
                                     "%Y.%m.%dD%H:%M:%S.%f"),
                                 "market": market,
                                 "tradeId": "",
                                 "side": hormonise_side(str(trade[3])),
                                 "price": float(trade[0]),
                                 "lhsFlow": float(trade[1]),
                                 "rhsFlow": float(trade[0]) * float(trade[1]),
                                 "orderType": str(trade[4]),
                                 "misc": str('""' if trade[5] == '' else trade[5])
                                 })
                rows.append(row)
        elif is_future(sym):
            if "trades" in result.keys():
                for trade in result["trades"]:
                    row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                                     "sym": sym,
                                     "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                                     "tradeTimestamp": datetime.datetime.fromtimestamp(
                                         float(trade["time"]) / 1e3).strftime(
                                         "%Y.%m.%dD%H:%M:%S.%f"),
                                     "market": market,
                                     "tradeId": str(trade["uid"]),
                                     "side": hormonise_side(str(trade["side"])),
                                     "price": float(trade["price"]),
                                     "lhsFlow": float(trade["qty"]),
                                     "rhsFlow": float(trade["price"]) * float(trade["qty"]),
                                     "orderType": "",
                                     "misc": str('""' if str(trade["type"]) == '' else str(trade["type"]))
                                     })
                    rows.append(row)
    elif market == "BINANCE":
        row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                         "sym": sym,
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
    elif market == "BITMEX":
        if len(result["data"]) > 0:
            for trade in result["data"]:
                row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                                 "sym": sym,
                                 "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                                 "tradeTimestamp": datetime.datetime.strptime(trade["timestamp"],
                                                                              '%Y-%m-%dT%H:%M:%S.%fZ').strftime(
                                     "%Y.%m.%dD%H:%M:%S.%f"),
                                 "market": market,
                                 "tradeId": str(trade["trdMatchID"]),
                                 "side": str(trade["side"]),
                                 "price": float(trade["price"]),
                                 "lhsFlow": float(trade["size"]),
                                 "rhsFlow": float(trade["size"]) * float(trade["price"]),
                                 "orderType": "",
                                 "misc": ""
                                 })
                rows.append(row)
    elif market == "BITFINEX":
        for trade in result[1]:
            row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                             "sym": sym,
                             "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                             "tradeTimestamp": datetime.datetime.fromtimestamp(trade[1] / 1e3).strftime(
                                 "%Y.%m.%dD%H:%M:%S.%f"),
                             "market": market,
                             "tradeId": trade[0],
                             "side": get_side_from_lhsFlow(trade[3]),
                             "price": float(trade[2]),
                             "lhsFlow": float(trade[3]),
                             "rhsFlow": float(trade[2]) * float(trade[3]),
                             "orderType": "",
                             "misc": ""
                             })
            rows.append(row)
    elif market == "COINBASE":
        row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                         "sym": sym,
                         "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                         "tradeTimestamp": datetime.datetime.strptime(result["time"],
                                                                      '%Y-%m-%dT%H:%M:%S.%fZ').strftime(
                             "%Y.%m.%dD%H:%M:%S.%f"),
                         "market": market,
                         "tradeId": result["trade_id"],
                         "side": result["side"],
                         "price": float(result["price"]),
                         "lhsFlow": float(result["size"]),
                         "rhsFlow": float(result["price"]) * float(result["size"]),
                         "orderType": "",
                         "misc": ""
                         })
        rows.append(row)
    elif market == "HUOBI":
        trades = result["tick"]["data"]
        for result in trades:
            if is_spot(sym):
                trade_id = result["tradeId"]
            elif is_future(sym):
                trade_id = result["id"]
            else:
                raise ValueError("Instrument not supported:", sym)
            row = pd.Series({"time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                             "sym": sym,
                             "gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                             "tradeTimestamp": datetime.datetime.fromtimestamp(float(result["ts"]) / 1e3).strftime(
                                 "%Y.%m.%dD%H:%M:%S.%f"),
                             "market": market,
                             "tradeId": trade_id,
                             "side": result["direction"],
                             "price": float(result["price"]),
                             "lhsFlow": float(result["amount"]),
                             "rhsFlow": float(result["price"]) * float(result["amount"]),
                             "orderType": "",
                             "misc": ""
                             })
            rows.append(row)

    else:
        raise ValueError("Trades subscription for market:" + market + " not supported.")
    return rows


def persist_trades_to_kdb(result, sym_market):
    """
    Persists the trades result of the Webscoket API to Kdb

    :param sym_market: dict, 2 keys: sym and market
    :param result: a dictionary containing the trades result from API call
    :return:
    """
    new_rows = get_data_from_trades_result(result, sym_market["sym"], sym_market["market"])
    for new_row in new_rows:
        kdb_row = convert_trades_series_to_kdb_row(new_row)
        persist_row_to_table(kdb_row, "trades", MARKET_DATA_KDB_HOST, MARKET_DATA_TP)


def get_side_from_lhsFlow(lhsFlow):
    if lhsFlow > 0:
        return "buy"
    else:
        return "sell"
