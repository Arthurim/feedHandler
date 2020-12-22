#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import datetime

from qpython import qconnection

from ohlcs import persist_ohlc_to_kdb
from orderbooks import persist_orderbook_to_kdb
from pythonToKdbConversion import convert_spread_to_kdb_row, convert_trades_series_to_kdb_row
import pandas as pd

from spreads import persist_spread_to_kdb
from trades import persist_trades_to_kdb


def get_market_from_row(row):
    """
    returns the market of a kdb row
    a row is a string of ; separated elements to insert into the kdb table, the first two elements being time and sym
    """
    all_markets = ["KRAKEN", "BITFINEX"]
    for market in all_markets:
        if market in row:
            return market


def get_sym_from_row(row):
    """
    returns the sym of a kdb row
    a row is a string of ; separated elements to insert into the kdb table, the first two elements being time and sym
    """
    return row.split(";")[1].replace("`", "")


def persist_row_to_table(row, table, host, port):
    """
    Persist a row to a kdb table on the host port
    a row is a string of ; separated elements to insert into the kdb table, the first two elements being time and sym
    """
    with qconnection.QConnection(host, port, "Quser", "passQtoday", True) as q:
        # TODO check that the timingId is not already persisted to kdb
        try:
            X = q('.u.upd[`' + table + ';(' + row + ')]')
        except Exception as error:
            app_log.error("Persisting " + table + " table for " + get_sym_from_row(row) + " on " + get_market_from_row(
                row) + ' - Caught this error: ' + repr(error))


def insert_trades_new_row_to_kdb(new_row):
    """
    Updates kdb table quotestackevents with the new row
    """
    kdb_row = convert_trades_series_to_kdb_row(new_row)
    with qconnection.QConnection(host='localhost', port=5000, username="Quser", password="passQtoday",
                                 pandas=True) as q:
        # TODO check that the timingId is not already persisted to kdb
        try:
            X = q('.u.upd[`trades;(' + kdb_row + ')]')
        except Exception as error:
            app_log.error("Persisting Trades for " + new_row["sym"] + " on " + new_row[
                "market"] + ' - Caught this error: ' + repr(error))
    return kdb_row


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


def get_args_for_subscription(subscription_type, sym, market):
    if subscription_type == "orderbooks":
        arg = {"sym": sym, "market": market, "marketTimestamp": {}, "bid": {}, "ask": {}}
    elif subscription_type == "trades":
        arg = ""
    elif subscription_type == "ohlcs":
        arg = ""
    elif subscription_type == "spreads":
        arg = ""
    else:
        raise ValueError("This subscription_type is not yet supported: " + str(subscription_type))
    return arg
