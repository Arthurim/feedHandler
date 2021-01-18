#!/usr/bin/env python3
"""
@author: Arthurim
@Description: various utils for persistence
"""
import gzip
import json
import logging

from qpython import qconnection

from .websocket_message_handler import is_ping_WS_result


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
    Persist a new row to TP

    :param row: string, kdb row to persist; a row is a string of ; separated elements to insert into the kdb table, the first two elements being time and sym
    :param table: string, name of the kdb table to persist the row to
    :param host: string, host of the kdb
    :param port: int, port of the kdb TP
    :return:
    """
    app_log = logging.getLogger('root')
    with qconnection.QConnection(host, port, "Quser", "passQtoday", True) as q:
        # TODO check that the timingId is not already persisted to kdb
        try:
            X = q('.u.upd[`' + table + ';(' + row + ')]')
        except Exception as error:
            app_log.error("Persisting " + table + " table for " + get_sym_from_row(row) + " on " + get_market_from_row(
                row) + ' - Caught this error: ' + repr(error))


def get_args_for_subscription(subscription_type, sym, market):
    """
    Returns the useful variables to use for a given type of persistence, for now only a dictionary representing
    the orderbook for orderbooks subscription_type

    :param subscription_type: string
    :param sym: string
    :param market: string
    :return: various
    """
    if subscription_type == "orderbooks":
        arg = {"sym": sym, "market": market, "marketTimestamp": {}, "bid": {}, "ask": {}}
    elif subscription_type == "trades":
        arg = market
    elif subscription_type == "ohlcs":
        arg = ""
    elif subscription_type == "spreads":
        arg = ""
    else:
        raise ValueError("This subscription_type is not yet supported: " + str(subscription_type))
    return arg


def get_ws_result(ws, market):
    """
    Depending on the market the result from the WS connection might need to be unzipped or not

    :param ws: handle to ws connection
    :param market: str, market
    :return:
    """
    if market == 'HUOBI':
        result = gzip.decompress(ws.recv()).decode("utf-8")
        result = json.loads(result)
        if is_ping_WS_result(result):
            ws.send(json.dumps({"pong": result["ping"]}))
    else:
        result = ws.recv()
        # TODO handle coonection messages, heartbeats and disconnections
        result = json.loads(result)
    result = handle_ws_errors(ws, result)
    return result


def handle_ws_errors(ws, result):
    """
    Handles the errors from WS

    :param ws: websocket handle,
    :param result: dict,
    :return:
    """
    error = ""
    app_log = logging.getLogger('root')
    if type(result) == dict:
        if "error" in result.keys():
            error = result["error"]
        elif "errors" in result.keys():
            error = result["errors"]
    if error != "":
        app_log.error("Closing WS connection to ", ws.url, "Received the following error:", error)
        ws.close()
        raise ConnectionError("WS received the following error:", error)
    else:
        return result
