#!/usr/bin/env python3
"""
@author: Arthurim
@Description: Various functions to handle creation of webscoket connections, logging, interpretation of WS results
"""
import datetime
import json
import logging
from logging.handlers import RotatingFileHandler

from websocket import create_connection

from .sym_handler import get_sym_format


def create_wss_connection(subscription_type, market, sym):
    """
    Creates the websocket connection for a given market,sym, subscription type

    :param subscription_type: str
    :param market: str
    :param sym: str
    :return: websocket
    """
    sym = get_sym_format(sym, market)
    if market == "KRAKEN":
        ws = create_connection("wss://ws.kraken.com")
        if subscription_type == "orderbooks":
            ws.send(json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "book", "depth": 10}}))
        elif subscription_type == "trades":
            ws.send(json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "trade"}}))
        elif subscription_type == "ohlcs":
            ws.send(json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "ohlc", "interval": 5}}))
        elif subscription_type == "spreads":
            ws.send(json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "spread"}}))
        else:
            raise ValueError(
                "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
    elif market == "BINANCE":
        if subscription_type == "orderbooks":
            ws = create_connection("wss://stream.binance.com:9443/ws/" + sym + "@depth10")
        elif subscription_type == "trades":
            ws = create_connection("wss://stream.binance.com:9443/ws/" + sym + "@trade")
        else:
            raise ValueError(
                "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
    elif market == "BITFINEX":
        ws = create_connection("wss://api-pub.bitfinex.com/ws/2")
        if subscription_type == "orderbooks":
            ws.send(json.dumps({"event": "subscribe", "channel": "book", "prec": "R0", "symbol": "t" + sym}))
        else:
            raise ValueError(
                "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
    elif market == "BITMEX":
        ws = create_connection("wss://www.bitmex.com/realtime")
        if subscription_type == "orderbooks":
            ws.send(json.dumps({"op": "subscribe", "args": ["orderBook10:" + sym]}))
        else:
            raise ValueError(
                "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
    elif market == "COINBASE":
        ws = create_connection("wss://ws-feed.pro.coinbase.com")
        if subscription_type == "orderbooks":
            ws.send(json.dumps({"type": "subscribe", "product_ids": [sym], "channels": ["level2"]}))
        elif subscription_type == "trades":
            ws.send(json.dumps({"type": "subscribe", "product_ids": [sym], "channels": ["matches"]}))
        else:
            raise ValueError(
                "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
    else:
        raise ValueError("Market not supported: " + market)
    return ws


def create_ws_subscription_logger(subscription_type, sym, market):
    """
    Creates a logger to the log folder
    :param subscription_type: str
    :param sym: str
    :param market: str
    :return: logger
    """
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s (%(lineno)d) %(message)s')
    # TODO: exctract path as a global variable ?
    logFile = 'C:/dev/log/marketdata/log_' + subscription_type + '_' + market + "_" + sym + "_" + datetime.datetime.now().strftime(
        "%Y-%m-%d-%H-%M-%S-%f")
    my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5 * 1024 * 1024,
                                     backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    my_handler.setLevel(logging.INFO)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)
    return app_log


def is_error_WS_result(result):
    if type(result) == dict:
        return "error" in result.keys()
    else:
        return False


def is_event_WS_result(result):
    if type(result) == dict:
        return "event" in result.keys()
    else:
        return False


def is_info_WS_result(result):
    if type(result) == dict:
        if "info" in result.keys():
            return True
        if "type" in result.keys():
            return result["type"] == "subscriptions"
    return False
