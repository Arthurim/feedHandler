#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import datetime
import json
import logging
from logging.handlers import RotatingFileHandler

from websocket import create_connection


def create_wss_connection_url_for_market(subscription_type, market, sym):
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
    else:
        raise ValueError("Market not supported: " + market)
    return ws


def create_ws_subscription_logger(subscription_type, sym, market):
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    logFile = 'C:/dev/log/marketdata/log_' + subscription_type + '_' + market + "_" + datetime.datetime.now().strftime(
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
        return "info" in result.keys()
    else:
        return False
