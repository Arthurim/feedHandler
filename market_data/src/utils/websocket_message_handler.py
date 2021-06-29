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

from core.src.sym_handler import format_sym_for_market, is_spot_market_ticker, is_future_market_ticker


def create_wss_connection(subscription_type, market, sym):
    """
    Creates the websocket connection for a given market,sym, subscription type

    :param subscription_type: str
    :param market: str
    :param sym: str
    :return: websocket
    """
    sym = format_sym_for_market(sym, market)
    if market == "KRAKEN":
        if is_spot_market_ticker(sym, market):
            ws = create_connection("wss://ws.kraken.com")
            if subscription_type == "orderbooks":
                ws.send(
                    json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "book", "depth": 10}}))
            elif subscription_type == "trades":
                ws.send(json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "trade"}}))
            elif subscription_type == "ohlcs":
                ws.send(
                    json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "ohlc", "interval": 5}}))
            elif subscription_type == "spreads":
                ws.send(json.dumps({"event": "subscribe", "pair": [sym], "subscription": {"name": "spread"}}))
            else:
                raise ValueError(
                    "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
        elif is_future_market_ticker(sym, market):
            ws = create_connection("wss://futures.kraken.com/ws/v1")
            if subscription_type == "orderbooks":
                ws.send(json.dumps({"event": "subscribe", "product_ids": [sym], "feed": "book"}))
            elif subscription_type == "trades":
                ws.send(json.dumps({"event": "subscribe", "product_ids": [sym], "feed": "trade"}))
            else:
                raise ValueError(
                    "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
        else:
            raise ValueError("Instrument type not supported for:" + sym)
    elif market == "BINANCE":
        if is_spot_market_ticker(sym, market):
            ws_url = "wss://stream.binance.com:9443/ws/"
        elif is_future_market_ticker(sym, market):
            ws_url = "wss://dstream.binance.com/ws/"  # +'stream?streams='
        else:
            raise ValueError("Instrument not supported:", sym)

        if subscription_type == "orderbooks":
            ws = create_connection(ws_url + sym + "@depth10")
        elif subscription_type == "trades":
            ws = create_connection(ws_url + sym + "@trade")
        else:
            raise ValueError(
                "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
    elif market == "BITFINEX":
        ws = create_connection("wss://api.bitfinex.com/ws/2")
        if subscription_type == "orderbooks":
            ws.send(json.dumps(
                {"event": "subscribe", "channel": "book", "prec": "P0", "length": "10", "symbol": "t" + sym}))
        elif subscription_type == "trades":
            ws.send(json.dumps(
                {"event": "subscribe", "channel": "trades", "symbol": "t" + sym}))
        else:
            raise ValueError(
                "This subscription_type is not yet supported: " + str(subscription_type) + " for market: " + market)
    elif market == "BITMEX":
        ws = create_connection("wss://www.bitmex.com/realtime")
        if subscription_type == "orderbooks":
            ws.send(json.dumps({"op": "subscribe", "args": ["orderBook10:" + sym]}))
        elif subscription_type == "trades":
            ws.send(json.dumps({"op": "subscribe", "args": ["trade:" + sym]}))
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
    elif market == "HUOBI":
        if is_spot_market_ticker(sym, market):
            ws = create_connection("wss://api.huobi.pro/ws")
        elif is_future_market_ticker(sym, market):
            ws = create_connection("wss://api.hbdm.com/ws")
        else:
            raise ValueError("Instrument not supported:", sym)
        if subscription_type == "orderbooks":
            ws.send(json.dumps({"sub": "market." + sym + ".depth.step0", "id": "id1"}))
        elif subscription_type == "trades":
            ws.send(json.dumps({"sub": "market." + sym + ".trade.detail", "id": "id1"}))
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
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - (%(lineno)d) - %(message)s')
    # TODO: exctract path as a global variable ?
    logFile = 'C:/dev/log/marketdata/log_' + subscription_type + '_' + market + "_" + sym.replace("/",
                                                                                                  "_") + "_" + datetime.datetime.now().strftime(
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


def is_ping_WS_result(result):
    if type(result) == dict:
        return "ping" in result.keys()
    else:
        return False


def is_info_WS_result(result):
    if type(result) == dict:
        if "info" in result.keys():
            return True
        if "type" in result.keys():
            return result["type"] == "subscriptions"
    return False


def is_not_huobi_tick_result(result, market):
    if market == "HUOBI":
        return "tick" not in result.keys()
    else:
        return False


def is_correct_subscription_message_bitmex(result, market):
    if market == "BITMEX":
        if "success" in result.keys():
            return result["success"]
    return False
