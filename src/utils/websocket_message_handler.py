#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import datetime
import json
import logging
import time
from logging.handlers import RotatingFileHandler

from websocket import create_connection

from .persistence_utils import get_args_for_subscription
from ..persistence import persist_subscription_result_to_kdb


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


def create_ws_subscription_kdb_persister(subscription_type, sym, market, depth=10):
    """
    Creates a WS subscription subscription_type for a given market and instrument
    subscription_type can be: trades, orderbooks,
    sym: XBTUSD etc
    market: only KRAKEN is supported for now
    """
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    app_log = create_ws_subscription_logger(subscription_type, sym, market)
    app_log.info('Creating a WS' + subscription_type + ' subcscription for ' + sym + " on " + market)
    try:
        ws = create_wss_connection_url_for_market(subscription_type, market, sym)
        app_log.info('WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " is successful")
    except Exception as error:
        app_log.error(
            'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + "- Caught the following error:\n" + repr(
                error))
        time.sleep(3)
        raise ConnectionError(
            "Stopping WS " + subscription_type + ' subcscription for ' + sym + " on " + market + " - Subscription failed")

        # TODO: check that sym is valid
    arg = get_args_for_subscription(subscription_type, sym, market)

    while True:
        try:
            result = ws.recv()
            # TODO handle heartbeats
            result = json.loads(result)
            app_log.info(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " - Received  '%s'" % result)
            if not (is_event_WS_result(result) or is_info_WS_result(result)):
                arg = persist_subscription_result_to_kdb(result, subscription_type, arg)
        except Exception as error:
            app_log.error(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + ' - create_ws_subscription_orderbook - Caught this error: ' + repr(
                    error))
            time.sleep(3)


def create_ws_subscription_kdb_persister_debug(subscription_type, sym, market, debug_time=1):
    """
    Creates a WS subscription subscription_type for a given market and instrument for debug_time minutes
    subscription_type can be: trades, orderbooks,
    sym: XBTUSD etc
    market: only KRAKEN is supported for now
    debug_time: duration of the subscription in minutes
    """
    end_time = datetime.datetime.now() + datetime.timedelta(0, 60 * debug_time)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    app_log = create_ws_subscription_logger(subscription_type, sym, market)
    app_log.info('Creating a WS' + subscription_type + ' subcscription for ' + sym + " on " + market)
    try:
        ws = create_wss_connection_url_for_market(subscription_type, market, sym)
        app_log.info('WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " is successful")
    except Exception as error:
        app_log.error(
            'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + "- Caught the following error:\n" + repr(
                error))
        time.sleep(3)
        raise ConnectionError(
            "Stopping WS " + subscription_type + ' subcscription for ' + sym + " on " + market + " - Subscription failed")

        # TODO: check that sym is valid
    arg = get_args_for_subscription(subscription_type, sym, market)

    while datetime.datetime.now() < end_time:
        try:
            result = ws.recv()
            # TODO handle heartbeats
            result = json.loads(result)
            app_log.info(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " - Received  '%s'" % result)
            if not (is_event_WS_result(result) or is_info_WS_result(result)):
                arg = persist_subscription_result_to_kdb(result, subscription_type, arg)
        except Exception as error:
            app_log.error(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + ' - create_ws_subscription_orderbook - Caught this error: ' + repr(
                    error))
            time.sleep(3)


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
