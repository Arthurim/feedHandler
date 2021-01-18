#!/usr/bin/env python3
"""
@author: Arthurim
@Description: functions to handle the persistence of market data from WS APIs to Kdb
"""
import datetime
import logging
import time

from market_data.src.constants.markets import is_supported_market
from market_data.src.ohlcs import persist_ohlc_to_kdb
from market_data.src.orderbooks import persist_orderbook_to_kdb
from market_data.src.spreads import persist_spread_to_kdb
from market_data.src.trades import persist_trades_to_kdb
from market_data.src.utils.persistence_utils import get_args_for_subscription, get_ws_result
from market_data.src.utils.websocket_message_handler import create_ws_subscription_logger, create_wss_connection, \
    is_event_WS_result, is_info_WS_result, is_ping_WS_result, is_not_huobi_tick_result, \
    is_correct_subscription_message_bitmex


def persist_subscription_result_to_kdb(result, subscription_type, arg=""):
    """
    Depending in the subscription_type, calls the required persistence function

    :param result: dictionary, the result from the API call
    :param subscription_type: string, the type of data we are persisting
    :param arg: any argument required to handle persistence,
            for now only a dictionary representing last state of the orderbook:
            {"sym": sym, "market": market, "marketTimestamp": {}, "bid": {}, "ask": {}}
    :return: the updated arg
    """
    if subscription_type == "orderbooks":
        arg = persist_orderbook_to_kdb(arg, result)
    elif subscription_type == "trades":
        persist_trades_to_kdb(result, arg)
    elif subscription_type == "ohlcs":
        persist_ohlc_to_kdb(result)
    elif subscription_type == "spreads":
        persist_spread_to_kdb(result)
    else:
        raise ValueError("This subscription_type is not yet supported: " + str(subscription_type))
    return arg


def create_ws_subscription_kdb_persister(subscription_type, sym, market):
    """
    Creates a WS subscription subscription_type for a given market and instrument

    :param subscription_type: string, the type of data we are persisting (trades, orderbooks, ohlcs, spreads)
    :param sym: string, XBTUSD etc
    :param market: string, see markets in SUPPORTED_MARKETS
    """
    if not is_supported_market(market):
        raise ValueError("Market not supported: " + market)
        # TODO: log it
    # TODO: check that sym is valid
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    app_log = create_ws_subscription_logger(subscription_type, sym, market)
    app_log.info('Creating a WS' + subscription_type + ' subcscription for ' + sym + " on " + market)
    try:
        ws = create_wss_connection(subscription_type, market, sym)
        app_log.info('WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " is successful")
    except Exception as error:
        app_log.error(
            'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + "- Caught the following error:\n" + repr(
                error))
        time.sleep(3)
        raise ConnectionError(
            "Stopping WS " + subscription_type + ' subcscription for ' + sym + " on " + market + " - Subscription failed")

    arg = get_args_for_subscription(subscription_type, sym, market)

    while True:
        try:
            result = get_ws_result(ws, market)
            app_log.info(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " - Received  '%s'" % result)
            if not (is_event_WS_result(result) or is_info_WS_result(result)):
                arg = persist_subscription_result_to_kdb(result, subscription_type, arg)
        except Exception as error:
            app_log.error(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + ' - create_ws_subscription_orderbook - Caught this error: ' + repr(
                    error))
            time.sleep(3)
    ws.close()


def create_ws_subscription_kdb_persister_debug(subscription_type, sym, market, debug_time=1):
    """
    Creates a WS subscription subscription_type for a given market and instrument for debug_time minutes

    :param debug_time: int, duration in minutes for the persistence
    :param subscription_type: string, the type of data we are persisting (trades, orderbooks, ohlcs, spreads)
    :param sym: string, XBTUSD etc
    :param market: string, see markets in SUPPORTED_MARKETS
    """
    end_time = datetime.datetime.now() + datetime.timedelta(0, 60 * debug_time)
    if not is_supported_market(market):
        raise ValueError("Market not supported: " + market)
        # TODO: log it
    # TODO: check that sym is valid
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    app_log = create_ws_subscription_logger(subscription_type, sym, market)
    app_log.info('Creating a WS' + subscription_type + ' subcscription for ' + sym + " on " + market)
    try:
        ws = create_wss_connection(subscription_type, market, sym)
        app_log.info('WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " is successful")
    except Exception as error:
        app_log.error(
            'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + "- Caught the following error:\n" + repr(
                error))
        time.sleep(3)
        raise ConnectionError(
            "Stopping WS " + subscription_type + ' subcscription for ' + sym + " on " + market + " - Subscription failed")

    arg = get_args_for_subscription(subscription_type, sym, market)

    while datetime.datetime.now() < end_time:
        try:
            result = get_ws_result(ws, market)
            app_log.info(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + " - Received  '%s'" % result)
            if not (is_event_WS_result(result) or is_info_WS_result(result) or is_ping_WS_result(
                    result) or is_not_huobi_tick_result(result, market) or is_correct_subscription_message_bitmex(
                result, market)):
                arg = persist_subscription_result_to_kdb(result, subscription_type, arg)
        except Exception as error:
            app_log.error(
                'WS ' + subscription_type + ' subcscription for ' + sym + " on " + market + ' - Caught this error: ' + repr(
                    error))
            time.sleep(3)
    ws.close()
    return app_log.handlers[0].baseFilename
