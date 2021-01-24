#!/usr/bin/env python3
"""
@author: Arthurim
@Description: functions to handle the creation of an orderbook from the ECNs APIs and to persist it to kdb
"""
import datetime
import logging
from uuid import uuid4

import pandas as pd

from market_data.src.constants.kdb_hosts import MARKET_DATA_KDB_HOST, MARKET_DATA_TP
from market_data.src.utils.kdb_utils_format import has_kdb_format_timestamp, convert_sym_to_kdb_format
from market_data.src.utils.persistence_utils import persist_row_to_table
from market_data.src.utils.sym_handler import is_spot_market_ticker, is_future_market_ticker


def dicttofloat(keyvalue):
    return float(keyvalue[0])


def process_side(levels):
    """
    From a dictionary where each key is volume and value is price,
    returns a list of prices and a list of sizes

    :param levels: dictionary or list, the levels of depth in orderbook with volume & price (& timestamp)
    :return: two lists of float, prices and sizes for each level of the orderbook
    """
    prices = []
    sizes = []
    if type(levels) == dict:
        for k, v in levels.items():
            prices.append(v)
            sizes.append(float(k))
    else:
        for quote in levels:
            if len(quote) == 2:
                prices.append(quote[0])
                sizes.append(quote[1])
            else:
                prices.append(quote[0])
                sizes.append(quote[2])
    return prices, sizes


def update_order_book_with_side_data(api_book, side, data, depth=10):
    """
    Updates the api_book on a given side with the new data for a max depth level

    :param api_book: dictionary, last representation of the orderbook
    :param side: string, "bid" or "ask"
    :param data:
    :param depth: int, max level of depth we want to persist data for
    :return: dictionary, new representation of the orderbook
    """
    for x in data:
        price_level = x[0]
        # marketTimestamp = datetime.datetime.fromtimestamp(float(x[2])).strftime("%Y.%m.%dD%H:%M:%S.%f")
        if float(x[1]) != 0.0:
            api_book[side].update({price_level: float(x[1])})
        else:
            if price_level in api_book[side]:
                api_book[side].pop(price_level)
    if side == "bid":
        api_book["bid"] = dict(sorted(api_book["bid"].items(), key=dicttofloat, reverse=True)[:int(depth)])
    elif side == "ask":
        api_book["ask"] = dict(sorted(api_book["ask"].items(), key=dicttofloat)[:int(depth)])
    return api_book


def persist_orderbook_to_kdb(api_book, result, depth=10):
    """
    Updates the orderbook api_book with the new result for a max depth level and calls the persistence function

    :param api_book: dictionary, last state of the orderbook
    :param result: dictionary, the result from the API
    :param depth: int, the max nb of levels we want to persist
    :return:
    """

    # TODO checksum see https://docs.kraken.com/websockets/#book-checksum
    result = get_data_from_orderbook_result(result, api_book["market"])
    app_log = logging.getLogger('root')
    if "asks" in result:
        api_book = update_order_book_with_side_data(api_book, "ask", result["asks"], depth)
        api_book = update_order_book_with_side_data(api_book, "bid", result["bids"], depth)
    elif "a" in result or "b" in result:
        if "a" in result:
            api_book = update_order_book_with_side_data(api_book, "ask", result["a"], depth)
        elif "b" in result:
            api_book = update_order_book_with_side_data(api_book, "bid", result["b"], depth)
    if has_kdb_format_timestamp(result["marketTimestamp"]):
        api_book["marketTimestamp"] = result["marketTimestamp"]
    else:
        if "T" in result["marketTimestamp"]:
            api_book["marketTimestamp"] = result["marketTimestamp"].replace("T", "D").replace("-", ".").replace("Z", "")
        else:
            api_book["marketTimestamp"] = datetime.datetime.fromtimestamp(float(result["marketTimestamp"])).strftime(
                "%Y.%m.%dD%H:%M:%S.%f")
    insert_orderbook_row_to_kdb(api_book)
    return api_book


def convert_orderbook_series_to_kdb_row(row):
    """
    Converts a pd.series row representing the orderbook to a string to insert into kdb

    :param row: pd.series, a row representing the orderbook, obtained from get_orderbook
    :return: str, the string representation of the orderbook, to insert as a new row in kdb
    """
    return ".z.N;" + \
           "`$\"" + convert_sym_to_kdb_format(row["sym"]) + "\";" + \
           ".z.p;" + \
           "`timestamp$" + row["marketTimestamp"] + ";" + \
           "`$\"" + row["quoteId"] + "\";" + \
           "`" + row["market"] + ";" + \
           str(row["bidPrices"]).replace("[", "(").replace("]", ")") + ";" + \
           str(row["bidSizes"]).replace("[", "(").replace("]", ")") + ";" + \
           str(row["offerPrices"]).replace("[", "(").replace("]", ")") + ";" + \
           str(row["offerPrices"]).replace("[", "(").replace("]", ")")


def get_data_from_orderbook_result(result, market):
    """
    Returns a dictionary of orderbook data depending on the result obtained from WS api as it differs for each market

    :param result: dictionary, the API result
    :param market: string, market
    :return: dictionary, orderbook
    """
    if market == "KRAKEN":
        if type(result) == list:
            if is_spot_market_ticker(result[3], market):
                if "bs" in result[1]:
                    data = {"bids": result[1]["bs"], "asks": result[1]["as"],
                            "marketTimestamp": get_timestamp_from_kraken_orderbook(result)}
                elif "b" in result[1]:
                    data = {"b": result[1]["b"], "marketTimestamp": get_timestamp_from_kraken_orderbook(result)}
                elif "a" in result[1]:
                    data = {"a": result[1]["a"], "marketTimestamp": get_timestamp_from_kraken_orderbook(result)}
            else:
                raise ValueError("API result should be a list only for spot for KRAKEN.")
        elif type(result) == dict:
            if is_future_market_ticker(result["product_id"]):
                if "bids" in result.keys():
                    data = {"bids": process_kraken_future_orderbook_side(result["bids"]),
                            "asks": process_kraken_future_orderbook_side(result["asks"]),
                            "marketTimestamp": datetime.datetime.fromtimestamp(
                                float(result["timestamp"]) / 1e3).strftime(
                                "%Y.%m.%dD%H:%M:%S.%f")}
                elif "side" in result.keys():
                    if result["side"] == "buy":
                        data = {"bids": [[result["price"], result["qty"]]],
                                "asks": [],
                                "marketTimestamp": datetime.datetime.fromtimestamp(
                                    float(result["timestamp"]) / 1e3).strftime(
                                    "%Y.%m.%dD%H:%M:%S.%f")}
                    elif result["side"] == "sell":
                        data = {"bids": [],
                                "asks": [[result["price"], result["qty"]]],
                                "marketTimestamp": datetime.datetime.fromtimestamp(
                                    float(result["timestamp"]) / 1e3).strftime(
                                    "%Y.%m.%dD%H:%M:%S.%f")}
            else:
                raise ValueError("API result should be a dictionary only for future for KRAKEN.")
    elif market == "BINANCE":
        result["marketTimestamp"] = datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f")
        data = result
    elif market == "BITMEX":
        data = {"bids": result["data"][0]["bids"], "asks": result["data"][0]["asks"],
                "marketTimestamp": result["data"][0]['timestamp']}
    elif market == "BITFINEX":
        data = {"bids": [], "asks": [], "marketTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f")}
        if type(result[1][0]) is list:
            for level in result[1]:
                data = update_level_bitfinex(data, level)
        else:
            data = update_level_bitfinex(data, result[1])
    elif market == "COINBASE":
        if result["type"] == "snapshot":
            data = {"bids": result["bids"], "asks": result["asks"],
                    "marketTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f")}
        elif result["type"] == "l2update":
            if result["changes"][0][0] == "buy":
                data = {"b": [], "marketTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f")}
            else:
                data = {"a": [], "marketTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f")}
            for change in result["changes"]:
                if change[0] == "buy":
                    data["b"].append([change[1], change[2]])
                if change[0] == "sell":
                    data["a"].append([change[1], change[2]])
    elif market == "HUOBI":
        data = {"bids": result["tick"]["bids"], "asks": result["tick"]["asks"],
                "marketTimestamp": datetime.datetime.fromtimestamp(float(result["ts"]) / 1e3).strftime(
                    "%Y.%m.%dD%H:%M:%S.%f")}
    return data


def update_level_bitfinex(data, level):
    price = level[0]
    count = level[1]
    size = level[2]
    if count > 0:
        if size > 0:
            data["bids"].append([price, size])
        else:
            data["asks"].append([price, -size])
    return data


def get_timestamp_from_kraken_orderbook(result_kraken):
    """
    Returns the marketTimestamp of the orderbook for Kraken API result,
    it depends on whether the API result is a snapshot or an update

    :param result_kraken: dictionary, API result from KRAKEN
    :return: float, market timestamp
    """
    timestamps = []
    if "as" in result_kraken[1]:
        for level in result_kraken[1]["as"]:
            timestamps.append(float(level[2]))
        for level in result_kraken[1]["bs"]:
            timestamps.append(float(level[2]))
    elif "b" in result_kraken[1]:
        for level in result_kraken[1]["b"]:
            timestamps.append(float(level[2]))
    elif "a" in result_kraken[1]:
        for level in result_kraken[1]["a"]:
            timestamps.append(float(level[2]))
    return str(max(timestamps))


def get_orderbook(api_book):
    """
    Converts the orderbook into a row ready to be inserted in dataframe or processed to kdb

    :param api_book: dictionary, represents the orderbook
    :return:
    """
    bids = api_book["bid"]
    asks = api_book["ask"]
    bid_prices, bid_sizes = process_side(bids)
    offer_prices, offer_sizes = process_side(asks)
    row = pd.Series({"gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                     "marketTimestamp": api_book["marketTimestamp"],
                     "time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                     "quoteId": str(uuid4()).replace("-", ""),
                     "sym": api_book["sym"],
                     "market": api_book["market"],
                     "bidPrices": bid_prices[:10],
                     "bidSizes": bid_sizes[:10],
                     "offerPrices": offer_prices[:10],
                     "offerSizes": offer_sizes[:10]})
    return row


def insert_orderbook_row_to_kdb(api_book):
    """
    Updates orderbooks kdb table with the new row created from the orderbook

    :param api_book: dictionary, last state of the orderbook
    :return:
    """
    row = get_orderbook(api_book)
    kdb_row = convert_orderbook_series_to_kdb_row(row)
    persist_row_to_table(kdb_row, "orderbooks", MARKET_DATA_KDB_HOST, MARKET_DATA_TP)


def process_kraken_future_orderbook_side(side):
    """

    :param side: is a list of dictionary, representing all the levels of that side of the orderbook,
    eg: [{"price":31,"qty":4},{"price":32,"qty":5},{"price":33,"qty":4}]
    :return: [[price,size],[price,size],[price,size]]
    """
    side_result = []
    for level in side:
        price = level["price"]
        size = level["qty"]
        side_result.append([price, size])
    return side_result
