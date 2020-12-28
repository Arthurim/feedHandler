#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import datetime
import logging
from uuid import uuid4

import pandas as pd

from kdb_utils_format import has_kdb_format_timestamp
from persistence import persist_row_to_table


def dicttofloat(keyvalue):
    return float(keyvalue[0])


def process_side(levels):
    """
    From a dictionary where each key is volume and value is price,
    returns a list of prices and a list of sizes
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
    Updates the orderbook api_book with the new result for a max depth level
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
    insert_orderbook_new_row_to_kdb(api_book)
    return api_book


def convert_orderbook_series_to_kdb_row(new_row):
    return ".z.N;" + \
           "`" + new_row["sym"].replace("/", "") + ";" + \
           ".z.p;" + \
           "`timestamp$" + new_row["marketTimestamp"] + ";" + \
           "`$\"" + new_row["quoteId"] + "\";" + \
           "`" + new_row["market"] + ";" + \
           str(new_row["bidPrices"]).replace("[", "(").replace("]", ")") + ";" + \
           str(new_row["bidSizes"]).replace("[", "(").replace("]", ")") + ";" + \
           str(new_row["offerPrices"]).replace("[", "(").replace("]", ")") + ";" + \
           str(new_row["offerPrices"]).replace("[", "(").replace("]", ")")


def get_data_from_orderbook_result(result, market):
    """
    returns a dictionary of orderbook data depending on the result obtained from WS api as it differs for each market
    """
    if market == "KRAKEN":
        if "bs" in result[1]:
            data = {"bids": result[1]["bs"], "asks": result[1]["as"],
                    "marketTimestamp": get_timestamp_from_kraken_orderbook(result)}
        elif "b" in result[1]:
            data = {"b": result[1]["b"], "marketTimestamp": get_timestamp_from_kraken_orderbook(result)}
        elif "a" in result[1]:
            data = {"a": result[1]["a"], "marketTimestamp": get_timestamp_from_kraken_orderbook(result)}
    elif market == "BINANCE":
        result["marketTimestamp"] = datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f")
        data = result
    elif market == "BITMEX":
        data = {"bids": result["data"][0]["bids"], "asks": result["data"][0]["asks"],
                "marketTimestamp": result["data"][0]['timestamp']}
    elif market == "BITFINEX":
        data = {"bids": result[1][0:25], "asks": result[1][25:], "marketTimestamp": ""}
    return data


def get_timestamp_from_kraken_orderbook(result_kraken):
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
    api_book is a dictionary with marketTimestamp, sym, market, bid, ask
    """
    bids = api_book["bid"]
    asks = api_book["ask"]
    bid_prices, bid_sizes = process_side(bids)
    offer_prices, offer_sizes = process_side(asks)
    new_row = pd.Series({"gatewayTimestamp": datetime.datetime.now().strftime("%Y.%m.%dD%H:%M:%S.%f"),
                         "marketTimestamp": api_book["marketTimestamp"],
                         "time": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                         "quoteId": str(uuid4()).replace("-", ""),
                         "sym": api_book["sym"],
                         "market": api_book["market"],
                         "bidPrices": bid_prices,
                         "bidSizes": bid_sizes,
                         "offerPrices": offer_prices,
                         "offerSizes": offer_sizes})
    return new_row


def insert_orderbook_new_row_to_kdb(api_book):
    """
    Updates kdb table quotestackevents with the new row
    """
    new_row = get_orderbook(api_book)
    kdb_row = convert_orderbook_series_to_kdb_row(new_row)
    persist_row_to_table(kdb_row, "orderbooks", "localhost", 5000)
