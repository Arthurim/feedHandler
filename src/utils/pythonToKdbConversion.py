#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""


def convert_trades_series_to_kdb_row(new_row):
    return ".z.N;" + \
           "`" + new_row["sym"].replace("/", "") + ";" + \
           ".z.p;" + \
           "`timestamp$" + new_row["tradeTimestamp"] + ";" + \
           "`" + new_row["market"] + ";" + \
           "`$\"" + new_row["tradeId"] + "\";" + \
           "`" + new_row["side"] + ";" + \
           str(new_row["price"]) + ";" + \
           str(new_row["lhsFlow"]) + ";" + \
           str(new_row["rhsFlow"]) + ";" + \
           "`" + new_row["orderType"] + ";" + \
           new_row["misc"]


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


def convert_spread_to_kdb_row(row):
    return ".z.N;" + \
           "`" + row["sym"].replace("/", "") + ";" + \
           ".z.p;" + \
           "`timestamp$" + row["marketTimestamp"] + ";" + \
           "`" + row["market"] + ";" + \
           str(row["bidPrice"]) + ";" + \
           str(row["bidSize"]) + ";" + \
           str(row["offerPrice"]) + ";" + \
           str(row["offerSize"])


def convert_ohlc_series_to_kdb_row(row):
    return ".z.N;" + \
           "`" + row["sym"].replace("/", "") + ";" + \
           ".z.p;" + \
           "`timestamp$" + row["marketTimestamp"] + ";" + \
           "`" + row["market"] + ";" + \
           "`timestamp$" + row["endTime"] + ";" + \
           str(row["interval"]) + ";" + \
           str(row["open"]) + ";" + \
           str(row["high"]) + ";" + \
           str(row["low"]) + ";" + \
           str(row["close"]) + ";" + \
           str(row["vwap"]) + ";" + \
           str(row["totalLhsFlow"]) + ";" + \
           str(row["tradeCount"])
