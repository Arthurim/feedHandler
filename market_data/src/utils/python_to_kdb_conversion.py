#!/usr/bin/env python3
"""
@author: Arthurim
@Description: various functions to convert to kdb
"""


def convert_trades_series_to_kdb_row(row):
    """
    :param row: pd.series, obtained from persist_trades_to_kdb
    :return: str, kdb row ready to be inserted
    """
    market = row["market"]
    return ".z.N;" + \
           "`$\"" + row["sym"] + "\";" + \
           ".z.p;" + \
           "`timestamp$" + row["tradeTimestamp"] + ";" + \
           "`" + market + ";" + \
           "`$\"" + str(row["tradeId"]) + "\";" + \
           "`" + row["side"] + ";" + \
           str(row["price"]) + ";" + \
           str(row["lhsFlow"]) + ";" + \
           str(row["rhsFlow"]) + ";" + \
           "`$\"" + row["orderType"] + "\";" + \
           "\"" + row["misc"] + "\""


def convert_orderbook_series_to_kdb_row(row):
    """
    :param row: pd.series, obtained from persist_orderbook_to_kdb
    :return: str, kdb row ready to be inserted
    """
    return ".z.N;" + \
           "`$\"" + row["sym"] + "\";" + \
           ".z.p;" + \
           "`timestamp$" + row["marketTimestamp"] + ";" + \
           "`$\"" + row["quoteId"] + "\";" + \
           "`" + row["market"] + ";" + \
           str(row["bidPrices"]).replace("[", "(").replace("]", ")") + ";" + \
           str(row["bidSizes"]).replace("[", "(").replace("]", ")") + ";" + \
           str(row["offerPrices"]).replace("[", "(").replace("]", ")") + ";" + \
           str(row["offerPrices"]).replace("[", "(").replace("]", ")")


def convert_spread_to_kdb_row(row):
    """
    :param row: pd.series, obtained from persist_spread_to_kdb
    :return: str, kdb row ready to be inserted
    """
    return ".z.N;" + \
           "`$\"" + row["sym"] + "\";" + \
           ".z.p;" + \
           "`timestamp$" + row["marketTimestamp"] + ";" + \
           "`" + row["market"] + ";" + \
           str(row["bidPrice"]) + ";" + \
           str(row["bidSize"]) + ";" + \
           str(row["offerPrice"]) + ";" + \
           str(row["offerSize"])


def convert_ohlc_series_to_kdb_row(row):
    """
    :param row: pd.series, obtained from persist_ohlc_to_kdb
    :return: str, kdb row ready to be inserted
    """
    return ".z.N;" + \
           "`$\"" + row["sym"] + "\";" + \
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
