#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import json
import urllib

import pandas as pd


def get_traded_pairs(market):
    ws_tickers = []
    if market == "KRAKEN":
        api_request = urllib.request.Request("https://api.kraken.com/0/public/AssetPairs")
        result = json.loads(urllib.request.urlopen(api_request).read())
        if result["error"] != []:
            raise ValueError("REST API for " + market + " failed: " + result["error"])
        else:
            result = result["result"]
            for ticker in result.keys():
                if "wsname" in result[ticker].keys():
                    ws_tickers.append(result[ticker]["wsname"])
        api_request = urllib.request.Request("https://futures.kraken.com/derivatives/api/v3/tickers")
        result = json.loads(urllib.request.urlopen(api_request).read())
        for ticker in result["tickers"]:
            ws_tickers.append(ticker["symbol"])
    return ws_tickers


def persist_traded_pairs(markets):
    traded_pairs_by_market_dict = {}
    for market in markets:
        ws_tickers = get_traded_pairs(market)
        traded_pairs_by_market_dict[market] = ws_tickers
    traded_pairs_by_market_df = pd.DataFrame(columns=["market", "sym"])
    for k in traded_pairs_by_market_dict.keys():
        pairs = traded_pairs_by_market_dict[k]
        data = [[k, pairs[i]] for i in range(len(pairs))]
        traded_pairs_df = pd.DataFrame(data, columns=["market", "sym"])
        traded_pairs_by_market_df = pd.concat([traded_pairs_by_market_df, traded_pairs_df])
    traded_pairs_by_market_df.to_csv('../resources/traded_pairs_by_market.csv', index=False)
    return traded_pairs_by_market_df
