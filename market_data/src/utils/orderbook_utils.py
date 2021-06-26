#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""


def convert_orderbook_to_string_representation(orderbook):
    orderbook_str = {'sym': orderbook['sym'], 'market': orderbook['market']}
    orderbook_str['marketTimestamp'] = str(orderbook["marketTimestamp"])
    for side in ["bid", "ask"]:
        orderbook_str[side] = {}
        for key, value in orderbook[side].items():
            orderbook_str[side][str("{:.5f}".format(key))] = str("{:.8f}".format(value))
    return orderbook_str


def convert_string_representation_of_orderbook_to_orderbook(orderbook_str):
    orderbook = {'sym': orderbook_str['sym'], 'market': orderbook_str['market']}
    orderbook['marketTimestamp'] = orderbook_str[
        "marketTimestamp"]  # pd.to_datetime(orderbook_str["marketTimestamp"], format="%Y.%m.%dD%H:%M:%S.%f") if orderbook_str['marketTimestamp'] != "" else ''
    for side in ["bid", "ask"]:
        orderbook[side] = {}
        for key, value in orderbook_str[side].items():
            orderbook[side][float(key)] = float(value)
    return orderbook
