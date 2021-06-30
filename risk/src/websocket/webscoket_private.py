#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""

import json

from websocket import create_connection

from core.src.rest.kraken_rest_api_utils import create_kraken_api, get_kraken_public_key, get_kraken_private_key
from core.src.sym_handler import SPOT, FUTURE, format_sym_for_market
from core.src.websocket.encryption_utils import sign_challenge
from core.src.websocket.websocket_constants import get_url_for_market


def create_wss_subscription_private(subscription_type, market, instrument_type, account="arthur"):
    ws = create_wss_private(market, instrument_type)
    if market == "KRAKEN":
        rest_api = create_kraken_api(instrument_type, account)
        if instrument_type == SPOT:
            ws_token = rest_api.get_private_token()
            ws.send(json.dumps({"event": "subscribe", "subscription": {"name": subscription_type, "token": ws_token}}))
        elif instrument_type == FUTURE:
            public_key = get_kraken_public_key(instrument_type, account)
            private_key = get_kraken_private_key(instrument_type, account)
            challenge_info = get_challenge_kraken_futures(ws, instrument_type, account)
            ws.send(json.dumps({
                "event": "subscribe",
                "feed": subscription_type,
                "api_key": public_key,
                "original_challenge": challenge_info["original_challenge"],
                "signed_challenge": challenge_info["signed_challenge"]
            }))
        else:
            raise ValueError("Instrument type not supported:", instrument_type)

    else:
        raise ValueError("Market not supported:", market)
    return ws


def add_order(ws, sym, instrument_type, market, buy, order_type, volume, userref=None, leverage=None, price=0.00001,
              account="arthur"):
    # check ws is associated with market ?
    if market == "KRAKEN":
        rest_api = create_kraken_api(instrument_type, account)
        if instrument_type == SPOT:
            ws_token = rest_api.get_private_token()
            if order_type == "market":
                order_args = {"event": "addOrder",
                              "token": ws_token,
                              "pair": format_sym_for_market(sym, market),
                              "type": "buy" if buy else "sell",
                              "ordertype": order_type,
                              "volume": volume}
            elif order_type == "limit":
                order_args = {"event": "addOrder",
                              "token": ws_token,
                              "pair": format_sym_for_market(sym, market),
                              "type": "buy" if buy else "sell",
                              "ordertype": order_type,
                              "price": price,
                              "volume": volume}

            if leverage is not None:
                order_args["leverage"] = leverage
            if userref is not None:
                order_args["userref"] = userref

            ws.send(json.dumps(order_args))
            # check all ok with ws
            ws_result = json.loads(ws.recv())
            if "errorMessage" in ws_result:
                print(ws_result["errorMessage"])
        elif instrument_type == FUTURE:
            challenge_info = get_challenge_kraken_futures(ws, instrument_type)
            ws.send(json.dumps({
                "event": "addOrder",
                "feed": subscription_type,
                "api_key": public_key,
                "original_challenge": challenge_info["original_challenge"],
                "signed_challenge": challenge_info["signed_challenge"]
            }))

    return ws_result


def cancel_order(ws, order_id, market, instrument_type, account="arthur"):
    if market == "KRAKEN":
        rest_api = create_kraken_api(instrument_type, account)
        if instrument_type == SPOT:
            ws_token = rest_api.get_private_token()
            order_args = {"event": "cancelOrder",
                          "token": ws_token,
                          "txid": [order_id]}
            ws.send(json.dumps(order_args))
            # check all ok with ws
            ws_result = json.loads(ws.recv())
            if "errorMessage" in ws_result:
                print(ws_result["errorMessage"])
    return ws_result


def create_wss_private(market, instrument_type):
    url = get_url_for_market(market, instrument_type, False)
    return create_connection(url)


def create_wss_order_management(subscription_type, market, instrument_type, sym, arg):
    url = get_url_for_market(market, instrument_type, False)
    ws = create_connection(url)
    if market == "KRAKEN":
        rest_api = create_kraken_api(instrument_type, "arthur")
        if instrument_type == SPOT:
            ws_token = rest_api.get_private_token()
            ws.send(json.dumps({"event": subscription_type,
                                "token": ws_token,
                                "pair": format_sym_for_market(sym, market),
                                "type": arg["type"],
                                "ordertype": arg["ordertype"],
                                "price": arg["price"],
                                "volume": arg["volume"],
                                # "leverage": arg["leverage"],
                                "userref": arg["userref"]}))
        else:
            raise ValueError("Instrument type not supported:", instrument_type)
    else:
        raise ValueError("Market not supported:", market)
    return ws


def get_challenge_kraken_futures(ws, instrument_type, account="arthur"):
    public_key = get_kraken_public_key(instrument_type, account)
    private_key = get_kraken_private_key(instrument_type, account)
    ws.send(json.dumps({
        "event": "challenge",
        "api_key": public_key
    }))
    version_info = json.loads(ws.recv())
    challenge_info = json.loads(ws.recv())
    if challenge_info["event"] == "challenge":
        original_challenge = challenge_info["message"]
        signed_challenge = sign_challenge(private_key, original_challenge)
    else:
        # tryagain
        raise ValueError("Should return challenge, but got this instead:", challenge_info)
    return {"original_challenge": original_challenge, "signed_challenge": signed_challenge}
