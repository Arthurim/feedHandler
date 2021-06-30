#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import json
import unittest
from time import sleep

from core.src.sym_handler import SPOT, FUTURE
from risk.src.websocket.webscoket_private import create_wss_subscription_private, add_order, \
    cancel_order, create_wss_private


class TestPrivateWebsocket(unittest.TestCase):

    def testShouldCreateSubscriptionForOwnTradesKrakenSpot(self):
        subscription_type = "ownTrades"
        market = "KRAKEN"
        instrument_type = SPOT
        ws = create_wss_subscription_private(subscription_type, market, instrument_type)

        r1 = json.loads(ws.recv())
        self.assertTrue(r1["status"] == "online", "Connection is not online")
        r2 = json.loads(ws.recv())
        self.assertTrue(r2["status"] == "subscribed", "Connection is not subscribed")
        r3 = json.loads(ws.recv())
        self.assertTrue((type(r3) == list) and (r3[1] == subscription_type), "Should receive a list of trades")

    def testShouldCreateSubscriptionForOwnOrdersKrakenSpot(self):
        subscription_type = "openOrders"
        market = "KRAKEN"
        instrument_type = SPOT
        ws = create_wss_subscription_private(subscription_type, market, instrument_type)

        r1 = json.loads(ws.recv())
        self.assertTrue(r1["status"] == "online", "Connection is not online")
        r2 = json.loads(ws.recv())
        self.assertTrue(r2["status"] == "subscribed", "Connection is not subscribed")
        r3 = json.loads(ws.recv())
        self.assertTrue((type(r3) == list) and (r3[1] == subscription_type), "Should receive a list of open orders")

    def testShouldCreateSubscriptionForOwnFillsnKrakenFuture(self):
        subscription_type = "fills"
        market = "KRAKEN"
        instrument_type = FUTURE
        ws = create_wss_subscription_private(subscription_type, market, instrument_type)

        ws_info = json.loads(ws.recv())
        self.assertTrue(ws_info["event"] == "subscribed", "Subscription failed")
        ws_info = json.loads(ws.recv())
        self.assertTrue((type(ws_info["fills"]) == list) and (ws_info["feed"] == subscription_type + "_snapshot"),
                        "Should receive a list of trades")

    def testSendAndCancelOrderKrakenSpot(self):
        market = "KRAKEN"
        instrument_type = SPOT
        sym = "XBTUSD"
        ws = create_wss_private(market, instrument_type)
        ws_info = json.loads(ws.recv())
        self.assertTrue(ws_info["status"] == "online", "Connection is not online")
        ws_result = add_order(ws, sym, instrument_type, market, True, "limit", "0.002", price="5")
        self.assertTrue(ws_result["status"] == "ok", "Something went wrong when adding order")
        txid = ws_result["txid"]
        sleep(10)
        ws_result = cancel_order(ws, txid, market, instrument_type)
        self.assertTrue(ws_result["status"] == "ok", "Something went wrong when canceling order")

    def testSendAndFillSmallMarketOrderKrakenSpot(self):
        market = "KRAKEN"
        instrument_type = SPOT
        sym = "XBTUSD"
        ws = create_wss_private(market, instrument_type)
        ws_info = json.loads(ws.recv())
        self.assertTrue(ws_info["status"] == "online", "Connection is not online")
        ws_result = add_order(ws, sym, instrument_type, market, True, "market", "0.0001")
        self.assertTrue(ws_result["status"] == "ok", "Something went wrong when adding order")
        txid = ws_result["txid"]

    def testSendAndCancelOrderKrakenFuture(self):
        market = "KRAKEN"
        instrument_type = FUTURE
        sym = "XBTUSD/Future/1M"
        ws = create_wss_private(market, instrument_type)
        addOrder
