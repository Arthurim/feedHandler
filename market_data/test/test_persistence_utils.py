#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import unittest

DEBUG_TIME = 1
import json
from market_data.src.utils.websocket_message_handler import create_wss_connection


class testPersistenceUtils(unittest.TestCase):

    def testShouldStartPersistence(self):
        a = 0

    def testShouldStopPersistence(self):
        sym = "XBTUSD"
        market = "KRAKEN"
        subscription_type = "orderbooks"
        ws = create_wss_connection(subscription_type, market, sym)
        r1 = json.loads(ws.recv())
        r2 = json.loads(ws.recv())
        status = str(ws.getstatus())
        ping = ws.send(json.dumps({"event": "ping", "reqid": 42}))
        pong = json.loads(ws.recv())
        self.assertTrue(pong["reqid"] == 42, "Learn to play ping pong")
        ws.send(json.dumps({"event": "unsubscribe", "channelID": r2["channelID"]}))
        ping = ws.send(json.dumps({"event": "ping"}))
        pong = json.loads(ws.recv())

    def testShould(self):
        sym = "XBTUSD"
        market = "KRAKEN"
        subscription_type = "orderbooks"
        ws1 = create_wss_connection(subscription_type, market, sym)
        ws2 = create_wss_connection(subscription_type, market, sym)
        r1 = json.loads(ws1.recv())
        r2 = json.loads(ws2.recv())
        r1 = json.loads(ws1.recv())
        r2 = json.loads(ws2.recv())
        self.assertTrue(r1 == r2, "Learn to play ping pong")


if __name__ == "__main__":
    unittest.main()
    print("Everything passed")
