#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import json
import unittest
from time import sleep

from core.src.rest.kraken_rest_api_utils import create_kraken_api
from core.src.sym_handler import SPOT, FUTURE
from risk.src.websocket.webscoket_private import create_wss_subscription_private, add_order, \
    cancel_order, create_wss_private, get_list_of_own_trades, get_list_of_open_orders, is_order_in_list, \
    create_ws_own_trades_subscription, get_associated_trade, is_first_trade_of_execution, \
    get_risk_increasing_trade_of_execution, get_trade_id_from_trade


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
        ws.close()

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
        ws.close()

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
        ws_orders = get_list_of_open_orders(market, instrument_type)
        self.assertTrue(is_order_in_list(txid, ws_orders), "Order should be in list of open orders")
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
        trades = get_list_of_own_trades(market, instrument_type)
        last_trade = trades[0]
        trade_id = list(last_trade.keys())[0]
        self.assertTrue(last_trade[trade_id]["ordertxid"] == txid, "Trade didn't execute?")

    def testSendAndFillSmallLimitOrderKrakenSpot(self):
        market = "KRAKEN"
        instrument_type = SPOT
        sym = "XBTUSD"
        apis = create_kraken_api(instrument_type)
        mid = apis.get_mid(sym)
        ws = create_wss_private(market, instrument_type)
        ws_info = json.loads(ws.recv())
        self.assertTrue(ws_info["status"] == "online", "Connection is not online")
        ws_result = add_order(ws, sym, instrument_type, market, True, "limit", "0.0001", price=round(mid, 1))
        self.assertTrue(ws_result["status"] == "ok", "Something went wrong when adding order")
        txid = ws_result["txid"]
        ws_orders = get_list_of_open_orders(market, instrument_type)
        self.assertTrue(is_order_in_list(txid, ws_orders), "Order should be in list of open orders")

        sleep(10)
        trades = get_list_of_own_trades(market, instrument_type)
        last_trade = trades[0]
        trade_id = list(last_trade.keys())[0]
        self.assertTrue(last_trade[trade_id]["ordertxid"] == txid, "Trade didn't execute?")


    def testProcess_executions(self):
        market = "KRAKEN"
        instrument_type = SPOT
        ws_trades = create_ws_own_trades_subscription(market, instrument_type, account="demo")
        orders_execution_strategies = {"s1": {
            "e1": {"orders": {"o1": {"last_status": "done"}, "o2": {"last_status": "open"}},
                   "trades": {"t1": {}, "t2": {}}}}}
        for strategy_id in orders_execution_strategies:
            strategy_info = orders_execution_strategies[strategy_id]
            for execution_id in strategy_info:
                execution_info = strategy_info[execution_id]
                for order_id in execution_info:
                    order_info = execution_info[order_id]
                    if order_info["last_status"] == "open":
                        # check current status
                        current_status = "open" if is_order_in_list(order_id, get_list_of_open_orders()) else "closed"
                        # if order gets filled
                        if current_status == "closed":
                            trade = get_associated_trade(ws_trades, order_id)
                            trade_id = get_trade_id_from_trade(trade)
                            if is_first_trade_of_execution(trade_id, orders_execution_strategies):
                                pnl = 0
                            else:
                                entry_price = get_risk_increasing_trade_of_execution(execution_info)["price"]
                                pnl = trade_volume * (trade["price"] - entry_price)
                            write_order_update_to_kdb(order_id)
                            write_trade_to_kdb(trade)
                            tell_strategy

    def testSendAndCancelOrderKrakenFuture(self):
        market = "KRAKEN"
        instrument_type = FUTURE
        sym = "XBTUSD/Future/1M"
        ws = create_wss_private(market, instrument_type)
