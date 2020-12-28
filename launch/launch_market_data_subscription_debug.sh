#!/bin/bash
# @author: Arthurim
# @Description: This script launches a websocket market data subscription for debug_time minutes
#               This writes to the TP (5000)
#               subscription_type is one of the following: orderbooks, ohlcs, trades, spreads
#               market is one of the following: KRAKEN, BINANCE, BITMEX, BITFINEX

subscription_type=$1
sym=$2
market=$3
debug_time=$4
echo "Launching $subscription_type subscription for $market and $sym for $debug_time minutes"
python -m launch.launch_market_data_subscription_debug $subscription_type $sym $market $debug_time