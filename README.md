# MARKET DATA WEBSCOKET FEED HANDLER

## TLDR
This python library allows you to create a webscoket subscription to get market data from crypto currency exchanges and
to write it to KDB

## Supported features
The following exchanges are supported:
- BINANCE
- BITFINEX
- BITMEX
- KRAKEN

The below market data are supported:
- orderbooks
- trades
- ohlcs
- spreads

For now only spot instruments are supported. Future support will be added later.

## Setup
TBAL

## Example of use:
This will launch the subscription of `orderbooks` data for market `KRAKEN` and currency pair `XBTUSD`
`./launch/launch_market_data_subscription.sh "orderbooks" "XBTUSD" "KRAKEN"`
