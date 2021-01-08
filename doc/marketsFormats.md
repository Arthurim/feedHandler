# Currency Pairs Format

## ECNs
Each ECN uses a different format for its currency pairs codes.
Below is a recap

ECN | Instrument | Code
----|--|--
KRAKEN | SPOT | XBT/USD  
KRAKEN | FUTURE | FI\_XBTUSD\_YYMMDD  
BINANCE | SPOT | btcusd
BINANCE | FUTURE | ?
COINBASE | SPOT | BTC-USD  
COINBASE | FUTURE | ?
BITMEX | SPOT | XBTUSD  
BITMEX | FUTURE | ?  
BITFINEX | SPOT | BTCUSD  
BITFINEX | FUTURE | ?
HUOBI | SPOT | btcusd
HUOBI | FUTURE | ?

## Internal

We will use the below conventions in code + in kdb:
- XBT not BTC
- Spot: XBTUSD
- Future: XBTUSD/Future/YYMMDD and XBTUSD/Future/1M, 1Q ? Depending on the date I know that the settlement date is last friday etc