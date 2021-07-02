# Variables Format

## Trades
In the codebase, a trade is represented by a dictionary with the following keys:
- order_id
- market
- sym
- 
Example:
```
{}
```

Now, each ECN's API is using a different format:
- KRAKEN:
    - doc: https://docs.kraken.com/websockets/#message-ownTrades
    - example: ``{'TOQG7L-HXPGP-GRO5M7': {'cost': '3.46308', 'fee': '0.00900', 'margin': '0.00000', 'ordertxid': 'OGZFLK-PA5ER-PMKPAK', 'ordertype': 'limit', 'pair': 'XBT/USD', 'postxid': 'TKH2SE-M7IF5-CFI7LT', 'price': '34630.80000', 'time': '1625052861.304954', 'type': 'buy', 'vol': '0.00010000'}}``
- BINANCE
- 

## Orders
In the codebase, an order is represented by a dictionary with the following keys:
- order_id
- market
- sym
- 
Example:
```
{}
```

Now, each ECN's API is using a different format:
- KRAKEN:
    - doc: https://docs.kraken.com/websockets/#message-openOrders 
    - example: ``<class 'dict'>: {'OBPA3G-TWWOL-WZZF2A': {'avg_price': '0.00000', 'cost': '0.00000', 'descr': {'close': None, 'leverage': None, 'order': 'buy 0.00200000 XBT/USD @ limit 5.00000', 'ordertype': 'limit', 'pair': 'XBT/USD', 'price': '5.00000', 'price2': '0.00000', 'type': 'buy'}, 'expiretm': None, 'fee': '0.00000', 'limitprice': '0.00000', 'misc': '', 'oflags': 'fciq', 'opentm': '1625072999.023368', 'refid': None, 'starttm': None, 'status': 'open', 'stopprice': '0.00000', 'timeinforce': 'GTC', 'userref': 0, 'vol': '0.00200000', 'vol_exec': '0.00000000'}}``
- BINANCE
- 