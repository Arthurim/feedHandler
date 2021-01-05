#!/usr/bin/env python3
"""
@author: Arthurim
@Description: various utilis for sym handling
"""


def get_sym_format(sym, market):
    """
    Formats the sym to adapt to ECN format, eg XBTUSD will be XBT/USD for Kraken

    :param sym: should be a string of 6 letters
    :param market: should be a string
    :return: string in the right format for the given market
    """
    if not len(sym) >= 6:
        raise ValueError("sym input should be a string of 6 letters, it was: ", sym)
    if market == "KRAKEN":
        return sym[0:3] + "/" + sym[3:6]
    elif market == "COINBASE":
        sym = sym.replace("XBT", "BTC")
        return sym[0:3] + "-" + sym[3:6]
    elif market == "BINANCE":
        sym = sym.replace("XBT", "BTC").lower()
        return sym
    else:
        return sym
