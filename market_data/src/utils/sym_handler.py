#!/usr/bin/env python3
"""
@author: Arthurim
@Description: various utilis for sym handling

@see marketsFormats.md
"""
import datetime

from market_data.src.utils.futures_expiry import get_month_code, get_month_str, get_month_int, \
    get_last_friday_of_the_month


def from_huobi_sym(sym):
    return sym.upper()


def hasNumbers(s):
    return any(char.isdigit() for char in s)


def format_sym_for_market(sym, market):
    """
    Formats the sym to adapt to ECN format, eg XBTUSD will be XBT/USD for Kraken

    :param sym: should be a string of 6 letters
    :param market: should be a string
    :return: string in the right format for the given market
    """
    if not len(sym) >= 6:
        raise ValueError("sym input should be a string of 6 letters, it was: ", sym)
    if is_spot(sym):
        if market == "KRAKEN":
            # XBT/USD
            return sym[0:3] + "/" + sym[3:6]
        elif market == "COINBASE":
            # BTC-USD
            sym = sym.replace("XBT", "BTC")
            return sym[0:3] + "-" + sym[3:6]
        elif market == "BINANCE":
            # btcusd
            sym = sym.replace("XBT", "BTC").lower()
            return sym
        elif market == "BITFINEX":
            # BTCUSD
            return sym.replace("XBT", "BTC")
        elif market == "HUOBI":
            # btcusd
            return sym.replace("XBT", "BTC").lower()
        elif market == "BITMEX":
            return sym
        else:
            raise ValueError("Market not handled: " + market)
    elif is_future(sym):
        # from: XBTUSD/Futures/210129 to:
        if market == "KRAKEN":
            # FI_XBTUSD_YYMMDD
            return "FI_" + sym.replace("/Future", "").replace("/", "_")
        elif market == "BITMEX":
            # XBTUSDF29
            # XBTH21
            return sym.split("/Future")[0].replace("USD", "") + get_month_code(int(sym[-4:-2])) + sym[-6:-4]
        elif market == "HUOBI":
            # BTC201225
            return sym.replace("XBT", "BTC").replace("USD/Future/", "")
        elif market == "BINANCE":
            # BTCUSD_210326
            return sym.replace("XBT", "BTC").replace("/Future/", "_").lower()
        else:
            return sym
    else:
        raise ValueError("Unknow instrument type for " + sym)


def format_sym_from_market(sym, market):
    """
    From sym of ECN format, get format that we use, ie:

    spot: XBTUSD
    future:XBTUSD/Future/YYMMDD

    :param sym:
    :param market:
    :return:
    """
    if market == "KRAKEN":
        if is_spot_market_ticker(sym, market):
            # XBT/USD
            return sym.replace("/", "")
        elif is_future_market_ticker(sym, market):
            # FI_XBTUSD_YYMMDD
            return sym.replace("FI_", "").split("_")[0] + "/Future/" + sym[-6:]
        else:
            raise ValueError("Instrument type not supported:", sym)
    elif market == "COINBASE":
        if is_spot_market_ticker(sym, market):
            # BTC-USD
            return sym.replace("-", "").replace("BTC", "XBT")
        else:
            raise ValueError("Instrument type not supported:", sym)
    elif market == "BINANCE":
        if is_spot_market_ticker(sym, market):
            # btcusd
            return sym.upper().replace("BTC", "XBT")
        elif is_future_market_ticker(sym, market):
            # BTCUSD_210326
            return sym.replace("BTC", "XBT").replace("_", "/Future/")
        else:
            raise ValueError("Instrument type not supported:", sym)
    elif market == "BITFINEX":
        if is_spot_market_ticker(sym, market):
            # BTCUSD
            return sym.replace("BTC", "XBT")
        else:
            raise ValueError("Instrument type not supported:", sym)
    elif market == "HUOBI":
        if is_spot_market_ticker(sym, market):
            # btcusd
            return sym.upper().replace("BTC", "XBT")
        elif is_future_market_ticker(sym, market):
            # BTC201225
            return sym.replace("BTC", "XBT")[:-6] + "USD" + "/Future/" + sym[-6:]
        else:
            raise ValueError("Instrument type not supported:", sym)
    elif market == "BITMEX":
        if is_spot_market_ticker(sym, market):
            # XBTUSD
            return sym
        elif is_future_market_ticker(sym, market):
            # XBTUSDF29
            month_str = get_month_str(sym[-3])
            year = datetime.datetime.now().year
            if len(sym[:-3]) >= 6:
                s = sym[:-3]
            else:
                s = sym[:-3] + "USD"
            return s + "/Future/" + str(year)[-2:] + month_str + sym[-2:]
        else:
            raise ValueError("Instrument type not supported:", sym)


def is_spot(sym):
    return not is_future(sym)


def is_future(sym):
    return "/Future/" in sym


def is_spot_market_ticker(sym, market):
    return get_instrument_type_from_market_ticker(sym, market) == "Spot"


def is_future_market_ticker(sym, market):
    return get_instrument_type_from_market_ticker(sym, market) == "Future"


def get_instrument_type_from_market_ticker(sym, market):
    if market == "KRAKEN":
        # spot: XBT/USD
        # future: FI_XBTUSD_YYMMDD
        if sym[:3] == "FI_":
            return "Future"
        else:
            return "Spot"
    elif market == "COINBASE":
        # spot:  BTC-USD
        # only spot
        return "Spot"
    elif market == "BINANCE":
        # spot: btcusd
        # future: BTCUSD_210326
        if "_" in sym:
            month_int = int(sym[-4:-2])
            year = datetime.datetime.now().year
            if get_last_friday_of_the_month(datetime.datetime(year, month_int, 1)) == datetime.datetime(year, month_int,
                                                                                                        int(sym[-2:])):
                return "Future"
        else:
            return "Spot"
    elif market == "BITFINEX":
        # spot: BTCUSD
        # only spot
        return "Spot"
    elif market == "HUOBI":
        # spot: btcusd
        # future: BTC201225
        for i in sym[-6:]:
            if not i.isdigit():
                return "Spot"
        else:
            return "Future"
    elif market == "BITMEX":
        # spot: XBTUSD
        # future: XBTH21
        if hasNumbers(sym):
            # check that the last friday of the expiry month is the day of expiry
            month_int = get_month_int(sym[-3])
            year = datetime.datetime.now().year
            if get_last_friday_of_the_month(datetime.datetime(year, month_int, 1)) == datetime.datetime(year, month_int,
                                                                                                        int(sym[-2:])):
                return "Future"
        else:
            return "Spot"
