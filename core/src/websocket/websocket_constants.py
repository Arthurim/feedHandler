#!/usr/bin/env python3
"""
@author: Arthurim
"""

from core.src.sym_handler import SPOT, FUTURE

WS_PUBLIC_URL_KRAKEN_SPOT = "wss://ws.kraken.com"
WS_PRIVATE_URL_KRAKEN_SPOT = "wss://ws-auth.kraken.com/"
WS_PUBLIC_URL_KRAKEN_FUTURE = "wss://futures.kraken.com/ws/v1"
WS_PRIVATE_URL_KRAKEN_FUTURE = "wss://futures.kraken.com/ws/v1"


# wss://api.futures.kraken.com/ws/v1


def get_url_for_market(market, instrument_type, public):
    """

    :param market: string
    :param public: boolean
    :return:
    """
    if market == "KRAKEN":
        if instrument_type == SPOT:
            if public:
                return WS_PUBLIC_URL_KRAKEN_SPOT
            else:
                return WS_PRIVATE_URL_KRAKEN_SPOT
        elif instrument_type == FUTURE:
            return WS_PUBLIC_URL_KRAKEN_FUTURE
        else:
            raise ValueError("Instrument type not supported:", instrument_type)
