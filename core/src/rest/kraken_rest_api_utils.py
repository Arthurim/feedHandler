#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""

from core.src.rest.kraken_rest_api_future import API_future
from core.src.rest.kraken_rest_api_spot import API_spot
from core.src.sym_handler import SPOT, FUTURE


def create_kraken_api(instrument_type, account="arthur"):
    with open("C:/dev/data/kraken/k" + account + "_" + instrument_type + ".txt") as f:
        content = f.readlines()
    public_key = content[0][:-1]
    private_key = content[1]
    if instrument_type == SPOT:
        return API_spot(apiPublicKey=public_key, apiPrivateKey=private_key)
    elif instrument_type == FUTURE:
        return API_future(apiPublicKey=public_key, apiPrivateKey=private_key)
    else:
        raise ValueError("Instrument type not supported:", instrument_type)


def get_kraken_public_key(instrument_type, account):
    with open("C:/dev/data/kraken/k" + account + "_" + instrument_type + ".txt") as f:
        content = f.readlines()
    return content[0][:-1]


def get_kraken_private_key(instrument_type, account):
    with open("C:/dev/data/kraken/k" + account + "_" + instrument_type + ".txt") as f:
        content = f.readlines()
    return content[1]