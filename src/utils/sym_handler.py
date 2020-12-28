#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""


def get_sym_format(sym, market):
    """

    :param sym: should be a string of 6 letters
    :param market: should be a string
    :return: string in the right format for the given market
    """
    if not len(sym) == 6:
        raise ValueError("sym input should be a string of 6 letters, it was: ", sym)
    if market == "KRAKEN":
        return sym[0:3] + "/" + sym[3:6]
    else:
        return sym
