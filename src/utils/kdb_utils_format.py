#!/usr/bin/env python3
"""
@author: Arthurim
@Description: Functions utils relating to kdb
"""
from .sym_handler import is_spot


def convert_sym_to_kdb_format(sym):
    """
    Formats the sym from the API to the sym format we have in kdb
    :param sym:
    :return:
    """
    if is_spot(sym):
        return sym.replace("-", "").replace("/", "").replace("BTC", "XBT")
    else:
        return sym.replace("-", "").replace("_", "/").replace("BTC", "XBT")


def has_kdb_format_time(d_time):
    """
    Parses the input and returns True if it has kdb time format: "16:45:11.123456"
    :param d_time: string
    :return: boolean
    """
    millis_res = False
    hour_min_sec_res = False
    try:
        if len(d_time.split(".")) == 2:
            d_times_plits = d_time.split(".")
            hour_min_sec = d_times_plits[0]
            millis = int(d_times_plits[1])
            if len(hour_min_sec.split(":")) == 3:
                hour_min_sec_split = hour_min_sec.split(":")
                hour = int(hour_min_sec_split[0])
                minutes = int(hour_min_sec_split[1])
                sec = int(hour_min_sec_split[2])
                if hour < 25 and minutes < 60 and sec < 60:
                    hour_min_sec_res = True
            if millis <= 999999:
                millis_res = True
        return millis_res & hour_min_sec_res
    except:
        return False


def has_kdb_format_date(d_date):
    """
    Parses the input and returns True if it has kdb date format: "2020.10.12"
    :param d_date: string
    :return: boolean
    """
    res = False
    try:
        if len(d_date.split(".")) == 3:
            d_date_splits = d_date.split(".")
            d_date_year = int(d_date_splits[0])
            d_date_month = int(d_date_splits[1])
            d_date_day = int(d_date_splits[2])
            if len(str(d_date_year / 1000).split(".")[0]) == 1:
                if d_date_month > 0 & d_date_month < 13:
                    if d_date_day > 0 & d_date_day < 32:
                        res = True
        return res
    except:
        return False


def has_kdb_format_timestamp(d):
    """
    Parses the input and returns True if it has kdb Timestamp format: "2020.10.12D14:45:17.123456"
    :param d: string
    :return: boolean
    """
    res = False
    try:
        if type(d) == str:
            d_splits = d.split("D")
            if len(d_splits) == 2:
                d_date = d_splits[0]
                d_time = d_splits[1]
                res = has_kdb_format_date(d_date) & has_kdb_format_time(d_time)
        return res
    except:
        return False
