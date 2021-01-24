#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import calendar
import datetime

from dateutil.relativedelta import relativedelta, FR

futures_months = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12
}


def get_month_int(month_code):
    if month_code in futures_months.keys():
        return futures_months[month_code]
    else:
        raise ValueError("Wrong month code:", month_code)


def get_month_str(month_code):
    month_code = get_month_int(month_code)
    if month_code < 10:
        return "0" + str(month_code)
    else:
        return str(month_code)


def get_month_code(month_number):
    if type(month_number) == int:
        if month_number >= 1 and month_number <= 12:
            for k in futures_months.keys():
                if futures_months[k] == month_number:
                    return k
        else:
            raise ValueError("Month should be an integer between 1 and 12, but was:", month_number)
    else:
        raise ValueError("Month should be an integer but was:", month_number, " of type", type(month_number))


def get_last_day_of_month(dt):
    """

    :param dt: datetime
    :return:
    """

    return datetime.datetime(dt.year, dt.month, calendar.monthrange(dt.year, dt.month)[1])


def get_last_friday_of_the_month(dt):
    """
    
    :param dt: datetime
    :return: 
    """
    return get_last_day_of_month(dt) + relativedelta(weekday=FR(-1))
