#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""
import datetime
import unittest

from market_data.src.orderbooks import update_orderbook, compute_check_sum_input, compute_check_sum
from market_data.src.utils.orderbook_utils import convert_string_representation_of_orderbook_to_orderbook, \
    convert_orderbook_to_string_representation

DEBUG_TIME = 1


class TestShouldCorrectlyUpdateOrderbook(unittest.TestCase):

    def test_orderbook_conversion_to_string_representation(self):
        # Check we correctly convert orderbook_str to orderbook
        orderbook_str = {'sym': 'XBTUSD', 'market': 'KRAKEN', 'marketTimestamp': '',
                         'bid': {'33035.10000': '5.12965941',
                                 '33035.00000': '0.01150000',
                                 '33034.00000': '0.15067597'},
                         'ask': {'33035.20000': '0.49527187',
                                 '33044.80000': '0.30597849',
                                 '33047.50000': '0.75679694'}}
        expected_orderbook = {'sym': 'XBTUSD', 'market': 'KRAKEN', 'marketTimestamp': '',
                              'bid': {33035.1: 5.12965941,
                                      33035.0: 0.0115,
                                      33034.0: 0.15067597},
                              'ask': {33035.2: 0.49527187,
                                      33044.8: 0.30597849,
                                      33047.50: 0.75679694}}

        orderbook = convert_string_representation_of_orderbook_to_orderbook(orderbook_str)
        self.assertTrue(orderbook == expected_orderbook,
                        "Failed to convert string orderbook representation to orderbook")

        # Check we correctly convert orderbook to its string representation
        expected_orderbook_str = {'sym': 'XBTUSD', 'market': 'KRAKEN', 'marketTimestamp': '',
                                  'bid': {'33035.10000': '5.12965941',
                                          '33035.00000': '0.01150000',
                                          '33034.00000': '0.15067597'},
                                  'ask': {'33035.20000': '0.49527187',
                                          '33044.80000': '0.30597849',
                                          '33047.50000': '0.75679694'}}
        orderbook = {'sym': 'XBTUSD', 'market': 'KRAKEN', 'marketTimestamp': '',
                     'bid': {33035.1: 5.12965941,
                             33035.0: 0.0115,
                             33034.0: 0.15067597},
                     'ask': {33035.2: 0.49527187,
                             33044.8: 0.30597849,
                             33047.50: 0.75679694}}

        orderbook_str = convert_orderbook_to_string_representation(orderbook)
        self.assertTrue(orderbook_str == expected_orderbook_str,
                        "Failed to convert orderbook to its string representation")

        # finally check that the of the composition of the 2 functions yields to the initial result
        self.assertTrue(convert_orderbook_to_string_representation(
            convert_string_representation_of_orderbook_to_orderbook(orderbook_str)) == orderbook_str,
                        "Failed composition for orderbook_str")
        self.assertTrue(convert_string_representation_of_orderbook_to_orderbook(
            convert_orderbook_to_string_representation(orderbook)) == orderbook, "Failed composition for orderbook")

    def test_orderbook_update(self):
        orderbook = {"sym": "XBTUSD", "market": "KRAKEN", "marketTimestamp": "",
                     "bid": {32190.30000: 0.04579906,
                             32189.90000: 0.28206982,
                             32189.50000: 0.10000000,
                             32187.80000: 0.15492926,
                             32187.40000: 0.19089654,
                             32187.30000: 0.54200000,
                             32186.70000: 0.39760000,
                             32186.60000: 0.10000000,
                             32184.80000: 0.07650675,
                             32184.70000: 0.42340000},
                     "ask": {32199.70000: 0.80291486,
                             32200.00000: 0.10000000,
                             32201.20000: 0.31061608,
                             32201.40000: 0.77653633,
                             32202.20000: 1.55334945,
                             32204.50000: 0.29346981,
                             32204.90000: 0.03105114,
                             32205.60000: 1.98440000,
                             32206.80000: 0.07650649,
                             32207.10000: 0.23251983}}
        orderbook_str = convert_orderbook_to_string_representation(orderbook)

        update = [320,
                  {'b': [['32186.70000', '0.39760000', '1624651751.611720']], 'marketTimestamp': '1624651751.61172',
                   'c': '1153505366'}, 'book-10', 'XBT/USD']
        orderbook_expected = {"sym": "XBTUSD", "market": "KRAKEN", "marketTimestamp": "2021.06.25D21:09:11.611720",
                              "bid": {32190.30000: 0.04579906,
                                      32189.90000: 0.28206982,
                                      32189.50000: 0.10000000,
                                      32187.80000: 0.15492926,
                                      32187.40000: 0.19089654,
                                      32187.30000: 0.54200000,
                                      32186.70000: 0.39760000,
                                      32186.60000: 0.10000000,
                                      32184.80000: 0.07650675,
                                      32184.70000: 0.42340000},
                              "ask": {32199.70000: 0.80291486,
                                      32200.00000: 0.10000000,
                                      32201.20000: 0.31061608,
                                      32201.40000: 0.77653633,
                                      32202.20000: 1.55334945,
                                      32204.50000: 0.29346981,
                                      32204.90000: 0.03105114,
                                      32205.60000: 1.98440000,
                                      32206.80000: 0.07650649,
                                      32207.10000: 0.23251983}}

        orderbook_str_expected = convert_orderbook_to_string_representation(orderbook_expected)
        orderbook, orderbook_str = update_orderbook([orderbook, orderbook_str], update)

        self.assertTrue(orderbook_str_expected == orderbook_str,
                        "Wrong update of the string representation of the orderbook")
        self.assertTrue(orderbook_expected == orderbook, "Wrong update of the orderbook")

    def test_maintain_valid_orderbook(self):
        # From https://support.kraken.com/hc/en-us/articles/360027821131-How-to-maintain-a-valid-order-book-

        # Test change volume for already present price's level
        orderbook_str = {'sym': 'XBTUSD', 'market': 'KRAKEN', 'marketTimestamp': '2021.06.25D21:24:09.233429',
                         "ask": {"5290.80000": "1.00000000",
                                 "5290.90000": "4.49956524",
                                 "5291.70000": "1.00000000",
                                 "5292.00000": "0.95388940",
                                 "5292.20000": "1.51300000",
                                 "5293.10000": "0.69800000",
                                 "5293.20000": "2.00000000",
                                 "5293.90000": "2.83200000",
                                 "5294.10000": "0.99600000",
                                 "5294.50000": "5.00000000"},
                         "bid": {"5290.10000": "1.43195600",
                                 "5289.80000": "2.00000000",
                                 "5289.40000": "0.49400000",
                                 "5289.20000": "0.89533312",
                                 "5287.40000": "3.23600000",
                                 "5287.30000": "3.33000000",
                                 "5287.00000": "10.20000000",
                                 "5286.00000": "3.86378703",
                                 "5285.70000": "6.40000000",
                                 "5283.90000": "0.50000000"}}
        update = [0, {"a": [["5293.10000", "0.39800000", "1556724673.104421"]], "c": "408163318"}, "book-10", "XBT/USD"]

        orderbook_str_expected = {'sym': 'XBTUSD', 'market': 'KRAKEN',
                                  'marketTimestamp': datetime.datetime.fromtimestamp(
                                      float(update[1]["a"][0][2])).strftime("%Y.%m.%dD%H:%M:%S.%f"),
                                  "ask": {"5290.80000": "1.00000000",
                                          "5290.90000": "4.49956524",
                                          "5291.70000": "1.00000000",
                                          "5292.00000": "0.95388940",
                                          "5292.20000": "1.51300000",
                                          "5293.10000": "0.39800000",
                                          "5293.20000": "2.00000000",
                                          "5293.90000": "2.83200000",
                                          "5294.10000": "0.99600000",
                                          "5294.50000": "5.00000000"},
                                  "bid": {"5290.10000": "1.43195600",
                                          "5289.80000": "2.00000000",
                                          "5289.40000": "0.49400000",
                                          "5289.20000": "0.89533312",
                                          "5287.40000": "3.23600000",
                                          "5287.30000": "3.33000000",
                                          "5287.00000": "10.20000000",
                                          "5286.00000": "3.86378703",
                                          "5285.70000": "6.40000000",
                                          "5283.90000": "0.50000000"}}

        orderbook, orderbook_str = update_orderbook(
            [convert_string_representation_of_orderbook_to_orderbook(orderbook_str), orderbook_str], update)
        self.assertTrue(orderbook_str_expected == orderbook_str,
                        "Wrong update of the string representation of the orderbook")
        self.assertTrue(convert_string_representation_of_orderbook_to_orderbook(orderbook_str_expected) == orderbook,
                        "Wrong update of the orderbook")
        check_sum = compute_check_sum(orderbook_str)
        expected_check_sum = int(update[1]["c"])
        self.assertTrue(check_sum == expected_check_sum,
                        "Wrong checksum")

        # Test add new level
        update = [0, {"a": [["5294.40000", "0.99600000", "1556724672.663220"]], "c": "393966308"}, "book-10", "XBT/USD"]
        orderbook, orderbook_str = update_orderbook(
            [convert_string_representation_of_orderbook_to_orderbook(orderbook_str), orderbook_str], update)
        orderbook_str_expected = {'sym': 'XBTUSD', 'market': 'KRAKEN',
                                  'marketTimestamp': datetime.datetime.fromtimestamp(
                                      float(update[1]["a"][0][2])).strftime("%Y.%m.%dD%H:%M:%S.%f"),
                                  "ask": {"5290.80000": "1.00000000",
                                          "5290.90000": "4.49956524",
                                          "5291.70000": "1.00000000",
                                          "5292.00000": "0.95388940",
                                          "5292.20000": "1.51300000",
                                          "5293.10000": "0.39800000",
                                          "5293.20000": "2.00000000",
                                          "5293.90000": "2.83200000",
                                          "5294.10000": "0.99600000",
                                          "5294.40000": "0.99600000"},
                                  "bid": {"5290.10000": "1.43195600",
                                          "5289.80000": "2.00000000",
                                          "5289.40000": "0.49400000",
                                          "5289.20000": "0.89533312",
                                          "5287.40000": "3.23600000",
                                          "5287.30000": "3.33000000",
                                          "5287.00000": "10.20000000",
                                          "5286.00000": "3.86378703",
                                          "5285.70000": "6.40000000",
                                          "5283.90000": "0.50000000"}}

        orderbook, orderbook_str = update_orderbook(
            [convert_string_representation_of_orderbook_to_orderbook(orderbook_str), orderbook_str], update)
        self.assertTrue(orderbook_str_expected == orderbook_str,
                        "Wrong update of the string representation of the orderbook")
        self.assertTrue(convert_string_representation_of_orderbook_to_orderbook(orderbook_str_expected) == orderbook,
                        "Wrong update of the orderbook")
        check_sum = compute_check_sum(orderbook_str)
        expected_check_sum = int(update[1]["c"])
        self.assertTrue(check_sum == expected_check_sum,
                        "Wrong checksum")

        # Test remove one level and add a new one
        update = [0, {"a": [["5294.10000", "0.00000000", "1556724670.010241"],
                            ["5294.70000", "3.34000000", "1556724653.951982", "r"]], "c": "3679121060"}, "book-10",
                  "XBT/USD"]

        orderbook_str_expected = {'sym': 'XBTUSD', 'market': 'KRAKEN',
                                  'marketTimestamp': datetime.datetime.fromtimestamp(
                                      float(update[1]["a"][0][2])).strftime("%Y.%m.%dD%H:%M:%S.%f"),
                                  "ask": {"5290.80000": "1.00000000",
                                          "5290.90000": "4.49956524",
                                          "5291.70000": "1.00000000",
                                          "5292.00000": "0.95388940",
                                          "5292.20000": "1.51300000",
                                          "5293.10000": "0.39800000",
                                          "5293.20000": "2.00000000",
                                          "5293.90000": "2.83200000",
                                          "5294.40000": "0.99600000",
                                          "5294.70000": "3.34000000"},
                                  "bid": {"5290.10000": "1.43195600",
                                          "5289.80000": "2.00000000",
                                          "5289.40000": "0.49400000",
                                          "5289.20000": "0.89533312",
                                          "5287.40000": "3.23600000",
                                          "5287.30000": "3.33000000",
                                          "5287.00000": "10.20000000",
                                          "5286.00000": "3.86378703",
                                          "5285.70000": "6.40000000",
                                          "5283.90000": "0.50000000"}}
        orderbook, orderbook_str = update_orderbook(
            [convert_string_representation_of_orderbook_to_orderbook(orderbook_str), orderbook_str], update)
        self.assertTrue(orderbook_str_expected == orderbook_str,
                        "Wrong update of the string representation of the orderbook")
        self.assertTrue(convert_string_representation_of_orderbook_to_orderbook(orderbook_str_expected) == orderbook,
                        "Wrong update of the orderbook")
        check_sum = compute_check_sum(orderbook_str)
        expected_check_sum = int(update[1]["c"])
        self.assertTrue(check_sum == expected_check_sum,
                        "Wrong checksum")

    def test_basic_checksum(self):
        # https://docs.kraken.com/websockets/#book-checksum
        orderbook_str = {'sym': 'XBTUSD', 'market': 'KRAKEN', 'marketTimestamp': '2021.06.25D21:24:09.233429',
                         "ask": {"0.05005": "0.00000500",
                                 "0.05010": "0.00000500",
                                 "0.05015": "0.00000500",
                                 "0.05020": "0.00000500",
                                 "0.05025": "0.00000500",
                                 "0.05030": "0.00000500",
                                 "0.05035": "0.00000500",
                                 "0.05040": "0.00000500",
                                 "0.05045": "0.00000500",
                                 "0.05050": "0.00000500"},
                         "bid": {"0.05000": "0.00000500",
                                 "0.04995": "0.00000500",
                                 "0.04990": "0.00000500",
                                 "0.04980": "0.00000500",
                                 "0.04975": "0.00000500",
                                 "0.04970": "0.00000500",
                                 "0.04965": "0.00000500",
                                 "0.04960": "0.00000500",
                                 "0.04955": "0.00000500",
                                 "0.04950": "0.00000500"}}
        check_sum_input = compute_check_sum_input(orderbook_str)
        expected_check_sum_input = "50055005010500501550050205005025500503050050355005040500504550050505005000500499550049905004980500497550049705004965500496050049555004950500"
        self.assertTrue(check_sum_input == expected_check_sum_input,
                        "Wrong checksum input")
        check_sum = compute_check_sum(orderbook_str)
        expected_check_sum = 974947235
        self.assertTrue(check_sum == expected_check_sum,
                        "Wrong checksum")


if __name__ == "__main__":
    unittest.main()
    print("Everything passed")
