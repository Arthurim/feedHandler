#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""

import warnings

warnings.filterwarnings('ignore')
import urllib.parse
import hashlib
import hmac
import base64
import time

from core.src.sym_handler import format_sym_for_market
from core.src.rest.kraken_rest_api import API


class API_spot(API):
    def __init__(self, apiPublicKey="", apiPrivateKey="", timeout=10, checkCertificate=True, useNonce=False):
        super().__init__('https://api.kraken.com', apiPublicKey, apiPrivateKey, timeout, checkCertificate, useNonce,
                         '0')

    def query_public(self, method, data=None, timeout=None):
        """ Performs an API query that does not require a valid key/secret pair.
        :param method: API method name
        :type method: str
        :param data: (optional) API request parameters
        :type data: dict
        :param timeout: (optional) if not ``None``, a :py:exc:`requests.HTTPError`
                        will be thrown after ``timeout`` seconds if a response
                        has not been received
        :type timeout: int or float
        :returns: :py:meth:`requests.Response.json`-deserialised Python object
        """
        if data is None:
            data = {}

        urlpath = '/' + self.apiversion + '/public/' + method

        return self._query(urlpath, data, timeout=timeout)

    def query_private(self, method, data=None, timeout=None):
        """ Performs an API query that requires a valid key/secret pair.
        :param method: API method name
        :type method: str
        :param data: (optional) API request parameters
        :type data: dict
        :param timeout: (optional) if not ``None``, a :py:exc:`requests.HTTPError`
                        will be thrown after ``timeout`` seconds if a response
                        has not been received
        :type timeout: int or float
        :returns: :py:meth:`requests.Response.json`-deserialised Python object
        """
        if data is None:
            data = {}

        if not self.apiPublicKey or not self.apiPrivateKey:
            raise Exception('Either key or secret is not set! (Use `load_key()`.')

        data['nonce'] = self._nonce()

        urlpath = '/' + self.apiversion + '/private/' + method

        headers = {
            'API-Key': self.apiPublicKey,
            'API-Sign': self._sign(data, urlpath)
        }

        return self._query(urlpath, data, headers, timeout=timeout)

    def _nonce(self):
        """ Nonce counter.
        :returns: an always-increasing unsigned integer (up to 64 bits wide)
        """
        return int(1000 * time.time())

    def _sign(self, data, urlpath):
        """ Sign request data according to Kraken's scheme.
        :param data: API request parameters
        :type data: dict
        :param urlpath: API URL path sans host
        :type urlpath: str
        :returns: signature digest
        """
        postdata = urllib.parse.urlencode(data)

        # Unicode-objects must be encoded before hashing
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()

        signature = hmac.new(base64.b64decode(self.apiPrivateKey),
                             message, hashlib.sha512)
        sigdigest = base64.b64encode(signature.digest())

        return sigdigest.decode()

    def get_bid(self, ticker):
        tickerinfo = self.query_public("Ticker", {"pair": ticker})
        bid = tickerinfo['result'][self.format_sym_for_rest_api(ticker)]['b'][0]
        return float(bid)

    def get_ask(self, ticker):
        tickerinfo = self.query_public("Ticker", {"pair": ticker})
        ask = tickerinfo['result'][self.format_sym_for_rest_api(ticker)]['a'][0]
        return float(ask)

    def get_mid(self, ticker):
        bid = self.get_bid(ticker)
        ask = self.get_ask(ticker)
        mid = (bid + ask) / 2
        return mid

    def get_balance(self, ccy):
        return self.query_private('Balance')['result'][ccy]

    def create_order(self, ticker, side, orderType, price, volume):
        response = self.query_private('AddOrder',
                                      {'pair': ticker,
                                       'type': side,
                                       'ordertype': orderType,
                                       'price': price,
                                       'volume': volume,
                                       })
        return response

    def transfer_from_spot_to_future(self, ccy, amount):
        response = self.query_private('WalletTransfer', {'asset': ccy,
                                                         'from': 'Spot Wallet',
                                                         'to': 'Futures Wallet',
                                                         'amount': amount})
        return response

    def get_private_token(self):
        return self.query_private("GetWebSocketsToken")["result"]["token"]

    def format_sym_for_rest_api(self, sym):
        if sym == "bchusd":
            return "BCHUSD"
        else:
            return "X" + sym.upper()[:3] + "Z" + sym.upper()[3:]
