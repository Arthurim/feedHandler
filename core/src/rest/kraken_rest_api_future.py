#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""

import warnings

warnings.filterwarnings('ignore')
import urllib.request as urllib2
import ssl
import pandas as pd
import ast
import urllib.parse
import hashlib
import hmac
import base64
import time
from core.src.rest.kraken_rest_api import API


class API_future(API):
    def __init__(self, apiPublicKey="", apiPrivateKey="", timeout=10, checkCertificate=True, useNonce=False):
        super().__init__('https://futures.kraken.com/derivatives', apiPublicKey, apiPrivateKey, timeout,
                         checkCertificate, useNonce, 'api/v3')
        self.apiPath = "https://www.cryptofacilities.com/derivatives"

    def _nonce(self):
        """ Nonce counter.
        :returns: an always-increasing unsigned integer (up to 64 bits wide)
        """
        return int(1000 * time.time())

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

        urlpath = '/' + self.apiversion + "/" + method + "?" + "symbol=%s" % data["pair"]
        # return urlpath
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

        urlpath = '/' + self.apiversion + '/' + method

        headers = {
            'API-Key': self.apiPublicKey,
            'API-Sign': self.sign_message(urlpath)
        }

        return self._query(urlpath, data, headers, timeout=timeout)

    def get_instruments(self):
        endpoint = "/api/v3/instruments"
        return self.make_request("GET", endpoint)

    # returns market data for all instruments
    def get_tickers(self):
        endpoint = "/api/v3/tickers"
        return self.make_request("GET", endpoint)

    # returns the entire order book of a futures
    def get_orderbook(self, symbol):
        endpoint = "/api/v3/orderbook"
        postUrl = "symbol=%s" % symbol
        return self.make_request("GET", endpoint, postUrl=postUrl)

    # returns historical data for futures and indices
    def get_history(self, symbol, lastTime=""):
        endpoint = "/api/v3/history"
        if lastTime != "":
            postUrl = "symbol=%s&lastTime=%s" % (symbol, lastTime)
        else:
            postUrl = "symbol=%s" % symbol
        return self.make_request("GET", endpoint, postUrl=postUrl)

    ##### private endpoints #####

    # returns key account information
    # Deprecated because it returns info about the Futures margin account
    # Use get_accounts instead
    def get_account(self):
        endpoint = "/api/v3/account"
        return self.make_request("GET", endpoint)

    # returns key account information
    def get_accounts(self):
        endpoint = "/api/v3/accounts"
        return self.make_request("GET", endpoint)

    # places an order
    def send_order(self, orderType, symbol, side, size, limitPrice, stopPrice=None, clientOrderId=None):
        endpoint = "/api/v3/sendorder"
        postBody = "orderType=%s&symbol=%s&side=%s&size=%s&limitPrice=%s" % (orderType, symbol, side, size, limitPrice)

        if orderType == "stp" and stopPrice is not None:
            postBody += "&stopPrice=%s" % stopPrice

        if clientOrderId is not None:
            postBody += "&cliOrdId=%s" % clientOrderId

        return self.make_request("POST", endpoint, postBody=postBody)

    # places an order
    def send_order_1(self, order):
        endpoint = "/api/v3/sendorder"
        postBody = urllib.parse.urlencode(order)
        return self.make_request("POST", endpoint, postBody=postBody)

    # edit an order
    def edit_order(self, edit):
        endpoint = "/api/v3/editorder"
        postBody = urllib.parse.urlencode(edit)
        return self.make_request("POST", endpoint, postBody=postBody)

    # cancels an order
    def cancel_order(self, order_id=None, cli_ord_id=None):
        endpoint = "/api/v3/cancelorder"

        if order_id is None:
            postBody = "cliOrdId=%s" % cli_ord_id
        else:
            postBody = "order_id=%s" % order_id

        return self.make_request("POST", endpoint, postBody=postBody)

    # cancel all orders
    def cancel_all_orders(selfs, symbol=None):
        endpoint = "/api/v3/cancelallorders"
        if symbol is not None:
            postbody = "symbol=%s" % symbol
        else:
            postbody = ""

        return selfs.make_request("POST", endpoint, postBody=postbody)

    # cancel all orders after
    def cancel_all_orders_after(selfs, timeoutInSeconds=60):
        endpoint = "/api/v3/cancelallordersafter"
        postbody = "timeout=%s" % timeoutInSeconds

        return selfs.make_request("POST", endpoint, postBody=postbody)

    # places or cancels orders in batch
    def send_batchorder(self, jsonElement):
        endpoint = "/api/v3/batchorder"
        postBody = "json=%s" % jsonElement
        return self.make_request("POST", endpoint, postBody=postBody)

    # returns all open orders
    def get_openorders(self):
        endpoint = "/api/v3/openorders"
        return self.make_request("GET", endpoint)

    # returns filled orders
    def get_fills(self, lastFillTime=""):
        endpoint = "/api/v3/fills"
        if lastFillTime != "":
            postUrl = "lastFillTime=%s" % lastFillTime
        else:
            postUrl = ""
        return self.make_request("GET", endpoint, postUrl=postUrl)

    # returns all open positions
    def get_openpositions(self):
        endpoint = "/api/v3/openpositions"
        return self.make_request("GET", endpoint)

    # return the user recent orders
    def get_recentorders(self, symbol=""):
        endpoint = "/api/v3/recentorders"
        if symbol != "":
            postUrl = "symbol=%s" % symbol
        else:
            postUrl = ""
        return self.make_request("GET", endpoint, postUrl=postUrl)

    # sends an xbt withdrawal request
    def send_withdrawal(self, targetAddress, currency, amount):
        endpoint = "/api/v3/withdrawal"
        postBody = "targetAddress=%s&currency=%s&amount=%s" % (targetAddress, currency, amount)
        return self.make_request("POST", endpoint, postBody=postBody)

    def send_withdrawal_to_spot(self, currency, amount):
        endpoint = "/api/v3/withdrawal"
        postBody = "currency=%s&amount=%s" % (currency, amount)
        return self.make_request("POST", endpoint, postBody=postBody)

    # returns xbt transfers
    def get_transfers(self, lastTransferTime=""):
        endpoint = "/api/v3/transfers"
        if lastTransferTime != "":
            postUrl = "lastTransferTime=%s" % lastTransferTime
        else:
            postUrl = ""
        return self.make_request("GET", endpoint, postUrl=postUrl)

    # returns all notifications
    def get_notifications(self):
        endpoint = "/api/v3/notifications"
        return self.make_request("GET", endpoint)

    # makes an internal transfer
    def transfer(self, fromAccount, toAccount, unit, amount):
        endpoint = "/api/v3/transfer"
        postBody = "fromAccount=%s&toAccount=%s&unit=%s&amount=%s" % (fromAccount, toAccount, unit, amount)
        return self.make_request("POST", endpoint, postBody=postBody)

    # signs a message
    def sign_message(self, endpoint, postData, nonce=""):
        # step 1: concatenate postData, nonce + endpoint
        message = postData + nonce + endpoint

        # step 2: hash the result of step 1 with SHA256
        sha256_hash = hashlib.sha256()
        sha256_hash.update(message.encode('utf8'))
        hash_digest = sha256_hash.digest()

        # step 3: base64 decode apiPrivateKey
        secretDecoded = base64.b64decode(self.apiPrivateKey)

        # step 4: use result of step 3 to has the result of step 2 with HMAC-SHA512
        hmac_digest = hmac.new(secretDecoded, hash_digest, hashlib.sha512).digest()

        # step 5: base64 encode the result of step 4 and return
        return base64.b64encode(hmac_digest)

    # creates a unique nonce
    def get_nonce(self):
        # https://en.wikipedia.org/wiki/Modulo_operation
        self.nonce = (self.nonce + 1) & 8191
        return str(int(time.time() * 1000)) + str(self.nonce).zfill(4)

    # sends an HTTP request
    def make_request(self, requestType, endpoint, postUrl="", postBody=""):
        # create authentication headers
        postData = postUrl + postBody

        if self.useNonce:
            nonce = self.get_nonce()
            signature = self.sign_message(endpoint, postData, nonce=nonce)
            authentHeaders = {"APIKey": self.apiPublicKey, "Nonce": nonce, "Authent": signature}
        else:
            signature = self.sign_message(endpoint, postData)
            authentHeaders = {"APIKey": self.apiPublicKey, "Authent": signature}

        # create request
        url = self.apiPath + endpoint + "?" + postUrl
        request = urllib2.Request(url, str.encode(postBody), authentHeaders)
        request.get_method = lambda: requestType

        # read response
        if self.checkCertificate:
            response = urllib2.urlopen(request, timeout=self.timeout)
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            response = urllib2.urlopen(request, context=ctx, timeout=self.timeout)

        response = response.read().decode("utf-8")

        # return
        return response

    def get_bid(self, ticker):
        orderBook = ast.literal_eval(self.get_orderbook(ticker))
        bids = orderBook["orderBook"]["bids"]
        bid = bids[0][0]
        return bid

    def get_ask(self, ticker):
        orderBook = ast.literal_eval(self.get_orderbook(ticker))
        asks = orderBook["orderBook"]["asks"]
        ask = asks[0][0]
        return ask

    def get_mid(self, ticker):
        bid = self.get_bid(ticker)
        ask = self.get_ask(ticker)
        mid = (bid + ask) / 2
        return mid

    def get_ticker_info(self, ticker, info):
        all_tickers_info = pd.DataFrame(
            ast.literal_eval(self.get_instruments().replace("true", "True").replace("false", "False"))['instruments'])
        ticker_info = all_tickers_info[all_tickers_info["symbol"] == ticker]
        info = ticker_info[info].values[0]
        return info

    def get_time_to_expiry(self, ticker):
        time_to_expiry = abs(
            pd.datetime.today().day - pd.datetime.strptime(self.get_ticker_info(ticker, "lastTradingTime"),
                                                           "%Y-%m-%dT%H:%M:%S.%fZ").day) / 365
        return time_to_expiry

    def get_ticker(self, future_type, expiry):
        """
        Only 2 types of future:
            - monthly M
            - quaterly Q
        """
        all_tickers_info = pd.DataFrame(
            ast.literal_eval(self.get_instruments().replace("true", "True").replace("false", "False"))['instruments'])
        futures = all_tickers_info[all_tickers_info['symbol'].str.contains(future_type)]
        futures['lastTradingTime'] = futures['lastTradingTime'].apply(
            lambda x: pd.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))
        if expiry == "M":
            result = futures[futures['lastTradingTime'] == min(futures['lastTradingTime'])]['symbol'].values[0]
        elif expiry == "Q":
            result = futures[futures['lastTradingTime'] == max(futures['lastTradingTime'])]['symbol'].values[0]
        else:
            raise ValueError('Bad expiry type: "' + expiry + '". Must be "Q" or "M"')
        return result

    def get_cash(self, currency):
        accountInfo = ast.literal_eval(self.get_accounts())['accounts']
        return accountInfo['cash']['balances'][currency]
