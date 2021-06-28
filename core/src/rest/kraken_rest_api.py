#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""

import warnings

warnings.filterwarnings('ignore')
import requests


class API(object):

    def __init__(self, apiPath, apiPublicKey="", apiPrivateKey="", timeout=10, checkCertificate=True, useNonce=False,
                 apiversion='0'):
        self.apiPath = apiPath
        self.apiPublicKey = apiPublicKey
        self.apiPrivateKey = apiPrivateKey
        self.timeout = timeout
        self.nonce = 0
        self.apiversion = apiversion
        self.session = requests.Session()
        self.checkCertificate = checkCertificate
        self.useNonce = useNonce
        self.response = None
        self._json_options = {}

    def get_apiversion(self):
        return self.apiversion

    def json_options(self, **kwargs):
        """ Set keyword arguments to be passed to JSON deserialization.
        :param kwargs: passed to :py:meth:`requests.Response.json`
        :returns: this instance for chaining
        """
        self._json_options = kwargs
        return self

    def close(self):
        """ Close this session.
        :returns: None
        """
        self.session.close()
        return

    def load_key(self, path):
        """ Load key and secret from file.
        Expected file format is key and secret on separate lines.
        :param path: path to keyfile
        :type path: str
        :returns: None
        """
        with open(path, 'r') as f:
            self.apiPublicKey = f.readline().strip()
            self.apiPrivateKey = f.readline().strip()
        return

    def _query(self, urlpath, data, headers=None, timeout=None):
        """ Low-level query handling.
        .. note::
           Use :py:meth:`query_private` or :py:meth:`query_public`
           unless you have a good reason not to.
        :param urlpath: API URL path sans host
        :type urlpath: str
        :param data: API request parameters
        :type data: dict
        :param headers: (optional) HTTPS headers
        :type headers: dict
        :param timeout: (optional) if not ``None``, a :py:exc:`requests.HTTPError`
                        will be thrown after ``timeout`` seconds if a response
                        has not been received
        :type timeout: int or float
        :returns: :py:meth:`requests.Response.json`-deserialised Python object
        :raises: :py:exc:`requests.HTTPError`: if response status not successful
        """
        if data is None:
            data = {}
        if headers is None:
            headers = {}

        url = self.apiPath + urlpath

        self.response = self.session.post(url, data=data, headers=headers,
                                          timeout=timeout)

        return self.response.json(**self._json_options)
