#!/usr/bin/env python3
"""
@author: Arthurim
@Description:
"""

import base64
import hashlib
import hmac


def sign_message(apiPrivateKey, endpoint, postData, nonce=""):
    # step 1: concatenate postData, nonce + endpoint
    message = postData + nonce + endpoint

    # step 2: hash the result of step 1 with SHA256
    sha256_hash = hashlib.sha256()
    sha256_hash.update(message.encode('utf8'))
    hash_digest = sha256_hash.digest()

    # step 3: base64 decode apiPrivateKey
    secretDecoded = base64.b64decode(apiPrivateKey)

    # step 4: use result of step 3 to has the result of step 2 with HMAC-SHA512
    hmac_digest = hmac.new(secretDecoded, hash_digest, hashlib.sha512).digest()

    # step 5: base64 encode the result of step 4 and return
    return base64.b64encode(hmac_digest)


def sign_challenge(api_secret, challenge):
    """Signed a challenge received from Crypto Facilities Ltd"""
    # step 1: hash the message with SHA256
    sha256_hash = hashlib.sha256()
    sha256_hash.update(challenge.encode("utf8"))
    hash_digest = sha256_hash.digest()

    # step 3: base64 decode apiPrivateKey
    secret_decoded = base64.b64decode(api_secret)

    # step 4: use result of step 3 to has the result of step 2 with HMAC-SHA512
    hmac_digest = hmac.new(secret_decoded, hash_digest, hashlib.sha512).digest()

    # step 5: base64 encode the result of step 4 and return
    sch = base64.b64encode(hmac_digest).decode("utf-8")
    return sch
