import asyncio
import base64
import hashlib
import hmac
import json
import unittest
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest

from hummingbot.connector.derivative.kraken_perpetual import kraken_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_auth import (
    KrakenPerpetualAuth,
    KrakenPerpetualAuthError,
)
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class TestKrakenPerpetualAuth(unittest.TestCase):
    # Constants for testing - using actual demo keys
    API_KEY = "aERLqo0QGjwVL1Wic3V8xLtcQzI9ZUu9X032t5RT8MiZoSGuZVH2Hf+K"
    SECRET_KEY = "pinFbd7ga50rtG1t51rer+XBrJcIUxmzuMshMkGWudTbT51S4ktnIKLAiKAO1J6sv+aqmrUgd3uEW/KwTRTt/Mpb"
    MOCK_NONCE = "1642000000"

    # Mock responses matching Kraken's format
    MOCK_BALANCE_RESPONSE = {
        "result": "success",
        "serverTime": "2024-01-19T10:38:19.059Z",
        "accounts": {
            "flex_account": {
                "balances": {
                    "USD": "10000.0000",
                    "XBT": "1.0000"
                }
            }
        }
    }

    MOCK_ORDER_RESPONSE = {
        "result": "success",
        "serverTime": "2024-01-19T10:38:19.059Z",
        "sendStatus": {
            "order_id": "order_id_123",
            "status": "placed",
            "receivedTime": "2024-01-19T10:38:19.059Z",
            "orderEvents": []
        }
    }

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000
        self.auth = KrakenPerpetualAuth(
            api_key=self.API_KEY,
            secret_key=self.SECRET_KEY,
            time_provider=self.mock_time_provider,
        )

    def _get_test_url(self, endpoint: str) -> str:
        """Helper to build test URLs using constants"""
        return CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN] + endpoint

    def test_auth_headers_for_get_accounts(self):
        """Test authentication for GET /accounts endpoint"""
        url = self._get_test_url(CONSTANTS.GET_WALLET_BALANCE_PATH_URL)
        params = {"nonce": self.MOCK_NONCE}
        
        request = RESTRequest(
            method=RESTMethod.GET,
            url=url,
            params=params,
            is_auth_required=True,
        )

        with patch.object(self.auth, '_get_nonce', return_value=self.MOCK_NONCE):
            request = asyncio.get_event_loop().run_until_complete(
                self.auth.rest_authenticate(request)
            )

        # Verify headers
        self.assertEqual(request.headers["Content-Type"], "application/json")
        self.assertEqual(request.headers["Accept"], "application/json")
        self.assertEqual(request.headers["APIKey"], self.API_KEY)
        self.assertTrue("Authent" in request.headers)
        self.assertEqual(request.headers["Nonce"], self.MOCK_NONCE)

        # Verify URL and params
        self.assertEqual(str(request.url), url)
        self.assertEqual(request.params["nonce"], self.MOCK_NONCE)

    def test_auth_headers_for_post_order(self):
        """Test authentication for POST /sendorder endpoint"""
        url = self._get_test_url(CONSTANTS.PLACE_ACTIVE_ORDER_PATH_URL)
        order_data = {
            "orderType": CONSTANTS.ORDER_TYPE_MAP[OrderType.LIMIT],
            "symbol": "PI_XBTUSD",
            "side": "buy",
            "size": 1,
            "limitPrice": 50000,
            "nonce": self.MOCK_NONCE
        }
        
        request = RESTRequest(
            method=RESTMethod.POST,
            url=url,
            data=json.dumps(order_data),
            is_auth_required=True
            )

        with patch.object(self.auth, '_get_nonce', return_value=self.MOCK_NONCE):
            request = asyncio.get_event_loop().run_until_complete(
                self.auth.rest_authenticate(request)
            )

        # Verify headers
        self.assertEqual(request.headers["Content-Type"], "application/json")
        self.assertEqual(request.headers["Accept"], "application/json")
        self.assertEqual(request.headers["APIKey"], self.API_KEY)
        self.assertTrue("Authent" in request.headers)
        self.assertEqual(request.headers["Nonce"], self.MOCK_NONCE)

        # Verify URL and data
        self.assertEqual(str(request.url), url)
        parsed_data = json.loads(request.data)
        self.assertEqual(parsed_data["orderType"], "lmt")
        self.assertEqual(parsed_data["side"], "buy")
        self.assertEqual(parsed_data["symbol"], "PI_XBTUSD")
        self.assertEqual(parsed_data["nonce"], self.MOCK_NONCE)

    def test_extract_endpoint_path(self):
        """Test endpoint path extraction from URLs"""
        test_cases = [
            (
                self._get_test_url(CONSTANTS.GET_WALLET_BALANCE_PATH_URL),
                "/derivatives/api/v3/accounts"  # Full path expected from Kraken API
            ),
            (
                self._get_test_url(CONSTANTS.PLACE_ACTIVE_ORDER_PATH_URL),
                "/derivatives/api/v3/sendorder"  # Full path expected from Kraken API
            ),
        ]

        for url, expected_path in test_cases:
            print(f"\nTesting URL extraction:")
            print(f"Full URL: {url}")
            extracted_path = self.auth._extract_endpoint_path(url)
            print(f"Extracted path: {extracted_path}")
            print(f"Expected path: {expected_path}")
            
            # For auth string generation
            post_data = "nonce=1234567890"
            message = f"{post_data}{self.MOCK_NONCE}{extracted_path}"
            print(f"Auth message that would be signed: {message}")
            
            self.assertEqual(extracted_path, expected_path)

    def test_auth_string_generation_get_request(self):
        """Test auth string generation for GET request"""
        endpoint_path = "/api/v3/accounts"  # Without /derivatives prefix
        params = {"nonce": self.MOCK_NONCE}
        post_data = "&".join([f"{key}={value}" for key, value in sorted(params.items())])

        print(f"\nTesting GET request auth:")
        print(f"Endpoint path: {endpoint_path}")
        print(f"Post data: {post_data}")

        auth_string = self.auth._generate_auth_string(post_data, self.MOCK_NONCE, endpoint_path)

        # Verify auth string generation steps
        message = f"{post_data}{self.MOCK_NONCE}{endpoint_path}"
        print(f"Final message to sign: {message}")
        
        sha256_hash = hashlib.sha256()
        sha256_hash.update(message.encode('utf8'))
        hash_digest = sha256_hash.digest()
        
        secret_decoded = base64.b64decode(self.SECRET_KEY)
        hmac_digest = hmac.new(secret_decoded, hash_digest, hashlib.sha512).digest()
        expected_auth_string = base64.b64encode(hmac_digest).decode('utf-8')

        self.assertEqual(auth_string, expected_auth_string)

    def test_auth_string_generation_post_request(self):
        """Test auth string generation for POST request"""
        endpoint_path = "/api/v3/sendorder"  # Without /derivatives prefix
        order_data = {
            "orderType": "lmt",
            "symbol": "PI_XBTUSD",
            "side": "buy",
            "size": 1,
            "limitPrice": 50000,
            "nonce": self.MOCK_NONCE
        }
        post_data = json.dumps(order_data)

        print(f"\nTesting POST request auth:")
        print(f"Endpoint path: {endpoint_path}")
        print(f"Post data: {post_data}")

        auth_string = self.auth._generate_auth_string(post_data, self.MOCK_NONCE, endpoint_path)

        # Verify auth string generation with JSON data
        message = f"{post_data}{self.MOCK_NONCE}{endpoint_path}"
        print(f"Final message to sign: {message}")
        
        sha256_hash = hashlib.sha256()
        sha256_hash.update(message.encode('utf8'))
        hash_digest = sha256_hash.digest()
        
        secret_decoded = base64.b64decode(self.SECRET_KEY)
        hmac_digest = hmac.new(secret_decoded, hash_digest, hashlib.sha512).digest()
        expected_auth_string = base64.b64encode(hmac_digest).decode('utf-8')

        self.assertEqual(auth_string, expected_auth_string)

    def test_invalid_url_extraction(self):
        """Test endpoint path extraction with invalid URL"""
        invalid_urls = [
            "https://invalid.url/test",  # Invalid domain
            "https://futures.kraken.com/test",  # Invalid path
            "https://futures.kraken.com/derivatives/test",  # Invalid API path
        ]
        
        for invalid_url in invalid_urls:
            print(f"\nTesting invalid URL: {invalid_url}")
            with self.assertRaises(ValueError) as cm:
                self.auth._extract_endpoint_path(invalid_url)
            print(f"Raised error: {str(cm.exception)}")


if __name__ == "__main__":
    unittest.main()
