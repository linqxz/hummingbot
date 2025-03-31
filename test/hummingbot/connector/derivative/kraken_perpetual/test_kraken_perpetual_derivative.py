import asyncio
import json
import re
import time
from copy import deepcopy
from decimal import Decimal
from itertools import chain, product
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

from aioresponses import aioresponses
from aioresponses.core import RequestCall

import hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_web_utils as web_utils
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_derivative import KrakenPerpetualDerivative
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderCancelledEvent  # Added imports


class KrakenPerpetualDerivativeTests(AbstractPerpetualDerivativeTests.PerpetualDerivativeTests):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)
        # Kraken API keys are 56 characters long
        cls.api_key = "kQH5HW/8p1uGOVjbgWA7FunAmGO4PaXfXFxmuwBoJLjdEv2mN/Eb8Tq8"
        # Kraken API secrets are 88 characters long
        cls.api_secret = "kQH5HW/8p1uGOVjbgWA7FunAmGO4PaXfXFxmuwBoJLjdEv2mN/Eb8Tq8n4p7QXp6ndy7/wB9yHWF7J3ABPxD9A=="
        cls.base_asset = "BTC"
        cls.quote_asset = "USD"
        cls.trading_pair = combine_to_hb_trading_pair(cls.base_asset, cls.quote_asset)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.loop.close()
        super().tearDownClass()

    @property
    def all_symbols_url(self):
        url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.QUERY_SYMBOL_ENDPOINT)
        return url

    @property
    def latest_prices_url(self):
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.TICKER_PRICE_ENDPOINT, trading_pair=self.trading_pair
        )
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def network_status_url(self):
        url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.SERVER_TIME_PATH_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.QUERY_SYMBOL_ENDPOINT)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.PLACE_ACTIVE_ORDER_PATH_URL, trading_pair=self.trading_pair
        )
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def balance_url(self):
        print("\n=== Getting balance URL ===")
        url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.GET_WALLET_BALANCE_PATH_URL)
        print(f"Generated balance URL: {url}")
        return url

    @property
    def funding_info_url(self):
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.TICKER_PRICE_ENDPOINT, trading_pair=self.trading_pair
        )
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    def configure_successful_set_position_mode(self, position_mode: PositionMode, mock_api: aioresponses,
                                               callback: Optional[Callable] = lambda *args, **kwargs: None) -> List[str]:
        """
        Configure the API response for setting position mode successfully.
        """
        if position_mode == PositionMode.ONEWAY:
            return []  # No API call needed since it's already in ONEWAY mode
        else:
            return []

    def configure_failed_set_position_mode(
            self,
            position_mode: PositionMode,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> Tuple[List[str], str]:
        """
        Configure the API response for failing to set position mode.
        """
        url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.SET_POSITION_MODE_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        error_msg = "Kraken only supports ONEWAY position mode"
        response = {"result": "error", "error": error_msg}
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return [url], error_msg

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]  # Kraken only supports one-way positions

    @property
    def funding_payment_url(self):
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.HISTORICAL_FUNDING_RATES_ENDPOINT, trading_pair=self.trading_pair
        )
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    def configure_all_symbols_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.all_symbols_url
        response = self.all_symbols_request_mock_response
        mock_api.get(url, body=json.dumps(response))
        return [url]

    def configure_trading_rules_response(
            self,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        print("\n=== Configuring trading rules response ===")
        url = self.trading_rules_url
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self.trading_rules_request_mock_response
        print(f"URL: {url}")
        print(f"Mock response: {response}")
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)

        # Add mock responses for initial margin and max order size endpoints
        margin_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.INITIAL_MARGIN_ENDPOINT)
        max_size_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.MAX_ORDER_SIZE_ENDPOINT)

        print("\n=== Configuring margin and size endpoints ===")
        print(f"Margin URL pattern: {margin_url}")
        print(f"Max size URL pattern: {max_size_url}")

        # Create regex patterns to match URLs with any symbol parameter
        margin_url_pattern = re.compile(f"^{margin_url}".replace(".", r"\.").replace("?", r"\?") + r"\?.*orderType=lmt.*")
        max_size_url_pattern = re.compile(f"^{max_size_url}".replace(".", r"\.").replace("?", r"\?") + r"\?.*orderType=lmt.*")

        print(f"Initial margin response: {self.initial_margin_mock_response}")
        print(f"Max order size response: {self.max_order_size_mock_response}")

        mock_api.get(margin_url_pattern, body=json.dumps(self.initial_margin_mock_response))
        mock_api.get(max_size_url_pattern, body=json.dumps(self.max_order_size_mock_response))

        return [url]

    def configure_erroneous_trading_rules_response(
            self,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> List[str]:
        print("\n=== Configuring erroneous trading rules response ===")
        url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.QUERY_SYMBOL_ENDPOINT)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self.trading_rules_request_erroneous_mock_response
        print(f"URL: {url}")
        print(f"Mock error response: {response}")
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)

        # Add mock error responses for initial margin and max order size endpoints
        margin_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.INITIAL_MARGIN_ENDPOINT)
        max_size_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.MAX_ORDER_SIZE_ENDPOINT)

        print("\n=== Configuring erroneous margin and size endpoints ===")
        print(f"Margin URL pattern: {margin_url}")
        print(f"Max size URL pattern: {max_size_url}")

        margin_url_pattern = re.compile(f"^{margin_url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        max_size_url_pattern = re.compile(f"^{max_size_url}".replace(".", r"\.").replace("?", r"\?") + ".*")

        suspended_response = {
            "result": "error",
            "error": "MARKET_SUSPENDED",
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

        print(f"Suspended market response: {suspended_response}")

        mock_api.get(margin_url_pattern, body=json.dumps(suspended_response))
        mock_api.get(max_size_url_pattern, body=json.dumps(suspended_response))

        return [url]

    @property
    def all_symbols_request_mock_response(self):
        mock_response = {
            "result": "success",
            "instruments": [
                {
                    "symbol": "PF_XBTUSD",  # Using Kraken's actual symbol format
                    "type": "flexible_futures",  # Kraken uses flex_futures for their perpetual contracts
                    "underlying": "XBT",     # XBT is Kraken's symbol for Bitcoin
                    "tickSize": "1",       # Kraken's actual tick size for BTC-USD
                    "contractSize": "1",     # 1 USD per contract
                    "tradeable": True,
                    "marginLevels": [
                        {
                            "contracts": 0,
                            "initialMargin": "0.02",    # 2% initial margin
                            "maintenanceMargin": "0.01"  # 1% maintenance margin
                        }
                    ],
                    "maxPositionSize": 100,
                    "fundingRateCoefficient": 24,
                    "contractValueTradePrecision": 4,
                    "postOnly": False,
                    "maxRelativeFundingRate": 0.0025,
                    "maxUnderlyingPosition": 100,
                    "base": "BTC",
                    "quote": "USD",
                    "pair": "BTC:USD",

                }
            ],
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        return mock_response

    @property
    def latest_prices_request_mock_response(self):
        mock_response = {
            "result": "success",
            "serverTime": "2025-01-09T18:52:43.97Z",
            "tickers": {
                "symbol": "PF_XBTUSD",
                "last": 9999.9,
                "lastTime": "2025-01-09T18:52:42.825Z",
                "tag": "perpetual",
                "pair": "XBT:USD",
                "markPrice": 2,
                "bid": 92986,
                "bidSize": 0.2203,
                "ask": 92987,
                "askSize": 0.66,
                "vol24h": 7317.5397,
                "volumeQuote": 683496735.9156,
                "openInterest": 1933.6742,
                "open24h": 94013,
                "high24h": 95383,
                "low24h": 91750,
                "lastSize": 0.01,
                "fundingRate": 3,
                "fundingRatePrediction": 0.3724459080887603,
                "suspended": False,
                "indexPrice": 92964.04,
                "postOnly": False,
                "change24h": -1.1
            }
        }
        return mock_response

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        mock_response = {
            "result": "success",
            "instruments": [
                {
                    "symbol": "PF_XBTUSD",  # Using Kraken's actual symbol format
                    "type": "flexible_futures",  # Kraken uses flex_futures for their perpetual contracts
                    "underlying": "XBT",     # XBT is Kraken's symbol for Bitcoin
                    "tickSize": "1",       # Kraken's actual tick size for BTC-USD
                    "contractSize": "1",     # 1 USD per contract
                    "tradeable": True,
                    "marginLevels": [
                        {
                            "contracts": 0,
                            "initialMargin": "0.02",    # 2% initial margin
                            "maintenanceMargin": "0.01"  # 1% maintenance margin
                        }
                    ],
                    "maxPositionSize": 100,
                    "fundingRateCoefficient": 24,
                    "contractValueTradePrecision": 4,
                    "postOnly": False,
                    "maxRelativeFundingRate": 0.0025,
                    "maxUnderlyingPosition": 100,
                    "base": "BTC",
                    "quote": "USD",
                    "pair": "BTC:USD",

                }
            ],
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        return "INVALID-PAIR", mock_response

    @property
    def network_status_request_successful_mock_response(self):
        mock_response = {
            "result": "success",
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        return mock_response

    @property
    def trading_rules_request_mock_response(self):
        return {
            "result": "success",
            "instruments": [{
                "symbol": "PF_XBTUSD",
                "type": "flex_futures",
                "underlying": "XBT",
                "tickSize": "0.5",
                "contractSize": "1",
                "tradeable": True,
                "marginLevels": [{
                    "contracts": 0,
                    "initialMargin": "0.02",
                    "maintenanceMargin": "0.01"
                }],
                "maxPositionSize": 100,
                "fundingRateCoefficient": 24,
                "contractValueTradePrecision": 4,
                "postOnly": False,
                "maxRelativeFundingRate": 0.0025,
                "maxUnderlyingPosition": 100,
                "base": "BTC",
                "quote": "USD",
                "pair": "BTC:USD"
            }]
        }

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "result": "error",
            "error": "Invalid request"
        }

    @property
    def maxordersize_request_mock_response(self):
        return {
            "result": "success",
            "maxBuySize": "10.0",
            "maxSellSize": "10.0",
            "buyPrice": "40000.0",
            "sellPrice": "40000.0",
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

    @property
    def order_creation_request_successful_mock_response(self):
        mock_response = {
            "result": "success",
            "sendStatus": {
                "orderEvents": [
                    {
                        "amount": 100,
                        "executionId": "e1ec9f63-2338-4c44-b40a-43486c6732d7",
                        "orderPriorEdit": "null",
                        "orderPriorExecution": {
                            "cliOrdId": self.expected_client_order_id,
                            "filled": 0,
                            "lastUpdateTimestamp": "2019-12-11T17:17:33.888Z",
                            "limitPrice": 10000,
                            "orderId": self.expected_exchange_order_id,
                            "quantity": 100,
                            "reduceOnly": False,
                            "side": "buy",
                            "symbol": "PF_XBTUSD",
                            "timestamp": "2019-12-11T17:17:33.888Z",
                            "type": "lmt"
                        },
                        "price": 10000,
                        "takerReducedQuantity": "null",
                        "type": "PLACE"
                    }
                ],
                "order_id": self.expected_exchange_order_id,
                "receivedTime": "2019-12-11T17:17:33.888Z",
                "status": "placed"
            },
            "serverTime": "2019-12-11T17:17:33.888Z"
        }
        return mock_response

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        mock_response = {
            "result": "success",
            "accounts": {
                "flex": {
                    "availableMargin": 34122.66,
                    "balanceValue": 34995.52,
                    "collateralValue": 34122.66,
                    "currencies": {
                        "XBT": {  # Base asset (Bitcoin)
                            "available": 10,
                            "collateral": 4886.49976674881,
                            "quantity": 15,
                            "value": 4998.721054420551
                        },
                        "USD": {  # Quote asset
                            "available": 2000,
                            "collateral": 2000,
                            "quantity": 2000,
                            "value": 2000
                        }
                    },
                    "initialMargin": 0,
                    "initialMarginWithOrders": 0,
                    "maintenanceMargin": 0,
                    "marginEquity": 34122.66,
                    "pnl": 0,
                    "portfolioValue": 34995.52,
                    "totalUnrealized": 0,
                    "totalUnrealizedAsMargin": 0,
                    "type": "multiCollateralMarginAccount",
                    "unrealizedFunding": 0
                }
            },
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        return mock_response

    def _configure_balance_response(
            self,
            response: Dict[str, Any],
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        print("\n=== Configuring balance response ===")
        url = self.balance_url
        print(f"URL to mock: {url}")
        print(f"Mock response: {response}")
        mock_api.get(url, body=json.dumps(response), callback=callback)
        return url

    def configure_trade_fills_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.USER_TRADE_RECORDS_PATH_URL, trading_pair=self.trading_pair
        )
        params = {
            "symbol": self.exchange_trading_pair,
            "limit": 200,
            "startTime": int(int(self.exchange._last_trade_history_timestamp) * 1e3),
        }
        encoded_params = urlencode(params)
        url = f"{url}?{encoded_params}"
        response = self._trade_fills_request_mock_response()
        mock_api.get(url, body=json.dumps(response), callback=callback)
        return [url]

    def configure_erroneous_trade_fills_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.USER_TRADE_RECORDS_PATH_URL, trading_pair=self.trading_pair
        )
        params = {
            "symbol": self.exchange_trading_pair,
            "limit": 200,
            "startTime": int(int(self.exchange._last_trade_history_timestamp) * 1e3),
        }
        encoded_params = urlencode(params)
        url = f"{url}?{encoded_params}"
        resp = {"result": "error", "error": "SOME ERROR"}
        mock_api.get(url, body=json.dumps(resp), status=404, callback=callback)
        return [url]

    def balance_request_mock_response_for_base_and_quote(self):
        mock_response = {
            "result": "success",
            "accounts": {
                "flex": {
                    "availableMargin": 34122.66,
                    "balanceValue": 34995.52,
                    "collateralValue": 34122.66,
                    "currencies": {
                        "XBT": {  # Base asset (Bitcoin)
                            "available": 10,
                            "collateral": 4886.49976674881,
                            "quantity": 15,
                            "value": 4998.721054420551
                        },
                        "USD": {  # Quote asset
                            "available": 2000,
                            "collateral": 2000,
                            "quantity": 2000,
                            "value": 2000
                        }
                    },
                    "initialMargin": 0,
                    "initialMarginWithOrders": 0,
                    "maintenanceMargin": 0,
                    "marginEquity": 34122.66,
                    "pnl": 0,
                    "portfolioValue": 34995.52,
                    "totalUnrealized": 0,
                    "totalUnrealizedAsMargin": 0,
                    "type": "multiCollateralMarginAccount",
                    "unrealizedFunding": 0
                }
            },
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        return mock_response

    @property
    def balance_request_mock_response_only_base(self):
        mock_response = self.balance_request_mock_response_for_base_and_quote
        for coin in mock_response["accounts"]["flex"][0]["currencies"]:
            if coin["currencies"] == self.quote_asset:
                mock_response["accounts"]["flex"][0]["currencies"].remove(coin)
        return mock_response

    def _trade_fills_request_mock_response(self):
        return {
            "result": "success",
            "fills": [
                {
                    "fillTime": "2020-07-22T13:37:27.077Z",
                    "fillType": "maker",
                    "fill_id": self.expected_fill_trade_id,
                    "order_id": self.expected_exchange_order_id,
                    "price": 9400,
                    "side": "buy",
                    "size": 5490,
                    "symbol": "PF_XBTUSD"
                }
            ],
            "serverTime": "2020-07-22T13:44:24.311Z"
        }

    @property
    def balance_event_websocket_update(self):
        return {
            "feed": "balances_snapshot",
            "account": "4a012c31-df95-484a-9473-d51e4a0c4ae7",
            "flex_futures": {
                "currencies": {
                    "XBT": {
                        "quantity": 15,
                        "value": 100000.0,
                        "collateral_value": 100000.0,
                        "available": 10,
                        "haircut": 0.0,
                        "conversion_spread": 0.0
                    },
                    "USD": {
                        "quantity": 2000.0,
                        "value": 2000.0,
                        "collateral_value": 2000.0,
                        "available": 2000.0,
                        "haircut": 0.0,
                        "conversion_spread": 0.0
                    }
                },
                "balance_value": 102000.0,
                "portfolio_value": 102000.0,
                "collateral_value": 102000.0,
                "initial_margin": 0.0,
                "initial_margin_without_orders": 0.0,
                "maintenance_margin": 0.0,
                "pnl": 0.0,
                "unrealized_funding": 0.0,
                "total_unrealized": 0.0,
                "total_unrealized_as_margin": 0.0,
                "margin_equity": 102000.0,
                "available_margin": 102000.0,
                "cross": {
                    "balance_value": 102000.0,
                    "portfolio_value": 102000.0,
                    "collateral_value": 102000.0,
                    "initial_margin": 0.0,
                    "initial_margin_without_orders": 0.0,
                    "maintenance_margin": 0.0,
                    "pnl": 0.0,
                    "unrealized_funding": 0.0,
                    "total_unrealized": 0.0,
                    "total_unrealized_as_margin": 0.0,
                    "margin_equity": 102000.0,
                    "available_margin": 102000.0,
                    "effective_leverage": 0.0
                }
            },
            "seq": 1,
            "timestamp": int(time.time() * 1000)
        }

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def empty_funding_payment_mock_response(self):
        return {
            "result": "success",
            "serverTime": "2022-07-06T13:20:00Z",
            "rates": []
        }

    @property
    def funding_payment_mock_response(self):
        return {
            "result": "success",
            "serverTime": "2022-07-06T20:50:00.444Z",
            "rates": [
                {
                    "timestamp": "2022-07-06T13:20:53Z",
                    "fundingRate": str(self.target_funding_payment_funding_rate),
                    "relativeFundingRate": str(self.target_funding_payment_payment_amount)
                }
            ]
        }

    def configure_funding_payment_mock_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.funding_payment_url
        response = self.funding_payment_mock_response
        mock_api.get(url, body=json.dumps(response), callback=callback)
        return [url]

    def configure_empty_funding_payment_mock_response(
        self,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        url = self.funding_payment_url
        response = self.empty_funding_payment_mock_response
        mock_api.get(url, body=json.dumps(response), callback=callback)
        return [url]

    @property
    def target_funding_info_next_funding_utc_str(self):
        return "1657099053000"

    @property
    def target_funding_payment_timestamp_str(self):
        return "1657110053000"

    @property
    def funding_info_mock_response(self):
        mock_response = {
            "result": "success",
            "serverTime": "2025-01-09T18:52:43.97Z",
            "tickers": [
                {
                    "symbol": "PF_XBTUSD",
                    "last": 92981,
                    "lastTime": "2025-01-09T18:52:42.825Z",
                    "tag": "perpetual",
                    "pair": "XBT:USD",
                    "markPrice": 2,
                    "bid": 92986,
                    "bidSize": 0.2203,
                    "ask": 92987,
                    "askSize": 0.66,
                    "vol24h": 7317.5397,
                    "volumeQuote": 683496735.9156,
                    "openInterest": 1933.6742,
                    "open24h": 94013,
                    "high24h": 95383,
                    "low24h": 91750,
                    "lastSize": 0.01,
                    "fundingRate": 3,
                    "fundingRatePrediction": 0.3724459080887603,
                    "suspended": False,
                    "indexPrice": 1,
                    "postOnly": False,
                    "change24h": -1.1
                }
            ]
        }
        return mock_response

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        print("\n=== Calculating expected trading rule ===")
        trading_rules_resp = self.trading_rules_request_mock_response["instruments"][0]
        precision = int(trading_rules_resp["contractValueTradePrecision"])
        min_order_size = Decimal(str("1" + "0" * abs(precision))) if precision < 0 else Decimal(
            ("0." + "0" * (precision - 1) + "1") if precision > 0 else "1")

        print(f"Contract value precision: {precision}")
        print(f"Calculated min_order_size: {min_order_size}")

        # Use the minimum of maxPositionSize and maxBuySize/maxSellSize from mock responses
        max_position_size = Decimal(str(trading_rules_resp["maxPositionSize"]))
        max_buy_size = Decimal(str(self.max_order_size_mock_response["maxBuySize"]))
        max_sell_size = Decimal(str(self.max_order_size_mock_response["maxSellSize"]))
        max_order_size = min(max_position_size, max_buy_size, max_sell_size)

        print(f"Max position size: {max_position_size}")
        print(f"Max buy size: {max_buy_size}")
        print(f"Max sell size: {max_sell_size}")
        print(f"Final max order size: {max_order_size}")

        trading_rule = TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(str(min_order_size)),
            max_order_size=max_order_size,
            min_price_increment=Decimal(str(trading_rules_resp["tickSize"])),
            min_base_amount_increment=Decimal(str(min_order_size)),
            min_notional_size=Decimal("0"),
            min_order_value=Decimal("0"),
        )
        print(f"Created trading rule: {trading_rule}")
        return trading_rule

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        return "Error getting trading rules: Invalid request"

    @property
    def expected_exchange_order_id(self):
        return "335fd977-e5a5-4781-b6d0-c772d5bfb95b"

    @property
    def expected_client_order_id(self):
        return "KRKNBBCUD62b5b01778d6b6264426dbd9cae"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return False

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("100")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("10")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0.1"))],
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "xxxxxxxx-xxxx-xxxx-8b66-c3d2fcd352f6"

    @property
    def latest_trade_hist_timestamp(self) -> int:
        return 1234

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        # Kraken uses PF_ prefix for perpetual futures and XBT instead of BTC
        base = "XBT" if base_token == "BTC" else base_token
        return f"PF_{base}{quote_token}"

    @property
    def exchange_trading_pair(self) -> str:
        # This should return the exchange's format of the trading pair
        return "PF_XBTUSD"

    def create_exchange_instance(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        exchange = KrakenPerpetualDerivative(
            client_config_map,
            self.api_key,
            self.api_secret,
            trading_pairs=[self.trading_pair],
        )
        exchange._last_trade_history_timestamp = self.latest_trade_hist_timestamp
        return exchange

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs["headers"]
        self.assertIn("APIKey", request_headers)
        self.assertIn("Authent", request_headers)
        self.assertIn("Nonce", request_headers)
        self.assertEqual(request_headers["APIKey"], self.api_key)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        print(f"Request data: {request_data}")
        self.assertEqual(order.trade_type.name.lower(), request_data["side"])
        self.assertEqual(self.exchange_trading_pair, request_data["symbol"])
        self.assertEqual(Decimal(float(order.amount)), Decimal(float(request_data["size"])))
        self.assertEqual(CONSTANTS.ORDER_TYPE_MAP[order.order_type].lower(), request_data["orderType"])
        self.assertEqual(order.client_order_id, request_data["cliOrdId"])
        self.assertEqual(order.position == PositionAction.CLOSE, request_data["reduceOnly"])
        if order.order_type == OrderType.LIMIT:
            self.assertEqual(Decimal(str(order.price)), Decimal(str(request_data["limitPrice"])))

            # f"Created {self.order_type.name.upper()} {self.trade_type.name.upper()} order "
            # f"{self.client_order_id} for {self.amount} to {self.position.name.upper()} a {self.trading_pair} position "
            # f"at {self.price}."

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_trading_pair, request_data["symbol"])
        self.assertEqual(order.exchange_order_id, request_data["orderId"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        if order.exchange_order_id is not None:
            self.assertEqual([order.exchange_order_id], request_params["orderIds"])
        else:
            self.assertEqual([order.client_order_id], request_params["cliOrdIds"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_trading_pair, request_params["symbol"])
        self.assertEqual(self.latest_trade_hist_timestamp * 1e3, request_params["start_time"])

    def configure_successful_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        """
        :return: the URL configured for the cancelation
        """
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.CANCEL_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.CANCEL_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {
            "ret_code": 20000,
            "ret_msg": "Could not find order",
        }
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
        self,
        successful_order: InFlightOrder,
        erroneous_order: InFlightOrder,
        mock_api: aioresponses,
    ) -> List[str]:
        """
        :return: a list of all configured URLs for the cancelations
        """
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.CANCEL_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        response = {
            "result": "error",
            "error": "Order not found",
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_order_not_found_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> List[str]:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        response = {
            "result": "error",
            "error": "Order does not exist",
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return [url]

    def configure_completely_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_open_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        mock_api.get(regex_url, status=404, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_partial_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        response = self._order_fills_request_partial_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_full_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.USER_TRADE_RECORDS_PATH_URL, trading_pair=order.trading_pair
        )
        params = {
            "symbol": self.exchange_trading_pair,
            "limit": 200,
            "startTime": int(int(self.exchange._last_trade_history_timestamp) * 1e3),
        }
        encoded_params = urlencode(params)
        url = f"{url}?{encoded_params}"
        response = self._order_fills_request_full_fill_mock_response(order=order)
        mock_api.get(url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL, trading_pair=order.trading_pair
        )
        regex_url = re.compile(url + r"\?.*")
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_failed_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.SET_LEVERAGE_PATH_URL, trading_pair=self.trading_pair
        )
        regex_url = re.compile(f"^{url}")

        err_msg = "Some problem"
        mock_response = {
            "result": "error",
            "error": err_msg,
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        mock_api.post(regex_url, body=json.dumps(mock_response), callback=callback)

        return url, err_msg

    def configure_successful_set_leverage(
        self,
        leverage: int,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.SET_LEVERAGE_PATH_URL, trading_pair=self.trading_pair
        )
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "success",
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

        mock_api.post(regex_url, body=json.dumps(mock_response), callback=callback)

        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "feed": "open_orders_verbose",
            "order": {
                "instrument": "PF_XBTUSD",
                "time": 1567597581495,
                "last_update_time": 1567597581495,
                "qty": 100.0,
                "filled": 0.0,
                "limit_price": 10000.0,
                "stop_price": 0.0,
                "type": "limit",
                "order_id": self.expected_exchange_order_id,
                "cli_ord_id": order.client_order_id,  # Use the actual order's client_order_id
                "direction": 0,
                "reduce_only": False
            },
            "is_cancel": False,
            "reason": "new_placed_order_by_user"
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        print("\n=== Creating canceled order websocket update ===")
        event = {
            "feed": "open_orders_verbose",
            "order": {
                "instrument": "PF_XBTUSD",
                "time": 1567597581495,
                "last_update_time": 1567597581495,
                "qty": str(order.amount),
                "filled": "0.0",
                "limit_price": str(order.price),
                "stop_price": "0.0",
                "type": "limit",
                "order_id": order.exchange_order_id,
                "cli_ord_id": order.client_order_id,
                "direction": 0,
                "reduce_only": False
            },
            "status": "CANCELLED",
            "reason": "CANCELLED_BY_USER",
            "timestamp": "2024-01-16T10:00:00.000Z"
        }
        print(f"Created event: {event}")
        return event

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "feed": "fills",
            "username": "DemoUser",
            "fills": [
                {
                    "instrument": "PF_XBTUSD",
                    "time": 1600256966528,
                    "price": 364.65,
                    "seq": 100,
                    "buy": True,
                    "qty": float(order.amount),  # Use the actual order amount
                    "remaining_order_qty": 0.0,
                    "order_id": order.exchange_order_id or "3696d19b-3226-46bd-993d-a9a7aacc8fbc",
                    "cli_ord_id": order.client_order_id,  # Use the actual order's client_order_id
                    "fill_id": "c14ee7cb-ae25-4601-853a-d0205e576099",
                    "fill_type": "taker",
                    "fee_paid": 0.00685588921,
                    "fee_currency": "ETH",
                    "taker_order_type": "liquidation",
                    "order_type": "limit"
                },
            ]
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return None  # Kraken sends both order and trade info in the same message

    def position_event_for_full_fill_websocket_update(self, order: InFlightOrder, unrealized_pnl: float):
        return {
            "feed": CONSTANTS.WS_OPEN_POSITIONS_TOPIC,
            "account": "DemoUser",
            "positions": [
                {
                    "instrument": "PF_XBTUSD",
                    "balance": str(order.amount),
                    "pnl": str(unrealized_pnl),
                    "entry_price": str(order.price),
                    "mark_price": str(order.price),
                    "index_price": str(order.price),
                    "liquidation_threshold": "0.0",
                    "effective_leverage": "1.0",
                    "return_on_equity": "0.0",
                    "initial_margin": "100.0",
                    "initial_margin_with_orders": "100.0",
                    "maintenance_margin": "50.0",
                    "unrealized_funding": "0.0",
                    "pnl_currency": self.quote_asset
                }
            ],
            "seq": 1,
            "timestamp": int(time.time() * 1000)
        }

    def funding_info_event_for_websocket_update(self):
        return {
            "feed": "ticker",
            "product_id": self.exchange_trading_pair,
            "index": str(self.target_funding_info_index_price_ws_updated),
            "markPrice": str(self.target_funding_info_mark_price_ws_updated),
            "funding_rate": str(self.target_funding_info_rate_ws_updated),
            "next_funding_rate_time": str(self.target_funding_info_next_funding_utc_timestamp_ws_updated),
            "time": int(time.time() * 1000)  # Kraken uses millisecond timestamps
        }

    def test_create_order_with_invalid_position_action_raises_value_error(self):
        """Test that creating an order with invalid position action raises ValueError."""
        with self.assertRaises(ValueError) as context:
            asyncio.get_event_loop().run_until_complete(
                self.exchange._create_order(
                    trade_type=TradeType.BUY,
                    order_id="C1",
                    trading_pair=self.trading_pair,
                    amount=Decimal("1"),
                    order_type=OrderType.LIMIT,
                    price=Decimal("10000.0"),
                    position_side=PositionSide.LONG,
                    position_action=PositionAction.NIL,
                )
            )
        self.assertEqual(
            str(context.exception),
            "Invalid position action PositionAction.NIL. Must be one of [<PositionAction.OPEN: 'OPEN'>, <PositionAction.CLOSE: 'CLOSE'>]",
        )

    def test_get_position_index(self):

        for trade_type, position_action in product(
            [TradeType.BUY, TradeType.SELL], [PositionAction.OPEN, PositionAction.CLOSE]
        ):
            position_idx = self.exchange._get_position_idx(trade_type=trade_type, position_action=position_action)
            self.assertEqual(
                CONSTANTS.POSITION_IDX_ONEWAY, position_idx, msg=f"Failed on {trade_type} and {position_action}."
            )
        for trade_type, position_action in chain(
            product([TradeType.RANGE], [PositionAction.CLOSE, PositionAction.OPEN]),
            product([TradeType.BUY, TradeType.SELL], [PositionAction.NIL]),
        ):
            with self.assertRaises(NotImplementedError, msg=f"Failed on {trade_type} and {position_action}."):
                self.exchange._get_position_idx(trade_type=trade_type, position_action=position_action)

    def test_get_buy_and_sell_collateral_tokens(self):
        """Test getting collateral tokens for buy and sell orders."""
        print("\n=== Starting test_get_buy_and_sell_collateral_tokens ===")

        print("Before trading rules initialization")
        print(f"Current trading rules: {self.exchange.trading_rules}")

        self._simulate_trading_rules_initialized()

        print("\nAfter trading rules initialization")
        print(f"Updated trading rules: {self.exchange.trading_rules}")
        print(f"Trading rule for {self.trading_pair}: {self.exchange.trading_rules.get(self.trading_pair)}")

        buy_collateral_token = self.exchange.get_buy_collateral_token(self.trading_pair)
        sell_collateral_token = self.exchange.get_sell_collateral_token(self.trading_pair)

        print(f"\nCollateral tokens:")
        print(f"Buy collateral token: {buy_collateral_token}")
        print(f"Sell collateral token: {sell_collateral_token}")
        print(f"Expected collateral asset: USD")

        # For Kraken Perpetual, we use USD as the standard collateral token for all trading pairs
        self.assertEqual("USD", buy_collateral_token)
        self.assertEqual("USD", sell_collateral_token)

    @aioresponses()
    def test_resolving_trading_pair_symbol_duplicates_on_trading_rules_update_first_is_good(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = self.trading_rules_url
        response = self.trading_rules_request_mock_response
        instruments = response["instruments"]
        duplicate = deepcopy(instruments[0])
        duplicate["symbol"] = f"{self.exchange_trading_pair}"
        duplicate["contractValueTradePrecision"] = str(float(duplicate["contractValueTradePrecision"]))
        instruments.append(duplicate)

        mock_api.get(url, body=json.dumps(response))

        # Add mock responses max order size endpoints for both symbols
        max_size_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.MAX_ORDER_SIZE_ENDPOINT)

        # Create regex patterns to match URLs with any symbol parameter
        max_size_url_pattern = re.compile(f"^{max_size_url}".replace(".", r"\.").replace("?", r"\?") + r"\?.*orderType=lmt.*")

        mock_api.get(max_size_url_pattern, body=json.dumps(self.max_order_size_mock_response))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertEqual(1, len(self.exchange.trading_rules))
        self.assertIn(self.trading_pair, self.exchange.trading_rules)
        self.assertEqual(repr(self.expected_trading_rule), repr(self.exchange.trading_rules[self.trading_pair]))

    @aioresponses()
    def test_resolving_trading_pair_symbol_duplicates_on_trading_rules_update_second_is_good(self, mock_api):
        self.exchange._set_current_timestamp(1000)
        url = self.trading_rules_url
        response = self.trading_rules_request_mock_response
        instruments = response["instruments"]
        duplicate = deepcopy(instruments[0])
        min_order_qty = float(duplicate["contractValueTradePrecision"])
        duplicate["symbol"] = f"{self.exchange_trading_pair}"
        duplicate["contractValueTradePrecision"] = str(min_order_qty)
        instruments.insert(0, duplicate)

        mock_api.get(url, body=json.dumps(response))

        # Add mock responses for max order size endpoints for both symbols
        max_size_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.MAX_ORDER_SIZE_ENDPOINT)

        # Create regex patterns to match URLs with any symbol parameter
        max_size_url_pattern = re.compile(f"^{max_size_url}".replace(".", r"\.").replace("?", r"\?") + r"\?.*orderType=lmt.*")

        mock_api.get(max_size_url_pattern, body=json.dumps(self.max_order_size_mock_response))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertEqual(1, len(self.exchange.trading_rules))
        self.assertIn(self.trading_pair, self.exchange.trading_rules)
        self.assertEqual(repr(self.expected_trading_rule), repr(self.exchange.trading_rules[self.trading_pair]))

    # @aioresponses()
    # def test_resolving_trading_pair_symbol_duplicates_on_trading_rules_update_cannot_resolve(self, mock_api):
    #     self.exchange._set_current_timestamp(1000)
    #
    #     url = self.trading_rules_url
    #     response = self.trading_rules_request_mock_response
    #     instruments = response["instruments"]
    #     min_order_qty = float(instruments[0]["contractValueTradePrecision"])
    #     first_duplicate = deepcopy(instruments[0])
    #     first_duplicate["symbol"] = f"{self.exchange_trading_pair}_12345"
    #     first_duplicate["contractValueTradePrecision"] = str(min_order_qty + 1)
    #     second_duplicate = deepcopy(instruments[0])
    #     second_duplicate["symbol"] = f"{self.exchange_trading_pair}_67890"
    #     second_duplicate["contractValueTradePrecision"] = str(min_order_qty + 2)
    #     instruments.pop(0)
    #     instruments.append(first_duplicate)
    #     instruments.append(second_duplicate)
    #
    #     response_with_duplicates = {
    #         "result": "success",
    #         "instruments": instruments
    #     }
    #
    #     mock_api.get(url, body=json.dumps(instruments))
    #
    #     # Add mock responses for max order size endpoints
    #     max_size_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.MAX_ORDER_SIZE_ENDPOINT)
    #
    #     max_size_url_pattern = re.compile(f"^{max_size_url}".replace(".", r"\.").replace("?", r"\?") + r"\?.*orderType=lmt.*")
    #
    #     mock_api.get(max_size_url_pattern, body=json.dumps(self.max_order_size_mock_response),repeat=True)
    #
    #     self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())
    #
    #     self.assertEqual(0, len(self.exchange.trading_rules))
    #     self.assertNotIn(self.trading_pair, self.exchange.trading_rules)
    #     self.assertTrue(
    #         self.is_logged(
    #             log_level="ERROR",
    #             message=(
    #                 f"Could not resolve the exchange symbols"
    #                 f" {self.exchange_trading_pair}_67890"
    #                 f" and {self.exchange_trading_pair}_12345"
    #             ),
    #         )
    #     )
    #
    # # def test_time_synchronizer_related_reqeust_error_detection(self):
    # #     error_code_str = self.exchange._format_ret_code_for_print(ret_code=CONSTANTS.RET_CODE_AUTH_TIMESTAMP_ERROR)
    # #     exception = IOError(f"{error_code_str} - Failed to cancel order for timestamp reason.")
    # #     self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(exception))
    # #
    # #     error_code_str = self.exchange._format_ret_code_for_print(ret_code=CONSTANTS.RET_CODE_ORDER_NOT_EXISTS)
    # #     exception = IOError(f"{error_code_str} - Failed to cancel order because it was not found.")
    # #     self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(exception))

    @aioresponses()
    @patch("asyncio.Queue.get")
    def test_listen_for_funding_info_update_initializes_funding_info(self, mock_api, mock_queue_get):
        url = self.funding_info_url

        response = self.funding_info_mock_response
        mock_api.get(url, body=json.dumps(response))

        event_messages = [asyncio.CancelledError]
        mock_queue_get.side_effect = event_messages

        try:
            self.async_run_with_timeout(self.exchange._listen_for_funding_info())
        except asyncio.CancelledError:
            pass

        funding_info: FundingInfo = self.exchange.get_funding_info(self.trading_pair)

        self.assertEqual(self.trading_pair, funding_info.trading_pair)
        self.assertEqual(self.target_funding_info_index_price, funding_info.index_price)
        self.assertEqual(self.target_funding_info_mark_price, funding_info.mark_price)
        self.assertEqual(
            self.target_funding_info_next_funding_utc_timestamp, 1657099053
        )
        self.assertEqual(self.target_funding_info_rate, funding_info.rate)

    @aioresponses()
    @patch("asyncio.Queue.get")
    def test_listen_for_funding_info_update_updates_funding_info(self, mock_api, mock_queue_get):
        url = self.funding_info_url

        response = self.funding_info_mock_response
        mock_api.get(url, body=json.dumps(response))

        funding_info_event = self.funding_info_event_for_websocket_update()

        event_messages = [funding_info_event, asyncio.CancelledError]
        mock_queue_get.side_effect = event_messages

        try:
            self.async_run_with_timeout(
                self.exchange._listen_for_funding_info())
        except asyncio.CancelledError:
            pass

        self.assertEqual(1, self.exchange._perpetual_trading.funding_info_stream.qsize())  # rest in OB DS tests

    @aioresponses()
    def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        """Test that canceling an order that is not found in the exchange is handled correctly."""
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=self.exchange_order_id_prefix + "1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        url = self.configure_order_not_found_error_cancelation_response(
            order=order,
            mock_api=mock_api)

        result = self.async_run_with_timeout(
            self.exchange._execute_cancel(
                trading_pair=order.trading_pair,
                order_id=order.client_order_id))

        self.assertIsNone(result)  # The base class returns None when order is not found
        self.assertTrue(self.is_logged("WARNING", f"Failed to cancel order {order.client_order_id} (order not found)"))

    @aioresponses()
    def test_lost_order_removed_if_not_found_during_order_status_update(self, mock_api):
        # Disabling this test because the connector has not been updated yet to validate
        # order not found during status update (check _is_order_not_found_during_status_update_error)
        pass

    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "result": "success",
            "cancelStatus": {
                "cliOrdId": order.client_order_id,
                "orderEvents": [
                    {
                        "order": {
                            "cliOrdId": order.client_order_id,
                            "filled": 0.0,
                            "lastUpdateTimestamp": "2020-08-27T17:03:33.196Z",
                            "limitPrice": 0.0,
                            "orderId": order.exchange_order_id,
                            "quantity": 0.0,
                            "reduceOnly": False,
                            "side": order.trade_type.name.lower(),
                            "symbol": self.exchange_trading_pair,
                            "timestamp": "2020-08-27T17:03:33.196Z",
                            "type": "lmt"
                        },
                        "reducedQuantity": 0.0,
                        "type": "PLACE"
                    }
                ],
                "order_id": order.exchange_order_id,
                "receivedTime": "2020-08-27T17:03:33.196Z",
                "status": "cancelled"
            },
            "serverTime": "2020-08-27T17:03:33.196Z"
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "result": "success",
            "orders": [
                {
                    "order": {
                        "orderId": order.exchange_order_id or "someExchangeOrderId",
                        "cliOrdId": order.client_order_id,
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "side": order.trade_type.name.lower(),
                        "orderType": "lmt",
                        "limitPrice": str(order.price),
                        "unfilledSize": str(order.amount),
                        "filledSize": "0.0",
                        "avgFillPrice": "0.0",
                        "status": "cancelled",
                        "timestamp": "2024-01-16T10:00:00.000Z"
                    },
                    "status": "CANCELLED",
                    "updateReason": "CANCELLED_BY_USER"
                }
            ],
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "result": "success",
            "orders": [
                {
                    "order": {
                        "orderId": order.exchange_order_id or "someExchangeOrderId",
                        "cliOrdId": order.client_order_id,
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "side": order.trade_type.name.lower(),
                        "orderType": "lmt",
                        "limitPrice": str(order.price),
                        "unfilledSize": "0.0",
                        "filledSize": str(order.amount),
                        "avgFillPrice": str(order.price),
                        "status": "closed",
                        "timestamp": "2024-01-16T10:00:00.000Z"
                    },
                    "status": "FULLY_EXECUTED",
                    "reason": None
                }
            ],
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "result": "success",
            "orders": [
                {
                    "order": {
                        "orderId": order.exchange_order_id or "someExchangeOrderId",
                        "cliOrdId": order.client_order_id,
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "side": order.trade_type.name.lower(),
                        "orderType": "lmt",
                        "limitPrice": str(order.price),
                        "unfilledSize": str(order.amount / 2),
                        "filledSize": str(order.amount / 2),
                        "avgFillPrice": str(order.price),
                        "status": "open",
                        "timestamp": "2024-01-16T10:00:00.000Z"
                    },
                    "status": "PARTIALLY_FILLED",
                    "reason": None
                }
            ],
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder):
        return {
            "fills": [
                {
                    "fillTime": "2021-11-18T02:39:41.826Z",
                    "fillType": "maker",
                    "fill_id": "98e3deeb-0385-4b25-b15e-7e8453512cb2",
                    "order_id": order.exchange_order_id,
                    "price": 47000,
                    "side": order.trade_type == TradeType.BUY,
                    "size": 10,
                    "symbol": "PF_XBTUSD"
                }
            ],
            "result": "success",
            "serverTime": "2020-08-27T17:03:33.196Z"
        }

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder):
        return {
            "fills": [
                {
                    "fillTime": "2021-11-18T02:39:41.826Z",
                    "fillType": "maker",
                    "fill_id": "98e3deeb-0385-4b25-b15e-7e8453512cb2",
                    "order_id": order.exchange_order_id,
                    "price": 47000,
                    "side": order.trade_type == TradeType.BUY,
                    "size": 10,
                    "symbol": "PF_XBTUSD"
                }
            ],
            "result": "success",
            "serverTime": "2020-08-27T17:03:33.196Z"
        }

    def _simulate_trading_rules_initialized(self):
        print("\n=== Simulating trading rules initialization ===")
        print(f"Trading pair: {self.trading_pair}")

        trading_rule = TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(str(0.01)),
            max_order_size=Decimal(str(1)),
            min_price_increment=Decimal(str(1e-4)),
            min_base_amount_increment=Decimal(str(0.000001)),
        )

        print(f"Created trading rule: {trading_rule}")
        print(f"Min order size: {trading_rule.min_order_size}")
        print(f"Max order size: {trading_rule.max_order_size}")
        print(f"Min price increment: {trading_rule.min_price_increment}")
        print(f"Min base amount increment: {trading_rule.min_base_amount_increment}")

        self.exchange._trading_rules = {self.trading_pair: trading_rule}

        print(f"Updated exchange trading rules: {self.exchange._trading_rules}")

    @aioresponses()
    def test_lost_order_user_stream_full_fill_events_are_processed(self, mock_api):
        # Disabling this test because the connector has not been updated yet to validate
        # order not found during status update (check _is_order_not_found_during_status_update_error)
        pass

    @property
    def initial_margin_mock_response(self):
        return {
            "result": "success",
            "estimatedLiquidationThreshold": 0.95,
            "initialMargin": 0.02,  # 2% initial margin
            "price": 40000.0,
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

    @property
    def max_order_size_mock_response(self):
        return {
            "result": "success",
            "buyPrice": 40000.0,
            "maxBuySize": 10.0,
            "maxSellSize": 10.0,
            "sellPrice": 40000.0,
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Dict[str, Any]:
        return {
            "result": "success",
            "orders": [
                {
                    "order": {
                        "orderId": order.exchange_order_id or "someExchangeOrderId",
                        "cliOrdId": order.client_order_id,
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "side": order.trade_type.name.lower(),
                        "orderType": "lmt",
                        "limitPrice": str(order.price),
                        "unfilledSize": str(order.amount),
                        "filledSize": "0.0",
                        "avgFillPrice": "0.0",
                        "status": "open",
                        "timestamp": "2024-01-16T10:00:00.000Z"
                    },
                    "status": "ENTERED_BOOK",
                    "reason": None
                }
            ],
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

    def test_supported_position_modes(self):
        supported_modes = self.exchange.supported_position_modes
        self.assertEqual([PositionMode.ONEWAY], supported_modes)  # Kraken only supports one-way positions

    @aioresponses()
    def test_set_position_mode_success(self, mock_api):
        """Test setting position mode to ONEWAY succeeds"""
        self.exchange._position_mode = None  # Reset position mode
        result = self.async_run_with_timeout(self.exchange.set_position_mode(PositionMode.ONEWAY))
        self.assertIsNone(result)  # No error means success

    @aioresponses()
    def test_set_position_mode_failure(self, mock_api):
        """Test setting position mode to anything other than ONEWAY fails"""
        self.exchange._position_mode = None  # Reset position mode
        with self.assertRaisesRegex(ValueError, ".*Only ONEWAY position mode is supported.*"):
            self.async_run_with_timeout(self.exchange.set_position_mode(PositionMode.HEDGE))

    def test_user_stream_update_for_order_failure(self):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id="OID1",
            exchange_order_id="100234",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders["OID1"]

        event_message = {
            "feed": "open_orders_verbose",
            "orders": [
                {
                    "order": {
                        "orderId": order.exchange_order_id,
                        "cliOrdId": order.client_order_id,
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "side": order.trade_type.name.lower(),
                        "orderType": "lmt",
                        "limitPrice": str(order.price),
                        "unfilledSize": str(order.amount),
                        "filledSize": "0.0",
                        "avgFillPrice": "0.0",
                        "status": "rejected",
                        "timestamp": "2024-01-16T10:00:00.000Z"
                    },
                    "status": "REJECTED",
                    "reason": "INSUFFICIENT_MARGIN"
                }
            ],
            "serverTime": "2024-01-16T10:00:00.000Z"
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(order.client_order_id, failure_event.order_id)
        self.assertEqual(order.order_type, failure_event.order_type)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_failure)
        self.assertTrue(order.is_done)

    def test_user_stream_update_for_canceled_order(self):
        print("\n=== Starting test_user_stream_update_for_canceled_order ===")

        self.exchange._set_current_timestamp(1640780000)

        # Create and track the order
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]
        print(f"\nCreated order: {order}")
        print(f"Initial order state: {order.current_state}")

        # Create the order event
        order_event = self.order_event_for_canceled_order_websocket_update(order=order)
        print(f"\nCreated order event: {order_event}")

        # Set up the mock queue
        mock_queue = AsyncMock()
        event_messages = [order_event, asyncio.CancelledError]
        mock_queue.get.side_effect = event_messages
        self.exchange._user_stream_tracker._user_stream = mock_queue
        print("\nMock queue set up with order event")

        # Process the event
        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        print(f"\nOrder cancelled logger events: {self.order_cancelled_logger.event_log}")
        print(f"Final order state: {order.current_state}")
        print(f"Order is cancelled: {order.is_cancelled}")
        print(f"Order is done: {order.is_done}")

        cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
        self.assertEqual(order.client_order_id, cancel_event.order_id)
        self.assertEqual(order.exchange_order_id, cancel_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_cancelled)
        self.assertTrue(order.is_done)

        self.assertTrue(
            self.is_logged(
                "INFO",
                f"Successfully canceled order {order.client_order_id}."
            )
        )

    @aioresponses()
    def test_update_balances(self, mock_api):
        url = self.balance_url
        response = {
            "result": "success",
            "accounts": {
                "flex": {
                    "availableMargin": 34122.66,
                    "balanceValue": 34995.52,
                    "collateralValue": 34122.66,
                    "currencies": {
                        "XBT": {  # Base asset (Bitcoin)
                            "available": 10,
                            "collateral": 4886.49976674881,
                            "quantity": 15,
                            "value": 4998.721054420551
                        },
                        "USD": {  # Quote asset
                            "available": 2000,
                            "collateral": 2000,
                            "quantity": 2000,
                            "value": 2000
                        }
                    },
                    "initialMargin": 0,
                    "initialMarginWithOrders": 0,
                    "maintenanceMargin": 0,
                    "marginEquity": 34122.66,
                    "pnl": 0,
                    "portfolioValue": 34995.52,
                    "totalUnrealized": 0,
                    "totalUnrealizedAsMargin": 0,
                    "type": "multiCollateralMarginAccount",
                    "unrealizedFunding": 0
                }
            },
            "serverTime": "2024-01-16T10:00:00.000Z"
        }
        mock_api.get(url, body=json.dumps(response))

        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertEqual(Decimal("10"), available_balances["BTC"])
        self.assertEqual(Decimal("2000"), available_balances["USD"])
        self.assertEqual(Decimal("15"), total_balances["BTC"])
        self.assertEqual(Decimal("2000"), total_balances["USD"])

    @aioresponses()
    def test_user_stream_update_for_order_full_fill(self, mock_api):
        print("\n=== Starting test_user_stream_update_for_order_full_fill ===")

        print("\nSetting up test conditions:")
        print(f"Trading pair: {self.trading_pair}")
        print(f"Client order ID prefix: {self.client_order_id_prefix}")
        print(f"Exchange order ID prefix: {self.exchange_order_id_prefix}")

        self.exchange._set_current_timestamp(1640780000)
        leverage = 2
        self.exchange._perpetual_trading.set_leverage(self.trading_pair, leverage)
        print(f"Set leverage to {leverage}")

        # Create the order
        client_order_id = self.client_order_id_prefix + "1"
        exchange_order_id = self.exchange_order_id_prefix + "1"
        print(f"\nAttempting to create order with:")
        print(f"Client order ID: {client_order_id}")
        print(f"Exchange order ID: {exchange_order_id}")
        print(f"Trading pair: {self.trading_pair}")
        print(f"Order type: {OrderType.LIMIT}")
        print(f"Trade type: {TradeType.SELL}")
        print(f"Price: {Decimal('10000')}")
        print(f"Amount: {Decimal('1')}")
        print(f"Position action: {PositionAction.OPEN}")

        print("\nBefore order creation:")
        print(f"Current in-flight orders: {self.exchange.in_flight_orders}")

        self.exchange.start_tracking_order(
            order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            price=Decimal("10000"),
            amount=Decimal("1"),
            position_action=PositionAction.OPEN,
        )

        order = self.exchange.in_flight_orders[client_order_id]
        print(f"\nAfter order creation:")
        print(f"Order in in_flight_orders: {order}")
        print(f"Updated in-flight orders: {self.exchange.in_flight_orders}")
        print(f"Order tracker state: {self.exchange._order_tracker.all_fillable_orders}")

        print(f"\nOrder details:")
        print(f"Order client_order_id: {order.client_order_id}")
        print(f"Order exchange_order_id: {order.exchange_order_id}")
        print(f"Order trading_pair: {order.trading_pair}")
        print(f"Order current_state: {order.current_state}")

        order_event = self.order_event_for_full_fill_websocket_update(order=order)
        trade_event = self.trade_event_for_full_fill_websocket_update(order=order)
        expected_unrealized_pnl = 12
        position_event = self.position_event_for_full_fill_websocket_update(
            order=order, unrealized_pnl=expected_unrealized_pnl
        )

        print(f"\nGenerated events:")
        print(f"Order event: {order_event}")
        print(f"Trade event: {trade_event}")
        print(f"Position event: {position_event}")

        mock_queue = AsyncMock()
        event_messages = []
        if trade_event:
            event_messages.append(trade_event)
        if order_event:
            event_messages.append(order_event)
        if position_event:
            event_messages.append(position_event)
        event_messages.append(asyncio.CancelledError)
        mock_queue.get.side_effect = event_messages
        self.exchange._user_stream_tracker._user_stream = mock_queue

        print(f"\nEvent messages to process: {event_messages}")

        if self.is_order_fill_http_update_executed_during_websocket_order_event_processing:
            self.configure_full_fill_trade_response(
                order=order,
                mock_api=mock_api)

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        print(f"\nAfter processing events:")
        print(f"Order state: {order.current_state}")
        print(f"Order is done: {order.is_done}")
        print(f"Order is filled: {order.is_filled}")
        print(f"Order completely filled event is set: {order.completely_filled_event.is_set()}")

        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(order.wait_until_completely_filled())

    @aioresponses()
    def test_set_assignment_program_success(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_ADD_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "success"
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.set_assignment_program(
                contract_type="flex",
                contract="PF_XBTUSD",
                max_size=1.0,
                max_position=2.0,
                accept_long=True,
                accept_short=False,
                time_frame="weekdays",
                enabled=True,
            )
        )

        self.assertEqual((True, ""), result)

    @aioresponses()
    def test_delete_assignment_program_success(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_DELETE_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "success",
            "id": 123,
            "participant": {
                "acceptLong": True,
                "acceptShort": True,
                "contract": "PF_BTCUSD",
                "contractType": "flex",
                "enabled": True,
                "maxPosition": 10,
                "maxSize": 10,
                "timeFrame": "weekdays"
            }
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.delete_assignment_program(program_id=123)
        )

        self.assertEqual((True, ""), result)

    @aioresponses()
    def test_set_assignment_program_failure(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_ADD_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "error",
            "error": "Invalid contract type"
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        contract_type = "invalid"
        result = self.async_run_with_timeout(
            self.exchange.set_assignment_program(contract_type=contract_type)
        )

        self.assertEqual((False, "Invalid contract type"), result)

    def test_set_assignment_program_invalid_time_frame(self):
        result = self.async_run_with_timeout(
            self.exchange.set_assignment_program(
                contract_type="flex",
                time_frame="invalid"
            )
        )

        self.assertEqual((False, "Invalid time_frame. Must be one of: all, weekdays, weekends"), result)

    @aioresponses()
    def test_get_current_assignment_programs_success(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_CURRENT_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "success",
            "programs": [
                {
                    "id": 123,
                    "participant": {
                        "acceptLong": True,
                        "acceptShort": True,
                        "contract": "PF_BTCUSD",
                        "contractType": "flex",
                        "enabled": True,
                        "maxPosition": 10,
                        "maxSize": 10,
                        "timeFrame": "weekdays"
                    }
                }
            ]
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_current_assignment_programs()
        )

        self.assertEqual((True, mock_response["programs"]), result)

    @aioresponses()
    def test_get_current_assignment_programs_failure(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_CURRENT_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "error",
            "error": "Authentication failed"
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_current_assignment_programs()
        )

        self.assertEqual((False, "Authentication failed"), result)

    @aioresponses()
    def test_get_assignment_program_history_success(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_HISTORY_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "success",
            "history": [
                {
                    "id": 123,
                    "participant": {
                        "acceptLong": True,
                        "acceptShort": True,
                        "contract": "PF_BTCUSD",
                        "contractType": "flex",
                        "enabled": True,
                        "maxPosition": 10,
                        "maxSize": 10,
                        "timeFrame": "weekdays"
                    },
                    "timestamp": "2024-01-16T10:00:00.000Z",
                    "action": "created"
                }
            ]
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_assignment_program_history()
        )

        self.assertEqual((True, mock_response["history"]), result)

    @aioresponses()
    def test_get_assignment_program_history_failure(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_HISTORY_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "error",
            "error": "Rate limit exceeded"
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_assignment_program_history()
        )

        self.assertEqual((False, "Rate limit exceeded"), result)

    @aioresponses()
    def test_delete_assignment_program_failure(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_DELETE_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "error",
            "error": "Program not found"
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.delete_assignment_program(program_id=999)
        )

        self.assertEqual((False, "Program not found"), result)

    @aioresponses()
    def test_get_current_assignment_programs_success(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_CURRENT_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "success",
            "programs": [
                {
                    "id": 123,
                    "participant": {
                        "acceptLong": True,
                        "acceptShort": True,
                        "contract": "PF_BTCUSD",
                        "contractType": "flex",
                        "enabled": True,
                        "maxPosition": 10,
                        "maxSize": 10,
                        "timeFrame": "weekdays"
                    }
                }
            ]
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_current_assignment_programs()
        )

        self.assertEqual((True, mock_response["programs"]), result)

    @aioresponses()
    def test_get_current_assignment_programs_failure(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_CURRENT_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "error",
            "error": "Authentication failed"
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_current_assignment_programs()
        )

        self.assertEqual((False, "Authentication failed"), result)

    @aioresponses()
    def test_get_assignment_program_history_success(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_HISTORY_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "success",
            "history": [
                {
                    "id": 123,
                    "participant": {
                        "acceptLong": True,
                        "acceptShort": True,
                        "contract": "PF_BTCUSD",
                        "contractType": "flex",
                        "enabled": True,
                        "maxPosition": 10,
                        "maxSize": 10,
                        "timeFrame": "weekdays"
                    },
                    "timestamp": "2024-01-16T10:00:00.000Z",
                    "action": "created"
                }
            ]
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_assignment_program_history()
        )

        self.assertEqual((True, mock_response["history"]), result)

    @aioresponses()
    def test_get_assignment_program_history_failure(self, mock_api):
        path_url = CONSTANTS.ASSIGNMENT_HISTORY_PATH_URL
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url)
        regex_url = re.compile(f"^{url}")

        mock_response = {
            "result": "error",
            "error": "Rate limit exceeded"
        }
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.get_assignment_program_history()
        )

        self.assertEqual((False, "Rate limit exceeded"), result)

    def setUp(self) -> None:
        self.loop = asyncio.get_event_loop()
        super().setUp()
        self.connector = self.create_exchange_instance()
        self._initialize_event_loggers()
        self._simulate_trading_rules_initialized()

    def test_initial_status_dict(self):
        self.exchange._set_trading_pair_symbol_map(None)
        self.exchange._perpetual_trading._funding_info = {}
        self.exchange._trading_rules.clear()  # Clear trading rules

        status_dict = self.exchange.status_dict

        expected_initial_dict = self._expected_initial_status_dict()
        expected_initial_dict["funding_info"] = False

        self.assertEqual(expected_initial_dict, status_dict)

    @aioresponses()
    def test_update_trading_rules_ignores_rule_with_error(self, mock_api):
        self.exchange._set_current_timestamp(1000)
        self.exchange._trading_rules.clear()  # Clear trading rules before test

        self.configure_erroneous_trading_rules_response(mock_api=mock_api)

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertEqual(0, len(self.exchange._trading_rules))

    def test_get_balance_with_usd_collateral(self):
        """Test that get_balance returns USD balance for any currency in trading pairs."""
        self._simulate_trading_rules_initialized()
        
        # Configure balances - only add USD balance
        self.exchange._account_balances = {
            "USD": Decimal("1000"),  # Only USD balance
            "ETH": Decimal("0")      # Zero ETH balance
        }
        self.exchange._account_available_balances = {
            "USD": Decimal("900"),   # Available USD balance
            "ETH": Decimal("0")      # Zero ETH available balance
        }
        
        # Test currencies directly
        usd_balance = self.exchange.get_balance("USD")
        eth_balance = self.exchange.get_balance("ETH")
        
        self.assertEqual(Decimal("1000"), usd_balance)
        self.assertEqual(Decimal("0"), eth_balance)  # Direct balance check shows 0
        
        # Test base currency in trading pair 
        # This should return USD balance since we're using USD as collateral
        btc_balance = self.exchange.get_balance("BTC")
        self.assertEqual(Decimal("1000"), btc_balance)  # Should return USD balance
        
        # Test available balance
        usd_available = self.exchange.get_available_balance("USD")
        btc_available = self.exchange.get_available_balance("BTC")
        eth_available = self.exchange.get_available_balance("ETH")
        
        self.assertEqual(Decimal("900"), usd_available)
        self.assertEqual(Decimal("900"), btc_available)  # Should return USD available balance
        self.assertEqual(Decimal("0"), eth_available)    # Direct balance check shows 0
