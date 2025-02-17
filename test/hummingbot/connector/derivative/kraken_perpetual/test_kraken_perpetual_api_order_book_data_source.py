import asyncio
import json
import re
import time
from datetime import datetime
from decimal import Decimal
from typing import Awaitable, Dict
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

import hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_web_utils as web_utils
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.kraken_perpetual import (
    kraken_perpetual_constants as CONSTANTS,
    kraken_perpetual_utils as utils,
)
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_api_order_book_data_source import (
    KrakenPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_derivative import KrakenPerpetualDerivative
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class KrakenPerpetualAPIOrderBookDataSourceTests(TestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "BTC"
        cls.quote_asset = "USD"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = "PF_XBTUSD"
        cls.domain = "kraken_perpetual_testnet"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.async_tasks = []
        self.mocking_assistant = NetworkMockingAssistant()

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = KrakenPerpetualDerivative(
            client_config_map,
            kraken_perpetual_api_key="",
            kraken_perpetual_secret_key="",
            trading_pairs=[self.trading_pair],
            trading_required=False,
            domain=self.domain,
        )
        self.data_source = KrakenPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain,
        )

        self._original_full_order_book_reset_time = self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def subscribe_to_feed(self) -> Dict:
        return {
            "event": "subscribed",
            "feed": "book",
            "product_ids": [
                self.ex_trading_pair
            ]
        }

    def subscribe_to_feed_unsuccessful(self) -> Dict:
        return {
            "event": "error",
            "message": "Invalid product id"
        }

    def get_rest_snapshot_msg(self) -> Dict:
        return {
            "result": "success",
            "timestamp": 1612269825817,
            "bids": [
                [65485.47, 47.081829],
                [65485.00, 10.924000]
            ],
            "asks": [
                [65557.70, 16.606555],
                [65558.00, 23.000000]
            ]
        }

    def get_ws_snapshot_msg(self) -> Dict:
        return {
            "feed": "book_snapshot",
            "product_id": self.ex_trading_pair,
            "timestamp": 1612269825817,
            "seq": 326072249,
            "bids": [
                [34892.5, 6385],
                [34892.0, 10924]
            ],
            "asks": [
                [34911.5, 20598],
                [34912.0, 2300]
            ]
        }

    def get_ws_diff_msg(self) -> Dict:
        return {
            "feed": "book",
            "product_id": self.ex_trading_pair,
            "side": "sell",
            "seq": 326094134,
            "price": 34981.0,
            "qty": 1.5,
            "timestamp": 1612269953629
        }

    def get_funding_info_msg(self) -> Dict:
        return {
            "result": "success",
            "serverTime": "2022-06-17T11:00:31.335Z",
            "tickers": [
                {
                    "ask": 49289,
                    "askSize": 139984,
                    "bid": 8634,
                    "bidSize": 1000,
                    "change24h": 1.9974017538161748,
                    "fundingRate": 1.18588737106e-7,
                    "fundingRatePrediction": 1.1852486794e-7,
                    "indexPrice": 21087.8,
                    "last": 49289,
                    "lastSize": 100,
                    "lastTime": "2022-06-17T10:46:35.705Z",
                    "markPrice": 30209.9,
                    "open24h": 49289,
                    "openInterest": 149655,
                    "pair": "XBT:USD",
                    "postOnly": False,
                    "suspended": False,
                    "symbol": "PF_XBTUSD",
                    "tag": "perpetual",
                    "vol24h": 15304,
                    "volumeQuote": 7305.2
                }
            ]
        }

    def get_funding_info_event(self) -> Dict:
        return {
            "feed": "ticker",
            "product_id": self.ex_trading_pair,
            "time": 1676393235406,
            "funding_rate": -6.2604214e-11,
            "funding_rate_prediction": -3.65989977e-10,
            "relative_funding_rate": -1.380384722222e-6,
            "relative_funding_rate_prediction": -8.047629166667e-6,
            "next_funding_rate_time": 1676394000000,
            "index": 21984.54,
            "markPrice": 21979.68641534714,
            "suspended": False,
            "tag": "perpetual"
        }

    def get_predicted_funding_info(self) -> Dict:
        return {
            "ret_code": 0,
            "ret_msg": "ok",
            "ext_code": "",
            "result": {
                "predicted_funding_rate": 0.0001,
                "predicted_funding_fee": 0
            },
            "ext_info": None,
            "time_now": "1577447415.583259",
            "rate_limit_status": 118,
            "rate_limit_reset_ms": 1577447415590,
            "rate_limit": 120
        }

    @aioresponses()
    def test_get_new_order_book_raises_exception(self, mock_api):
        endpoint = CONSTANTS.ORDER_BOOK_ENDPOINT
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=endpoint,
            domain=self.domain,
            api_version="DERIVATIVES"
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=400)
        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(IOError):
            self.async_run_with_timeout(
                self.data_source._order_book_snapshot(self.trading_pair)
            )

    @aioresponses()
    def test_get_new_order_book_successful(self, mock_api):
        endpoint = CONSTANTS.ORDER_BOOK_ENDPOINT
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=endpoint,
            domain=self.domain,
            api_version="DERIVATIVES"
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        resp = self.get_rest_snapshot_msg()
        mock_api.get(regex_url, body=json.dumps(resp))

        order_book: OrderBookMessage = self.async_run_with_timeout(
            self.data_source._order_book_snapshot(self.trading_pair)
        )

        self.assertEqual(OrderBookMessageType.SNAPSHOT, order_book.type)
        self.assertEqual(-1, order_book.trade_id)
        self.assertIsInstance(order_book.update_id, int)  # Just check it's an integer
        self.assertGreater(order_book.update_id, 0)  # Should be positive

        bids = order_book.bids
        asks = order_book.asks
        self.assertEqual(2, len(bids))
        self.assertEqual(2, len(asks))

        first_bid = bids[0]
        self.assertEqual(65485.47, first_bid.price)
        self.assertEqual(47.081829, first_bid.amount)

        first_ask = asks[0]
        self.assertEqual(65557.70, first_ask.price)
        self.assertEqual(16.606555, first_ask.amount)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_subscribes_to_trades_diffs_and_funding_info(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        result_subscribe_diffs = {
            "event": "subscribed",
            "feed": CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC,
            "product_ids": [self.ex_trading_pair]
        }
        result_subscribe_trades = {
            "event": "subscribed",
            "feed": CONSTANTS.WS_TRADES_TOPIC,
            "product_ids": [self.ex_trading_pair]
        }
        result_subscribe_funding = {
            "event": "subscribed",
            "feed": CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC,
            "product_ids": [self.ex_trading_pair]
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_diffs)
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_trades)
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_funding)
        )

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(3, len(sent_messages))
        expected_book_subscription = {
            "event": "subscribe",
            "feed": CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC,
            "product_ids": [self.ex_trading_pair]
        }
        self.assertEqual(expected_book_subscription, sent_messages[0])
        expected_trade_subscription = {
            "event": "subscribe",
            "feed": CONSTANTS.WS_TRADES_TOPIC,
            "product_ids": [self.ex_trading_pair]
        }
        self.assertEqual(expected_trade_subscription, sent_messages[1])
        expected_ticker_subscription = {
            "event": "subscribe",
            "feed": CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC,
            "product_ids": [self.ex_trading_pair]
        }
        self.assertEqual(expected_ticker_subscription, sent_messages[2])

        self.assertTrue(self._is_logged(
            "INFO",
            "Subscribed to public order book, trade and funding info channels..."
        ))

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect")
    def test_listen_for_subscriptions_raises_cancel_exception(self, mock_ws, _: AsyncMock):
        mock_ws.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())
            self.async_run_with_timeout(self.listening_task)

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock):
        mock_ws.side_effect = Exception("TEST ERROR.")
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

        self.async_run_with_timeout(self.resume_test_event.wait(), timeout=5)

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred when listening to order book streams wss://demo-futures.kraken.com/ws/v1. "
                "Retrying in 5.0 seconds..."
            )
        )

    def test_subscribe_to_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source._subscribe_to_channels(mock_ws, [self.trading_pair])
            )
            self.async_run_with_timeout(self.listening_task)

    def test_subscribe_to_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")

        with self.assertRaises(Exception):
            self.listening_task = self.ev_loop.create_task(
                self.data_source._subscribe_to_channels(mock_ws, [self.trading_pair])
            )
            self.async_run_with_timeout(self.listening_task)

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred subscribing to order book trading and ticker streams..."))

    def test_listen_for_trades_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_trades_logs_exception(self):
        incomplete_resp = {
            "feed": "trade",
            "product_id": self.ex_trading_pair,
            "side": "buy",
            "type": "fill",
            # Missing required fields: time, price, qty
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_trades(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public trade updates from exchange"))

    def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()
        trade_event = {
            "feed": "trade",
            "product_id": self.ex_trading_pair,
            "side": "buy",
            "type": "fill",
            "seq": 123456,
            "time": 1612269825817,
            "timestamp": 1612269825817,
            "price": 34892.5,
            "qty": 0.001,
            "order_id": "00c706e1-ba52-5bb0-98d0-bf694bdc69f7",
        }
        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_trades(self.ev_loop, msg_queue))

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(trade_event["seq"], msg.trade_id)
        self.assertEqual(trade_event["timestamp"], msg.timestamp)

    def test_listen_for_order_book_diffs_cancelled(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_order_book_diffs_logs_exception(self):
        incomplete_resp = self.get_ws_diff_msg()
        del incomplete_resp["timestamp"]

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public order book updates from exchange"))

    def test_listen_for_order_book_diffs_successful(self):
        mock_queue = AsyncMock()
        diff_event = self.get_ws_diff_msg()
        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue))

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())
        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(-1, msg.trade_id)
        self.assertEqual(diff_event["timestamp"], msg.timestamp)
        expected_update_id = diff_event["seq"]
        self.assertEqual(expected_update_id, msg.update_id)

        # For a single update, one side should be empty and the other should have one entry
        if diff_event["side"] == "sell":
            self.assertEqual(0, len(msg.bids))
            self.assertEqual(1, len(msg.asks))
            self.assertEqual(diff_event["price"], msg.asks[0].price)
            self.assertEqual(diff_event["qty"], msg.asks[0].amount)
            self.assertEqual(expected_update_id, msg.asks[0].update_id)
        else:
            self.assertEqual(1, len(msg.bids))
            self.assertEqual(0, len(msg.asks))
            self.assertEqual(diff_event["price"], msg.bids[0].price)
            self.assertEqual(diff_event["qty"], msg.bids[0].amount)
            self.assertEqual(expected_update_id, msg.bids[0].update_id)

    @aioresponses()
    def test_listen_for_order_book_snapshots_cancelled_when_fetching_snapshot(self, mock_api):
        endpoint = CONSTANTS.ORDER_BOOK_ENDPOINT
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=endpoint,
            domain=self.domain,
            api_version="DERIVATIVES"
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, exception=asyncio.CancelledError)

        msg_queue: asyncio.Queue = asyncio.Queue()
        with self.assertRaises(asyncio.CancelledError):
            self.async_run_with_timeout(
                self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
            )

    @aioresponses()
    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    def test_listen_for_order_book_snapshots_log_exception(self, mock_api, sleep_mock):
        msg_queue: asyncio.Queue = asyncio.Queue()
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        endpoint = CONSTANTS.ORDER_BOOK_ENDPOINT
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=endpoint,
            domain=self.domain,
            api_version="DERIVATIVES"
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, exception=Exception)

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )
        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error fetching order book snapshot for XBT-USD.")
        )

    @aioresponses()
    def test_listen_for_order_book_snapshots_successful(self, mock_api):
        msg_queue: asyncio.Queue = asyncio.Queue()
        endpoint = CONSTANTS.ORDER_BOOK_ENDPOINT
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=endpoint,
            domain=self.domain,
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        resp = self.get_rest_snapshot_msg()
        mock_api.get(regex_url, body=json.dumps(resp))

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(-1, msg.trade_id)
        self.assertEqual(resp["timestamp"], msg.timestamp)

    def test_listen_for_funding_info_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[self.data_source._funding_info_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_funding_info(msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_funding_info_logs_exception(self):
        incomplete_resp = {
            "feed": CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC,
            "product_id": self.ex_trading_pair,
            "time": 1676393235406,
            "markPrice": "35000.5",
            "funding_rate": "0.0001",
            "next_funding_rate_time": 1676397600000,
            # Missing "index" field to trigger KeyError
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._funding_info_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_funding_info(msg_queue))

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR",
                            "Unexpected error when processing public funding info updates from exchange"))

    def test_listen_for_funding_info_successful(self):
        funding_info_event = self.get_funding_info_event()
        mock_queue = asyncio.Queue()
        mock_queue.put_nowait(funding_info_event)
        self.data_source._message_queue[self.data_source._funding_info_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_funding_info(msg_queue))

            # Wait for the message to be processed
            msg: FundingInfoUpdate = self.async_run_with_timeout(msg_queue.get(), timeout=5)

            self.assertEqual(self.ex_trading_pair, str("PF_XBTUSD"))
            self.assertEqual(Decimal(str(funding_info_event["funding_rate"])), msg.rate)
            self.assertEqual(funding_info_event["next_funding_rate_time"], msg.next_funding_utc_timestamp)
            self.assertEqual(Decimal(str(funding_info_event["index"])), msg.index_price)
            self.assertEqual(Decimal(str(funding_info_event["markPrice"])), msg.mark_price)
        finally:
            if self.listening_task is not None:
                self.listening_task.cancel()
                self.listening_task = None

    @aioresponses()
    def test_get_funding_info(self, mock_api):
        endpoint = CONSTANTS.TICKER_PRICE_ENDPOINT
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=endpoint,
            domain=self.domain,
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = self.get_funding_info_msg()
        mock_api.get(regex_url, body=json.dumps(resp))

        funding_info: FundingInfo = self.async_run_with_timeout(
            self.data_source.get_funding_info(self.trading_pair)
        )

        ticker_info = resp["tickers"][0]
        self.assertEqual(self.trading_pair, funding_info.trading_pair)
        self.assertEqual(Decimal(str(ticker_info["indexPrice"])), funding_info.index_price)
        self.assertEqual(Decimal(str(ticker_info["markPrice"])), funding_info.mark_price)
        self.assertEqual(utils.get_next_funding_timestamp(time.time()), funding_info.next_funding_utc_timestamp)
        self.assertEqual(Decimal(str(ticker_info["fundingRate"])), funding_info.rate)

    def test_get_bids_and_asks_from_rest_msg_data(self):
        """Test parsing order book snapshot from REST response."""
        snapshot = self.get_rest_snapshot_msg()
        bids, asks = self.data_source._get_bids_and_asks_from_rest_msg_data(snapshot)

        # Check bids
        self.assertEqual(2, len(bids))
        self.assertEqual(65485.47, bids[0][0])  # price
        self.assertEqual(47.081829, bids[0][1])  # size
        self.assertEqual(65485.00, bids[1][0])
        self.assertEqual(10.924000, bids[1][1])

        # Check asks
        self.assertEqual(2, len(asks))
        self.assertEqual(65557.70, asks[0][0])  # price
        self.assertEqual(16.606555, asks[0][1])  # size
        self.assertEqual(65558.00, asks[1][0])
        self.assertEqual(23.000000, asks[1][1])

    def test_get_bids_and_asks_from_ws_msg_data(self):
        """Test parsing order book update from WebSocket message."""
        snapshot = self.get_ws_snapshot_msg()
        bids, asks = self.data_source._get_bids_and_asks_from_ws_msg_data(snapshot)

        # Check bids
        self.assertEqual(2, len(bids))
        self.assertEqual(34892.5, bids[0][0])  # price
        self.assertEqual(6385, bids[0][1])  # size
        self.assertEqual(34892.0, bids[1][0])
        self.assertEqual(10924, bids[1][1])

        # Check asks
        self.assertEqual(2, len(asks))
        self.assertEqual(34911.5, asks[0][0])  # price
        self.assertEqual(20598, asks[0][1])  # size
        self.assertEqual(34912.0, asks[1][0])
        self.assertEqual(2300, asks[1][1])

    def test_get_bids_and_asks_from_ws_msg_data_with_empty_levels(self):
        """Test parsing order book update with empty levels (size=0)."""
        snapshot = {
            "feed": "book",
            "product_id": self.ex_trading_pair,
            "timestamp": 1672304484978,
            "seq": 7961638724,
            "bids": [
                [16493.50, 0.000000],  # Empty level
                [16493.00, 0.100000]
            ],
            "asks": [
                [16611.00, 0.029000],
                [16612.00, 0.000000]  # Empty level
            ]
        }
        bids, asks = self.data_source._get_bids_and_asks_from_ws_msg_data(snapshot)

        # Check bids (should skip empty level)
        self.assertEqual(1, len(bids))
        self.assertEqual(16493.00, bids[0][0])
        self.assertEqual(0.100000, bids[0][1])

        # Check asks (should skip empty level)
        self.assertEqual(1, len(asks))
        self.assertEqual(16611.00, asks[0][0])
        self.assertEqual(0.029000, asks[0][1])

    def test_process_order_book_snapshot(self):
        """Test processing order book snapshot message."""
        snapshot = self.get_ws_snapshot_msg()
        message_queue = asyncio.Queue()

        self.data_source._last_sequence_numbers[self.trading_pair] = snapshot["seq"]

        self.async_run_with_timeout(
            self.data_source._parse_order_book_diff_message(snapshot, message_queue)
        )

        message = self.async_run_with_timeout(message_queue.get(), timeout=5)

        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(snapshot["seq"], message.update_id)
        self.assertEqual(snapshot["timestamp"], message.timestamp)
        self.assertEqual(snapshot["seq"], self.data_source._last_sequence_numbers[self.trading_pair])

        bids = message.bids
        asks = message.asks

        self.assertEqual(len(snapshot["bids"]), len(bids))
        self.assertEqual(len(snapshot["asks"]), len(asks))

        first_bid = bids[0]
        first_ask = asks[0]

        self.assertEqual(snapshot["bids"][0][0], first_bid.price)
        self.assertEqual(snapshot["bids"][0][1], first_bid.amount)
        self.assertEqual(snapshot["asks"][0][0], first_ask.price)
        self.assertEqual(snapshot["asks"][0][1], first_ask.amount)

    def test_process_order_book_diff(self):
        """Test processing order book diff message."""
        diff = self.get_ws_diff_msg()
        self.data_source._last_sequence_numbers[self.trading_pair] = diff["seq"] - 1
        message_queue = asyncio.Queue()

        self.async_run_with_timeout(
            self.data_source._parse_order_book_diff_message(diff, message_queue)
        )

        message = self.async_run_with_timeout(message_queue.get(), timeout=5)

        self.assertEqual(OrderBookMessageType.DIFF, message.type)
        self.assertEqual(-1, message.trade_id)
        self.assertEqual(diff["seq"], message.update_id)
        self.assertEqual(diff["timestamp"], message.timestamp)

        bids = message.bids
        asks = message.asks

        self.assertEqual(0 if diff["side"] == "buy" else 1, len(asks))
        self.assertEqual(1 if diff["side"] == "buy" else 0, len(bids))

        first_ask = asks[0] if len(asks) > 0 else None
        first_bid = bids[0] if len(bids) > 0 else None

        if diff["side"] == "buy":
            self.assertIsNone(first_ask)
            self.assertEqual(diff["price"], first_bid.price)
            self.assertEqual(diff["qty"], first_bid.amount)
        else:
            self.assertIsNone(first_bid)
            self.assertEqual(diff["price"], first_ask.price)
            self.assertEqual(diff["qty"], first_ask.amount)

    def test_process_out_of_order_message(self):
        """Test processing out-of-order message (should be ignored)."""
        diff = self.get_ws_diff_msg()
        self.data_source._last_sequence_numbers[self.trading_pair] = diff["seq"] + 1
        message_queue = asyncio.Queue()

        self.async_run_with_timeout(
            self.data_source._parse_order_book_diff_message(diff, message_queue)
        )

        self.assertEqual(0, message_queue.qsize())  # Message should be ignored

    def test_heartbeat_message_handling(self):
        """Test handling of heartbeat messages."""
        heartbeat_message = {
            "feed": CONSTANTS.WS_HEARTBEAT_TOPIC,
            "time": 1534262350627
        }

        # Create a mock message queue
        message_queue = asyncio.Queue()

        # Process heartbeat message
        self.async_run_with_timeout(
            self.data_source._process_message(heartbeat_message, message_queue)
        )

        # Queue should be empty since heartbeat messages are ignored
        self.assertEqual(0, message_queue.qsize())

    def test_unsubscribe_message_handling(self):
        """Test handling of unsubscribe confirmation messages."""
        unsubscribe_message = {
            "event": "unsubscribed",
            "feed": CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC,
            "product_id": self.ex_trading_pair
        }

        # Create a mock message queue
        message_queue = asyncio.Queue()

        # Process unsubscribe message
        self.async_run_with_timeout(
            self.data_source._process_websocket_messages(unsubscribe_message, message_queue)
        )

        # Verify log message
        self.assertTrue(
            self._is_logged("INFO", f"Successfully unsubscribed from {CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC} feed")
        )

    def test_error_message_handling(self):
        """Test handling of error messages."""
        error_message = {
            "event": "error",
            "errorCode": "MARKET_SUSPENDED",
            "message": "Market is suspended"
        }

        # Create a mock message queue
        message_queue = asyncio.Queue()

        # Process error message
        self.async_run_with_timeout(
            self.data_source._process_websocket_messages(error_message, message_queue)
        )

        # Verify error is logged
        self.assertTrue(
            self._is_logged("ERROR", "WebSocket error: MARKET_SUSPENDED - Market is suspended")
        )

    def test_invalid_message_handling(self):
        """Test handling of invalid messages."""
        invalid_message = {
            "feed": "unknown_feed",
            "data": "invalid"
        }

        # Create a mock message queue
        message_queue = asyncio.Queue()

        # Process invalid message
        self.async_run_with_timeout(
            self.data_source._process_websocket_messages(invalid_message, message_queue)
        )

        # Queue should be empty since invalid messages are ignored
        self.assertEqual(0, message_queue.qsize())

    @aioresponses()
    def test_get_order_book_data(self, mock_api):
        url = web_utils.get_rest_url_for_endpoint(
            endpoint=CONSTANTS.ORDER_BOOK_ENDPOINT,
            domain=self.domain
        )
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        resp = {
            "timestamp": 1234567890,
            "bids": [[100.0, 1.0], [99.0, 2.0]],
            "asks": [[101.0, 1.0], [102.0, 2.0]]
        }
        mock_api.get(regex_url, body=json.dumps(resp))

        order_book_snapshot = self.async_run_with_timeout(
            self.data_source._order_book_snapshot(
                trading_pair=self.trading_pair
            )
        )

        self.assertEqual(OrderBookMessageType.SNAPSHOT, order_book_snapshot.type)
        self.assertEqual(1234567890, order_book_snapshot.timestamp)
        self.assertEqual(1234567890, order_book_snapshot.update_id)
        self.assertEqual(self.trading_pair, order_book_snapshot.content["trading_pair"])
        self.assertEqual([[100.0, 1.0], [99.0, 2.0]], order_book_snapshot.content["bids"])
        self.assertEqual([[101.0, 1.0], [102.0, 2.0]], order_book_snapshot.content["asks"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_order_book_diffs(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        diff_msg = {
            "feed": CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC,
            "product_id": self.ex_trading_pair,  # Exchange format (PF_XBTUSD)
            "side": "buy",
            "price": 100.0,
            "qty": 1.0,
            "timestamp": 1234567890
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(diff_msg)
        )

        output_queue = asyncio.Queue()
        self.async_tasks.append(
            asyncio.get_event_loop().create_task(
                self.data_source._parse_order_book_diff_message(
                    raw_message=diff_msg,
                    message_queue=output_queue
                )
            )
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(1234567890, msg.timestamp)
        self.assertEqual(1234567890, msg.update_id)
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])  # Expect Hummingbot format (XBT-USD)
        self.assertEqual([[100.0, 1.0]], msg.content["bids"])
        self.assertEqual([], msg.content["asks"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_order_book_snapshots(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        snapshot_msg = {
            "feed": CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC,
            "product_id": self.ex_trading_pair,
            "bids": [[100.0, 1.0], [99.0, 2.0]],
            "asks": [[101.0, 1.0], [102.0, 2.0]],
            "timestamp": 1234567890
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(snapshot_msg)
        )

        output_queue = asyncio.Queue()
        self.async_tasks.append(
            asyncio.get_event_loop().create_task(
                self.data_source._parse_order_book_diff_message(
                    raw_message=snapshot_msg,
                    message_queue=output_queue
                )
            )
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(1234567890, msg.timestamp)
        self.assertEqual(1234567890, msg.update_id)
        self.assertEqual(self.ex_trading_pair, msg.content["trading_pair"])
        self.assertEqual([[100.0, 1.0], [99.0, 2.0]], msg.content["bids"])
        self.assertEqual([[101.0, 1.0], [102.0, 2.0]], msg.content["asks"])
