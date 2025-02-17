import asyncio
import json
from typing import Awaitable
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_auth import KrakenPerpetualAuth
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_user_stream_data_source import (
    KrakenPerpetualUserStreamDataSource,
)
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant


class KrakenPerpetualUserStreamDataSourceTests(TestCase):
    # the level is required to receive logs from the data source loger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant()
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        auth = KrakenPerpetualAuth(
            api_key="drUfSSmBbDpcIpwpqK0OBTcGLdAYZJU+NlPIsHaKspu/8feT2YSKl+Jw",  # Example base64 API key
            secret_key="Ds0wtsHaXlAby/Vnoil59Q+yJIrJwZGUlgECD3+qEvFcTFfacJi2LrSRzAoqwBAeZk4pGXSmyyIW0uDymZ3olw==",  # Example base64 secret
            time_provider=self.mock_time_provider
        )
        api_factory = web_utils.build_api_factory(auth=auth)
        self.data_source = KrakenPerpetualUserStreamDataSource(
            auth=auth, api_factory=api_factory, domain=self.domain
        )
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.mocking_assistant = NetworkMockingAssistant()

        self.resume_test_event = asyncio.Event()

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    @staticmethod
    def _challenge_message() -> str:
        message = {
            "event": "challenge",
            "challenge": "c094497e-9b5f-40da-a122-3751c39b107f"
        }
        return json.dumps(message)

    @staticmethod
    def _authentication_response(authenticated: bool, ret_msg: str) -> str:
        message = {
            "event": "challenge",
            "success": authenticated,
            "error": ret_msg if not authenticated else ""
        }
        return json.dumps(message)

    @staticmethod
    def _subscription_response(subscribed: bool, subscription: str) -> str:
        request = {
            "event": "subscribe",
            "feed": subscription,
            "product_ids": ["PF_XBTUSD"]
        }
        message = {
            "feed": subscription,
            "success": subscribed,
            "error": "" if subscribed else "Subscription failed"
        }
        return json.dumps(message)

    def _raise_exception(self, exception_class):
        raise exception_class

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listening_process_authenticates_and_subscribes_to_events(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        initial_last_recv_time = self.data_source.last_recv_time

        # Add the authentication messages for the websocket
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._challenge_message()
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._authentication_response(True, "")
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._subscription_response(True, "open_orders")
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._subscription_response(True, "fills")
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._subscription_response(True, "balances")
        )

        self.listening_task = asyncio.get_event_loop().create_task(
            self.data_source.listen_for_user_stream(messages)
        )
        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        self.assertTrue(
            self._is_logged("INFO", "Subscribed to private open_orders, fills, and balances channels")
        )

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(ws_connect_mock.return_value)
        self.assertEqual(4, len(sent_messages))  # Auth + 3 subscriptions

        auth_request = sent_messages[0]
        self.assertEqual("challenge", auth_request["event"])

        orders_sub = sent_messages[1]
        self.assertEqual({
            "event": "subscribe",
            "feed": "open_orders",
            "api_key": auth_request["api_key"]
        }, orders_sub)

        fills_sub = sent_messages[2]
        self.assertEqual({
            "event": "subscribe",
            "feed": "fills",
            "api_key": auth_request["api_key"]
        }, fills_sub)

        balances_sub = sent_messages[3]
        self.assertEqual({
            "event": "subscribe",
            "feed": "balances",
            "api_key": auth_request["api_key"]
        }, balances_sub)

        self.assertGreater(self.data_source.last_recv_time, initial_last_recv_time)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_authentication_failure(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()
        ret_msg = "FAILED FOR SOME REASON"

        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._challenge_message()
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            ws_connect_mock.return_value,
            self._authentication_response(False, ret_msg=ret_msg)
        )

        self.listening_task = asyncio.get_event_loop().create_task(
            self.data_source.listen_for_user_stream(messages))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        self.assertTrue(self._is_logged("ERROR", f"Error during WebSocket authentication: Authentication failed: {ret_msg}"))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_does_not_queue_empty_payload(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            self._challenge_message()
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            self._authentication_response(True, "")
        )
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, "")

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_connection_failed(self, mock_ws):
        mock_ws.side_effect = lambda *arg, **kwars: self._create_exception_and_unlock_test_with_event(
            Exception("TEST ERROR."))

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged(
                "ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds..."
            )
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listening_process_canceled_on_cancel_exception(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = asyncio.get_event_loop().create_task(
                self.data_source.listen_for_user_stream(messages))
            # We need to wait a bit for the task to start
            self.async_run_with_timeout(asyncio.sleep(0.5))
            self.listening_task.cancel()
            self.async_run_with_timeout(self.listening_task)
