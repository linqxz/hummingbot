import asyncio
import time
from typing import Any, Dict, List, Optional

from hummingbot.connector.derivative.kraken_perpetual import (
    kraken_perpetual_constants as CONSTANTS,
    kraken_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_auth import KrakenPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSResponse
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class KrakenPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: KrakenPerpetualAuth,
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._api_factory = api_factory
        self._domain = domain
        self._ws_assistants: List[WSAssistant] = []
        self._last_recv_time = 0
        self._challenge = None
        self._signed_challenge = None


    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message

        :return: the timestamp of the last received message in seconds
        """
        t = 0.0
        if len(self._ws_assistants) > 0:
            t = min([wsa.last_recv_time for wsa in self._ws_assistants])
        return t

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue
        """
        ws = None
        while True:
            try:
                try:
                    ws: WSAssistant = await self._connected_websocket_assistant()
                    self._ws_assistants.append(ws)
                    await self._subscribe_to_feeds(ws)
                    
                    # Process messages
                    async for ws_response in ws.iter_messages():
                        self._last_recv_time = time.time()
                        await self._process_event_message(ws_response.data, output)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().error(f"Error in user stream: {str(e)}", exc_info=True)
                    raise
                finally:
                    ws and await ws.disconnect()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in user stream: {str(e)}. Retrying in 5 seconds...", exc_info=True)
                await asyncio.sleep(5)

    async def _subscribe_to_feeds(self, ws: WSAssistant):
        """Subscribe to all required private feeds"""
        try:
            self.logger().info("\n=== Starting private feed subscriptions ===")
            
            # Define all private feeds we want to subscribe to
            subscriptions = [
                CONSTANTS.WS_HEARTBEAT_TOPIC,            # Connection health check
                CONSTANTS.WS_BALANCES_TOPIC,             # Balance & margin updates
                CONSTANTS.WS_ACCOUNT_LOG_TOPIC,          # Account activity log
                CONSTANTS.WS_FILLS_TOPIC,                # Trade fills
                CONSTANTS.WS_OPEN_POSITIONS_TOPIC,       # Position updates
                CONSTANTS.WS_OPEN_ORDERS_VERBOSE_TOPIC,  # Detailed order updates
                CONSTANTS.WS_NOTIFICATIONS_AUTH_TOPIC,   # Account notifications
            ]
            
            self.logger().info(f"Attempting to subscribe to feeds: {', '.join(subscriptions)}")
            
            # Send all subscription requests
            for feed in subscriptions:
                # Construct payload based on feed type
                if feed == CONSTANTS.WS_HEARTBEAT_TOPIC:
                    payload = {
                        "event": "subscribe",
                        "feed": feed
                    }
                else:
                    payload = self._auth.get_ws_subscribe_payload(
                        feed=feed,
                        challenge=self._challenge,
                        signed_challenge=self._signed_challenge
                    )
                
                await ws.send(WSJSONRequest(payload=payload))

            self.logger().info("Subscription requests sent for all channels")

        except Exception as e:
            self.logger().error(f"Error subscribing to feeds: {str(e)}")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """Process received events and put them in the events queue"""
        try:
            if not event_message:
                return

            # Handle subscription responses
            if event_message.get("event") == "subscribed":
                self.logger().info(f"Successfully subscribed to {event_message.get('feed')} feed")
                return
            elif event_message.get("event") == "error":
                self.logger().error(f"Error in user stream: {event_message.get('message', '')}")
                return

            # Process different message types
            feed = event_message.get("feed")
            if not feed:
                return

            # Convert message to the expected format
            formatted_message = self._format_message(event_message)
            if formatted_message:
                await queue.put(formatted_message)

        except Exception as e:
            self.logger().error(f"Error processing event message: {str(e)}", exc_info=True)

    def _format_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Format the message based on its feed type"""
        try:
            feed = message.get("feed")
            timestamp = message.get("timestamp", int(time.time() * 1000))

            if feed == CONSTANTS.WS_HEARTBEAT_TOPIC:
                self.logger().debug(f"♥ Heartbeat received: {message}")
                return None
            elif feed == CONSTANTS.WS_BALANCES_TOPIC:
                return {
                    "event_type": "balance",
                    "timestamp": timestamp,
                    "data": message
                }
            elif feed == CONSTANTS.WS_ACCOUNT_LOG_TOPIC:
                return {
                    "event_type": "account_log",
                    "timestamp": timestamp,
                    "data": message,
                    "feed": message.get("feed")
                }
            elif feed == CONSTANTS.WS_FILLS_TOPIC:
                return {
                    "event_type": "trade",
                    "timestamp": timestamp,
                    "data": message,
                    "feed": message.get("feed")
                }
            elif feed == CONSTANTS.WS_OPEN_POSITIONS_TOPIC:
                return {
                    "event_type": "position",
                    "timestamp": timestamp,
                    "data": message,
                    "feed": message.get("feed")
                }
            elif feed == CONSTANTS.WS_OPEN_ORDERS_VERBOSE_TOPIC:
                return {
                    "event_type": "order",
                    "timestamp": timestamp,
                    "data": message,
                    "feed": message.get("feed")
                }
            elif feed == CONSTANTS.WS_NOTIFICATIONS_AUTH_TOPIC:
                return {
                    "event_type": "notification",
                    "timestamp": timestamp,
                    "data": message,
                    "feed": message.get("feed")
                }
            else:
                self.logger().debug(f"Received message from unknown feed {feed}: {message}")
                return None
        except Exception as e:
            self.logger().error(f"Error formatting message: {str(e)}")
            return None

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Create and connect a WebSocket assistant"""
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_PRIVATE_URLS[self._domain],
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
        )
        
        # Handle authentication
        await self._auth.ws_authenticate(ws)
        return ws

    @staticmethod
    def _get_server_timestamp():
        return web_utils.get_current_server_time()

    def _time(self):
        return time.time()
