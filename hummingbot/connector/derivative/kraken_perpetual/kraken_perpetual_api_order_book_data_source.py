import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from hummingbot.connector.derivative.kraken_perpetual import (
    kraken_perpetual_constants as CONSTANTS,
    kraken_perpetual_utils as utils,
    kraken_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_utils import get_exchange_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book import OrderBookMessage
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_derivative import KrakenPerpetualDerivative


class KrakenPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    INITIAL_BACKOFF = 5.0
    MAX_BACKOFF = 300.0  # 5 minutes
    SUBSCRIPTION_TIMEOUT = 30.0  # 30 seconds

    def __init__(
        self,
        trading_pairs: List[str],
        connector: 'KrakenPerpetualDerivative',
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._nonce_provider = NonceCreator.for_microseconds()
        self._last_sequence_numbers = defaultdict(lambda: -1)
        self._ws_url = web_utils.get_ws_url(domain)
        self._funding_info = {}
        self._funding_info_event = asyncio.Event()
        self._mapping_initialized = asyncio.Event()
        self._check_trading_pair_mapping_task = asyncio.create_task(self._check_trading_pair_mapping())
        self._funding_info_initialized = asyncio.Event()
        self._check_funding_info_initialized_task = asyncio.create_task(self._initialize_funding_info())
        
        # Initialize message queues for different feed types
        self._message_queue = {
            self._snapshot_messages_queue_key: asyncio.Queue(),
            self._diff_messages_queue_key: asyncio.Queue(),
            self._trade_messages_queue_key: asyncio.Queue(),
            self._funding_info_messages_queue_key: asyncio.Queue(),
            "ticker": asyncio.Queue(),
            "ticker_lite": asyncio.Queue(),
            "heartbeat": asyncio.Queue(),
        }
        
        # Initialize order book queues for each trading pair
        self._order_book_snapshot_queues = defaultdict(asyncio.Queue)
        self._order_book_diff_queues = defaultdict(asyncio.Queue)
        
        # self.logger().info("Initializing Kraken Perpetual order book data source...")

    async def _check_trading_pair_mapping(self):
        """Periodically check if trading pair mapping is initialized and set the event."""
        # self.logger().info("Starting trading pair mapping check...")
        while True:
            try:
                if hasattr(self._connector, '_trading_pair_symbol_map') and self._connector._trading_pair_symbol_map:
                    # self.logger().info("Trading pair mapping is now initialized.")
                    # self.logger().info(f"Found {len(self._connector._trading_pair_symbol_map)} trading pairs")
                    # self.logger().debug(f"Trading pair map: {self._connector._trading_pair_symbol_map}")
                    self._mapping_initialized.set()
                    break
                else:
                    # self.logger().debug("Waiting for trading pair mapping to be initialized...")
                    if hasattr(self._connector, '_trading_pair_symbol_map'):
                        # self.logger().debug("Map exists but is empty")
                        pass
                    else:
                        # self.logger().debug("Map attribute not found on connector")
                        pass
                await asyncio.sleep(0.1)
            except AttributeError as ae:
                self.logger().debug(f"Connector not ready yet: {str(ae)}")
                await asyncio.sleep(1.0)
            except Exception as e:
                self.logger().error(f"Error checking trading pair mapping: {str(e)}", exc_info=True)
                await asyncio.sleep(1.0)

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    def _convert_funding_message_to_order_book_row(self, message: Dict[str, Any]) -> FundingInfo:
        try:
            trading_pair = message["product_id"]
            index_price = Decimal(message["index"])
            mark_price = Decimal(message["markPrice"])
            next_funding_time = int(message["next_funding_rate_time"])
            funding_rate = Decimal(message["funding_rate"])

            funding_info = FundingInfo(
                trading_pair=trading_pair,
                index_price=index_price,
                mark_price=mark_price,
                next_funding_utc_timestamp=next_funding_time,
                rate=funding_rate,
            )
            self._funding_info[trading_pair] = funding_info
            self._funding_info_event.set()
            return funding_info
        except KeyError:
            self.logger().error("Invalid funding info message received")
            return None

    async def get_funding_info(self, trading_pair: str) -> Optional[FundingInfo]:
        """Get funding information for a specific trading pair."""
        self.logger().info(f"\n=== Getting funding info for {trading_pair} ===")
        try:
            exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
            self.logger().info(f"Using exchange symbol: {exchange_symbol}")

            rest_assistant = await self._api_factory.get_rest_assistant()
            endpoint = CONSTANTS.HISTORICAL_FUNDING_RATES_ENDPOINT
            params = {"symbol": exchange_symbol}
            self.logger().info(f"Making request to endpoint: {endpoint} with params: {params}")

            # First get historical funding rates
            resp = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(endpoint=endpoint, domain=self._domain),
                method=RESTMethod.GET,
                params=params,
                throttler_limit_id=web_utils.GET_LIMIT_ID,  # Use general GET limit ID
            )

            if resp.get("result") == "success":
                rates = resp.get("rates", [])
                if rates:
                    # Sort rates by timestamp in descending order to get the most recent first
                    sorted_rates = sorted(
                        rates,
                        key=lambda x: datetime.strptime(x["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp(),
                        reverse=True
                    )
                    latest_rate = sorted_rates[0]  # Get the most recent rate
                    self.logger().info(f"Processing latest rate: {latest_rate}")

                    # Get mark price from ticker endpoint
                    ticker_endpoint = CONSTANTS.TICKER_SYMBOL_ENDPOINT.format(symbol=exchange_symbol)
                    ticker_resp = await rest_assistant.execute_request(
                        url=web_utils.public_rest_url(endpoint=ticker_endpoint, domain=self._domain),
                        method=RESTMethod.GET,
                        throttler_limit_id=web_utils.GET_LIMIT_ID,  # Use general GET limit ID
                    )
                    
                    if ticker_resp.get("result") == "success":
                        ticker_data = ticker_resp.get("tickers", [{}])[0]
                        mark_price = Decimal(str(ticker_data.get("markPrice", "0")))
                        index_price = Decimal(str(ticker_data.get("indexPrice", mark_price)))
                        
                        # Parse timestamp with proper error handling for both formats
                        try:
                            timestamp_str = latest_rate.get("timestamp", "")
                            if timestamp_str:
                                try:
                                    # Try parsing with milliseconds first
                                    next_funding_time = int(datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp())
                                except ValueError:
                                    # If that fails, try without milliseconds
                                    next_funding_time = int(datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
                                
                                # Add 8 hours to get the next funding time
                                next_funding_time += 8 * 60 * 60
                            else:
                                next_funding_time = int(time.time()) + (8 * 60 * 60)  # Default to 8 hours from now
                        except (ValueError, TypeError) as e:
                            self.logger().warning(f"Error parsing timestamp {timestamp_str}: {e}")
                            next_funding_time = int(time.time()) + (8 * 60 * 60)  # Default to 8 hours from now
                        
                        funding_info = FundingInfo(
                            trading_pair=trading_pair,
                            index_price=index_price,
                            mark_price=mark_price,
                            next_funding_utc_timestamp=next_funding_time,
                            rate=Decimal(str(latest_rate.get("fundingRate", "0"))),
                        )
                        self.logger().info(f"Created funding info: {funding_info}")
                        return funding_info
                    else:
                        self.logger().warning(f"Error in ticker response: {ticker_resp.get('error', 'Unknown error')}")
                else:
                    self.logger().warning(f"No funding rates found for {trading_pair}")
            else:
                self.logger().warning(f"Error in funding info response: {resp.get('error', 'Unknown error')}")

            # If we get here, something went wrong. Return a default FundingInfo
            self.logger().warning(f"Using default funding info for {trading_pair}")
            return FundingInfo(
                trading_pair=trading_pair,
                index_price=Decimal("0"),
                mark_price=Decimal("0"),
                next_funding_utc_timestamp=int(time.time()) + (8 * 60 * 60),  # Default to 8 hours from now
                rate=Decimal("0"),
            )

        except Exception as e:
            self.logger().error(f"Error getting funding info for {trading_pair}: {str(e)}", exc_info=True)
            # Return a default FundingInfo instead of None
            return FundingInfo(
                trading_pair=trading_pair,
                index_price=Decimal("0"),
                mark_price=Decimal("0"),
                next_funding_utc_timestamp=int(time.time()) + (8 * 60 * 60),  # Default to 8 hours from now
                rate=Decimal("0"),
            )

    async def _initialize_funding_info(self):
        """Initialize funding info for all trading pairs."""
        self.logger().info("\n=== Initializing funding info ===")
        try:
            await self._mapping_initialized.wait()
            self.logger().info("Trading pair mapping is ready, proceeding with funding info initialization")
            
            for trading_pair in self._trading_pairs:
                try:
                    self.logger().info(f"Getting funding info for {trading_pair}")
                    funding_info = await self.get_funding_info(trading_pair)
                    if funding_info:
                        self._funding_info[trading_pair] = funding_info
                        self.logger().info(f"Initialized funding info for {trading_pair}: {funding_info}")
                    else:
                        self.logger().warning(f"Could not get funding info for {trading_pair}")
                except Exception as e:
                    self.logger().error(f"Error getting funding info for {trading_pair}: {str(e)}", exc_info=True)
            
            if self._funding_info:
                self.logger().info(f"Successfully initialized funding info for {len(self._funding_info)} pairs")
                self._funding_info_initialized.set()
            else:
                self.logger().warning("No funding info was initialized")
        except Exception as e:
            self.logger().error(f"Error in funding info initialization: {str(e)}", exc_info=True)
            raise

    async def listen_for_subscriptions(self):
        """Subscribe to the order book, trade, and funding info channels."""
        try:
            self.logger().info("\n==== Starting WebSocket Subscription Process ====")
            self.logger().info("Waiting for trading pair mapping and funding info to be initialized...")
            await asyncio.wait([self._mapping_initialized.wait(), self._funding_info_initialized.wait()], timeout=30.0)
            
            if not self._mapping_initialized.is_set():
                self.logger().warning("Trading pair mapping not initialized after timeout")
            if not self._funding_info_initialized.is_set():
                self.logger().warning("Funding info not initialized after timeout")

            self.logger().info("Creating WebSocket connection...")
            ws = await self._api_factory.get_ws_assistant()
            await ws.connect(ws_url=self._ws_url)
            self.logger().info("✓ WebSocket connection established")

            await self._subscribe_to_channels(ws, self._trading_pairs)
            self.logger().info("\n==== Starting Message Processing Loop ====")

            # Track subscription states
            subscribed_feeds = set()
            expected_feeds = set(self._subscriptions)

            # Process incoming messages
            async for msg in ws.iter_messages():
                try:
                    self.logger().debug(f"Raw message received: {msg}")
                    
                    if isinstance(msg.data, bytes):
                        msg_data = msg.data.decode('utf-8')
                        self.logger().debug("Decoded bytes message to string")
                    else:
                        msg_data = msg.data
                        
                    self.logger().debug(f"Processing message data: {msg_data}")
                    
                    msg_json = json.loads(msg_data) if isinstance(msg_data, str) else msg_data
                    self.logger().debug(f"Parsed message: {msg_json}")
                    
                    # Handle subscription confirmations
                    if msg_json.get("event") == "subscribed":
                        feed = msg_json.get("feed")
                        subscribed_feeds.add(feed)
                        self.logger().info(f"✓ Received subscription confirmation for {feed}")
                        self.logger().info(f"Subscribed feeds: {len(subscribed_feeds)}/{len(expected_feeds)} - {sorted(subscribed_feeds)}")
                        continue
                    elif msg_json.get("event") == "error":
                        self.logger().error(f"Subscription error: {msg_json}")
                        continue
                    
                    # Log message details based on feed type
                    feed = msg_json.get("feed")
                    if feed == CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC:
                        self.logger().info(f"Order book message received - Type: {'snapshot' if 'bids' in msg_json else 'delta'}")
                        self.logger().debug(f"Order book message content: {msg_json}")
                    elif feed == CONSTANTS.WS_TRADES_TOPIC:
                        self.logger().info("Trade message received")
                        self.logger().debug(f"Trade message content: {msg_json}")
                    elif feed == CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC:
                        self.logger().info("Instrument info message received")
                        self.logger().debug(f"Instrument info content: {msg_json}")
                    elif feed == "heartbeat":
                        self.logger().debug("Heartbeat received")
                    else:
                        self.logger().info(f"Message received from feed: {feed}")
                        self.logger().debug(f"Message content: {msg_json}")
                    
                    self.logger().debug("Passing message to _process_message")
                    await self._process_message(msg_json, self._message_queue)
                    self.logger().debug("Message processing complete")
                    
                except Exception as e:
                    self.logger().error(
                        f"Error processing message: {str(e)}",
                        exc_info=True
                    )
                    
        except asyncio.CancelledError:
            self.logger().info("WebSocket subscription task cancelled")
            raise
        except Exception as e:
            self.logger().error(
                f"Unexpected error in WebSocket subscription loop: {str(e)}",
                exc_info=True
            )
            await self._sleep(5.0)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.get_ws_url(self._domain),
            message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE
        )
        return ws

    async def _subscribe_to_channels(self, ws: WSAssistant, trading_pairs: List[str]):
        """Subscribe to all public WebSocket channels."""
        try:
            self.logger().info("\n=== Subscribing to public WebSocket channels ===")
            
            # Convert trading pairs to exchange symbols
            symbols = [await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                      for trading_pair in trading_pairs]
            
            self.logger().info(f"Converting trading pairs to symbols:")
            for tp, symbol in zip(trading_pairs, symbols):
                self.logger().info(f"{tp} -> {symbol}")
            
            # Define all feeds we want to subscribe to
            self._subscriptions = [
                CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC,
                CONSTANTS.WS_TRADES_TOPIC,
                CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC,
                CONSTANTS.WS_INSTRUMENTS_INFO_LITE_TOPIC,
                CONSTANTS.WS_HEARTBEAT_TOPIC,
            ]
            
            # Subscribe to all feeds
            for feed in self._subscriptions:
                try:
                    # Construct payload based on feed type
                    payload = {
                        "event": "subscribe",
                        "feed": feed,
                    }
                    # Add product_ids for all feeds except heartbeat
                    if feed != CONSTANTS.WS_HEARTBEAT_TOPIC:
                        payload["product_ids"] = symbols
                    
                    self.logger().info(f"Sending subscription request for {feed}: {payload}")
                    subscribe_request = WSJSONRequest(payload=payload, is_auth_required=False)
                    await ws.send(subscribe_request)
                    self.logger().info(f"✓ Subscription request sent for {feed}")
                except Exception as e:
                    self.logger().error(f"Error subscribing to {feed}: {str(e)}", exc_info=True)
            
            self.logger().info("Subscription requests sent for all public channels")
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Error subscribing to channels: {str(e)}",
                exc_info=True
            )
            raise

    async def _wait_for_mapping_ready(self, timeout: float = 30) -> bool:
        """
        Wait for the trading pair mapping to be initialized.
        :param timeout: The maximum time to wait in seconds
        :return: True if mapping is ready, False if timeout occurred
        """
        try:
            # Wait for the mapping initialized event
            self.logger().info(f"Waiting up to {timeout} seconds for trading pair mapping to be ready...")
            is_ready = await asyncio.wait_for(self._mapping_initialized.wait(), timeout=timeout)
            
            if is_ready:
                self.logger().info(f"Trading pair mapping initialized with {len(self._connector._trading_pair_symbol_map)} pairs")
                self.logger().debug(f"Current mapping: {self._connector._trading_pair_symbol_map}")
            else:
                self.logger().warning("Timeout waiting for trading pair mapping to be ready")
            
            return is_ready
        except asyncio.TimeoutError:
            self.logger().warning(f"Timeout after {timeout} seconds waiting for trading pair mapping")
            return False
        except Exception as e:
            self.logger().error(f"Error checking mapping status: {str(e)}", exc_info=True)
            return False

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "feed" in event_message:
            feed = event_message["feed"]
            if feed == CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC:
                channel = self._diff_messages_queue_key
            elif feed == CONSTANTS.WS_TRADES_TOPIC:
                channel = self._trade_messages_queue_key
            elif feed == CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC:
                channel = self._funding_info_messages_queue_key
        return channel

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if "event" not in data:  # Only process messages with data, not subscription responses
                channel = self._channel_originating_message(data)
                if channel == "":
                    continue
                message_queue = self._message_queue[channel]
                await message_queue.put(data)

    async def _handle_subscription_message(self, message: Dict[str, Any]):
        """Handle subscription confirmation and error messages."""
        event = message.get("event", "")
        feed = message.get("feed", "")

        if event == "subscribed":
            self.logger().info(f"Successfully subscribed to {feed} feed")
        elif event == "error":
            self.logger().error(f"Error in {feed} subscription: {message.get('message', '')}")

    async def _process_message(self, msg: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Processes received WebSocket messages and routes them to appropriate handling methods.
        :param msg: The received message in dictionary format
        :param message_queue: The queue to put processed messages into
        """
        try:
            feed = msg.get("feed")
            self.logger().debug(f"Processing message from feed: {feed}")
            
            if feed in [CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC, "book_snapshot", "book"]:
                # Process order book messages
                if feed == "book_snapshot" or "bids" in msg or "asks" in msg:
                    self.logger().info(f"Processing order book snapshot for {msg.get('product_id')}")
                    await self._process_order_book_message(msg)
                else:
                    self.logger().info(f"Processing order book delta for {msg.get('product_id')}")
                    await self._process_order_book_message(msg)
                    
            elif feed == CONSTANTS.WS_TRADES_TOPIC:
                # Process trade messages
                self.logger().info(f"Processing trade message for {msg.get('product_id')}")
                await self._process_trade_message(msg)
                
            elif feed == CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC:
                # Process funding info messages
                self.logger().info(f"Processing funding info for {msg.get('product_id')}")
                await self._parse_funding_info_message(msg, message_queue)
                
            elif feed == "heartbeat":
                # Process heartbeat - already handled in main loop
                self.logger().debug("Received heartbeat")
                pass
                
            else:
                self.logger().warning(f"Received message from unhandled feed: {feed}")
                self.logger().debug(f"Message content: {msg}")
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error processing message from feed {feed}: {str(e)}", exc_info=True)
            self.logger().error(f"Message content: {msg}")
            raise

    async def _process_order_book_message(self, message: Dict[str, Any]):
        """Process order book update and snapshot messages."""
        try:
            symbol = message.get("product_id", "")
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            timestamp = message.get("timestamp", int(time.time() * 1e3))  # Default to current time in milliseconds
            sequence_number = message.get("seq", int(time.time() * 1e3))

            self.logger().debug(f"\n=== Processing Order Book Message ===\n"
                              f"Feed Type: {message.get('feed')}\n"
                              f"Trading Pair: {trading_pair}\n"
                              f"Sequence: {sequence_number}\n"
                              f"Raw Message: {message}")

            # Handle order book snapshot
            if message.get("feed") == "book_snapshot" or ("bids" in message and "asks" in message):
                self.logger().debug(f"Processing snapshot message for {trading_pair}")
                
                # Convert bids and asks to the expected format
                bids, asks = self._get_bids_and_asks_from_ws_msg_data(message)
                
                snapshot_msg = OrderBookMessage(
                    message_type=OrderBookMessageType.SNAPSHOT,
                    content={
                        "trading_pair": trading_pair,
                        "update_id": sequence_number,
                        "bids": bids,
                        "asks": asks,
                    },
                    timestamp=timestamp * 1e-3  # Convert to seconds
                )
                
                self.logger().info(f"Created snapshot for {trading_pair}:"
                                 f"\n  Timestamp: {snapshot_msg.timestamp}"
                                 f"\n  Sequence: {sequence_number}"
                                 f"\n  # of Bids: {len(bids)}"
                                 f"\n  # of Asks: {len(asks)}"
                                 f"\n  First Bid: {bids[0] if bids else 'None'}"
                                 f"\n  First Ask: {asks[0] if asks else 'None'}")
                
                await self._order_book_snapshot_queues[trading_pair].put(snapshot_msg)
                self.logger().debug(f"Queued snapshot message for {trading_pair}")
                
            # Handle order book delta
            else:
                side = message.get("side", "").lower()
                price = float(message.get("price", 0))
                quantity = float(message.get("qty", 0))
                
                self.logger().debug(f"\n=== Processing Delta Message ===\n"
                                  f"Trading Pair: {trading_pair}\n"
                                  f"Side: {side}\n"
                                  f"Price: {price}\n"
                                  f"Quantity: {quantity}\n"
                                  f"Action: {'Remove' if quantity == 0 else 'Update'}")
                
                # For deletions (qty = 0), we need to send a diff message that will remove the price level
                # The order book tracker will handle qty = 0 by removing the price level
                bids = [[price, quantity]] if side == "buy" else []
                asks = [[price, quantity]] if side == "sell" else []
                
                diff_msg = OrderBookMessage(
                    message_type=OrderBookMessageType.DIFF,
                    content={
                        "trading_pair": trading_pair,
                        "update_id": sequence_number,
                        "bids": bids,
                        "asks": asks,
                    },
                    timestamp=timestamp * 1e-3  # Convert to seconds
                )
                
                self.logger().info(f"Created delta for {trading_pair}:"
                                 f"\n  Timestamp: {diff_msg.timestamp}"
                                 f"\n  Sequence: {sequence_number}"
                                 f"\n  Side: {side.upper()}"
                                 f"\n  Price: {price}"
                                 f"\n  Quantity: {quantity}"
                                 f"\n  Action: {'Remove' if quantity == 0 else 'Update'}"
                                 f"\n  Message Type: {diff_msg.type}")
                
                await self._order_book_diff_queues[trading_pair].put(diff_msg)
                self.logger().debug(f"Queued delta message for {trading_pair}")
                
        except Exception as e:
            self.logger().error(f"Error processing order book message: {str(e)}", exc_info=True)
            self.logger().error(f"Message: {message}")
            raise

    async def _process_trade_message(self, message: Dict[str, Any]):
        """Process trade messages."""
        try:
            # Handle string messages by attempting to parse them as JSON
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                except json.JSONDecodeError:
                    self.logger().error(f"Failed to parse trade message as JSON: {message}")
                    return

            # Validate message type
            if not isinstance(message, (dict, OrderBookMessage)):
                self.logger().error(f"Received invalid message type: {type(message)}")
                return

            # Handle pre-formatted OrderBookMessage
            if isinstance(message, OrderBookMessage):
                await self._message_queue[self._trade_messages_queue_key].put(message)
                return

            # Process dictionary message
            if message.get("event") == "subscribed":
                return

            # Validate feed type
            feed = message.get("feed")
            if not feed:
                self.logger().error(f"Message missing feed field: {message}")
                return
            if feed != CONSTANTS.WS_TRADES_TOPIC:
                return  # Skip non-trade messages

            # Validate required fields
            required_fields = ["product_id", "price", "qty", "side", "seq"]
            missing_fields = [field for field in required_fields if field not in message]
            if missing_fields:
                self.logger().error(f"Message missing required fields {missing_fields}: {message}")
                return

            # Convert message to order book row format
            msg = self._convert_trade_message_to_order_book_row(message)
            await self._message_queue[self._trade_messages_queue_key].put(msg)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Unexpected error when processing public trade updates from exchange: {str(e)}",
                exc_info=True
            )
            await self._sleep(5.0)

    async def _process_ticker_message(self, message: Dict[str, Any]):
        """Process ticker messages."""
        try:
            # Extract trading pair
            symbol = message.get("product_id")
            if not symbol:
                self.logger().warning(f"No product_id in ticker message: {message}")
                return

            # Convert to internal trading pair format
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)

            # Put message in ticker queue
            await self._message_queue["ticker"].put({
                "trading_pair": trading_pair,
                "timestamp": message.get("time", time.time()),
                "bid": Decimal(str(message.get("bid", "0"))),
                "ask": Decimal(str(message.get("ask", "0"))),
                "last_price": Decimal(str(message.get("last", "0"))),
                "volume": Decimal(str(message.get("volume24h", "0"))),
                "high": Decimal(str(message.get("high24h", "0"))),
                "low": Decimal(str(message.get("low24h", "0"))),
                "funding_rate": Decimal(str(message.get("fundingRate", "0"))),
                "mark_price": Decimal(str(message.get("markPrice", "0"))),
                "index_price": Decimal(str(message.get("index", "0"))),
            })
        except Exception as e:
            self.logger().error(f"Error processing ticker message: {str(e)}", exc_info=True)

    async def _process_ticker_lite_message(self, message: Dict[str, Any]):
        """Process ticker lite messages (lightweight version with less data)."""
        try:
            # Extract trading pair
            symbol = message.get("product_id")
            if not symbol:
                self.logger().warning(f"No product_id in ticker_lite message: {message}")
                return

            # Convert to internal trading pair format
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)

            # Put message in ticker_lite queue
            await self._message_queue["ticker_lite"].put({
                "trading_pair": trading_pair,
                "timestamp": message.get("time", time.time()),
                "last_price": Decimal(str(message.get("last", "0"))),
                "volume": Decimal(str(message.get("volume24h", "0"))),
            })
        except Exception as e:
            self.logger().error(f"Error processing ticker lite message: {str(e)}", exc_info=True)

    async def _process_heartbeat_message(self, message: Dict[str, Any]):
        """Process heartbeat messages."""
        try:
            await self._message_queue["heartbeat"].put({
                "timestamp": message.get("time", time.time()),
                "message": "heartbeat"
            })
        except Exception as e:
            self.logger().error(f"Error processing heartbeat message: {str(e)}", exc_info=True)

    def _convert_snapshot_message_to_order_book_row(
        self,
        snapshot: Dict[str, Any],
        trading_pair: str,
        sequence_number: int,
        timestamp: float
    ) -> OrderBookMessage:
        """Convert snapshot message to order book row."""
        self.logger().debug(f"\n=== Converting snapshot message to order book row ===")
        self.logger().debug(f"Input snapshot: {snapshot}")
        self.logger().debug(f"Trading pair: {trading_pair}")
        self.logger().debug(f"Sequence number: {sequence_number}")
        self.logger().debug(f"Timestamp: {timestamp}")

        bids = []
        asks = []

        # Process bids and asks
        for bid_price, bid_size in snapshot.get("bids", []):
            bids.append([Decimal(str(bid_price)), Decimal(str(bid_size))])
        for ask_price, ask_size in snapshot.get("asks", []):
            asks.append([Decimal(str(ask_price)), Decimal(str(ask_size))])

        self.logger().debug(f"Processed bids: {bids}")
        self.logger().debug(f"Processed asks: {asks}")

        message = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": sequence_number,
                "bids": bids,
                "asks": asks,
            },
            timestamp=timestamp
        )
        self.logger().debug(f"Created OrderBookMessage: {message}")
        return message

    def _convert_diff_message_to_order_book_row(
        self,
        message: Dict[str, Any],
        timestamp: float
    ) -> OrderBookMessage:
        """Convert diff message to order book row."""
        self.logger().debug(f"\n=== Converting diff message to order book row ===")
        self.logger().debug(f"Input message: {message}")
        self.logger().debug(f"Timestamp: {timestamp}")

        exchange_trading_pair = message["product_id"]
        trading_pair = utils.convert_from_exchange_trading_pair(exchange_trading_pair)
        self.logger().debug(f"Converted trading pair: {trading_pair}")

        sequence_number = message.get("seq", int(timestamp))
        self.logger().debug(f"Sequence number: {sequence_number}")

        # Determine if this is a bid or ask
        is_bid = message["side"].lower() == "buy"
        self.logger().debug(f"Is bid: {is_bid}")

        # Create the order book row
        order_dict = {
            "trading_pair": trading_pair,
            "update_id": sequence_number,
            "bids": [[Decimal(str(message["price"])), Decimal(str(message["qty"]))]] if is_bid else [],
            "asks": [] if is_bid else [[Decimal(str(message["price"])), Decimal(str(message["qty"]))]]
        }
        self.logger().debug(f"Created order dict: {order_dict}")

        message = OrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=order_dict,
            timestamp=timestamp
        )
        self.logger().debug(f"Created OrderBookMessage: {message}")
        return message

    def _convert_trade_message_to_order_book_row(self, message: Dict[str, Any]) -> OrderBookMessage:
        """Convert raw trade message to OrderBookMessage."""
        try:
            # Get timestamp, handling both WebSocket and REST formats
            timestamp = message.get("timestamp", message.get("time", int(time.time() * 1e3)))
            if isinstance(timestamp, str):
                try:
                    # Try to parse ISO format timestamp
                    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                    timestamp = int(dt.timestamp() * 1e3)  # Convert to milliseconds
                except ValueError:
                    # If parsing fails, use current time
                    timestamp = int(time.time() * 1e3)

            # Get trading pair
            product_id = message.get("product_id", message.get("instrument", ""))
            if not product_id:
                raise ValueError("No product_id or instrument in trade message")

            # Convert trading pair if needed
            trading_pair = utils.convert_from_exchange_trading_pair(product_id)

            # Determine trade type
            side = message.get("side", "").lower()
            if not side:
                # Try alternate field names
                if message.get("buy") is True:
                    side = "buy"
                elif message.get("buy") is False:
                    side = "sell"
                else:
                    raise ValueError("No side information in trade message")

            trade_type = float(1.0) if side == "buy" else float(2.0)

            # Get price and quantity
            price = Decimal(str(message.get("price", "0")))
            amount = Decimal(str(message.get("qty", message.get("size", "0"))))

            # Get trade ID and sequence number
            trade_id = message.get("seq", int(timestamp))
            sequence = message.get("seq", int(timestamp))

            return OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_type": trade_type,
                    "trade_id": trade_id,
                    "update_id": sequence,
                    "price": price,
                    "amount": amount,
                    "timestamp": timestamp
                },
                timestamp=timestamp * 1e-3  # Convert milliseconds to seconds
            )

        except Exception as e:
            self.logger().error(f"Error converting trade message: {str(e)}", exc_info=True)
            raise

    def _parse_funding_info_message(self, message: Dict[str, Any]) -> FundingInfoUpdate:
        """Parse funding info message from Kraken's API."""
        if "tickers" in message:  # REST API format
            ticker = next((t for t in message["tickers"] if t["symbol"] == self._trading_pairs[0]), None)
            if not ticker:
                raise ValueError(f"No funding info found for {self._trading_pairs[0]}")

            trading_pair = utils.convert_from_exchange_trading_pair(ticker["symbol"])
            return FundingInfoUpdate(
                trading_pair=trading_pair,
                index_price=Decimal(str(ticker.get("indexPrice", "0"))),
                mark_price=Decimal(str(ticker.get("markPrice", "0"))),
                rate=Decimal(str(ticker.get("fundingRate", "0"))),
                next_funding_utc_timestamp=int(message.get("serverTime", utils.get_next_funding_timestamp(time.time())) * 1000),  # Convert to milliseconds
            )
        else:  # WebSocket format
            if "product_id" not in message:
                raise ValueError("No product_id in funding info message")
            trading_pair = utils.convert_from_exchange_trading_pair(message["product_id"])
            next_funding_time = int(message.get("next_funding_rate_time", utils.get_next_funding_timestamp(time.time()) * 1000))  # Convert to milliseconds
            return FundingInfoUpdate(
                trading_pair=trading_pair,
                index_price=Decimal(str(message.get("index", "0"))),
                mark_price=Decimal(str(message.get("markPrice", "0"))),
                rate=Decimal(str(message.get("funding_rate", "0"))),
                next_funding_utc_timestamp=next_funding_time,
            )

    async def listen_for_funding_info(self, output: asyncio.Queue):
        """Listen for funding info messages."""
        while True:
            try:
                message = await self._message_queue[self._funding_info_messages_queue_key].get()
                if message.get("feed") != CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC:
                    continue
                try:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(message["product_id"])
                    funding_info = FundingInfoUpdate(
                        trading_pair=trading_pair,
                        index_price=Decimal(str(message["index"])),
                        mark_price=Decimal(str(message.get("markPrice", "0"))),
                        rate=Decimal(str(message.get("funding_rate", "0"))),
                        next_funding_utc_timestamp=int(message.get("next_funding_rate_time", 0)),
                    )
                    await output.put(funding_info)
                except (KeyError, ValueError, TypeError) as e:
                    self.logger().error(
                        "Unexpected error when processing public funding info updates from exchange",
                        exc_info=True
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(
                    "Unexpected error when processing public funding info updates from exchange",
                    exc_info=True
                )
                await self._sleep(5.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Listen for order book diffs."""
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                message = await message_queue.get()
                feed = message.get("feed")
                
                self.logger().debug(f"\n=== Order Book Diff Listener ===\n"
                                  f"Received message from feed: {feed}\n"
                                  f"Message: {message}")
                
                if feed in ["book", "book_snapshot"]:
                    # Extract message details for logging
                    side = message.get("side", "unknown")
                    price = message.get("price", "unknown")
                    qty = message.get("qty", "unknown")
                    seq = message.get("seq", "unknown")
                    product_id = message.get("product_id", "unknown")
                    
                    self.logger().debug(f"Processing order book update:"
                                      f"\n  Product: {product_id}"
                                      f"\n  Side: {side}"
                                      f"\n  Price: {price}"
                                      f"\n  Quantity: {qty}"
                                      f"\n  Sequence: {seq}"
                                      f"\n  Action: {'Remove' if float(qty or 0) == 0 else 'Update'}")
                    
                    try:
                        # Process the message and create an OrderBookMessage
                        order_msg = await self._process_order_book_message(message)
                        
                        # Queue the message for the order book tracker
                        await output.put(order_msg)
                        
                        self.logger().debug(f"Successfully processed and queued order book update:"
                                          f"\n  Message Type: {order_msg.type}"
                                          f"\n  Trading Pair: {order_msg.trading_pair}"
                                          f"\n  Update ID: {order_msg.update_id}")
                        
                    except Exception as e:
                        self.logger().error(f"Error processing order book update: {str(e)}", exc_info=True)
                else:
                    self.logger().debug(f"Ignoring message with feed: {feed}")
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in order book listener: {str(e)}", exc_info=True)
                await self._sleep(5.0)

    def _get_bids_and_asks_from_ws_msg_data(
        self,
        message: Dict[str, Any]
    ) -> Tuple[List[List[float]], List[List[float]]]:
        """Extract bids and asks from WebSocket message."""
        bids = []
        asks = []
        
        self.logger().debug(f"\n=== Processing Bids and Asks ===\n"
                           f"Message: {message}")
        
        if "bids" in message:
            for bid in message["bids"]:
                price = float(bid["price"])
                qty = float(bid["qty"])
                bids.append([price, qty])
                self.logger().debug(f"Added bid: Price={price}, Quantity={qty}")
        
        if "asks" in message:
            for ask in message["asks"]:
                price = float(ask["price"])
                qty = float(ask["qty"])
                asks.append([price, qty])
                self.logger().debug(f"Added ask: Price={price}, Quantity={qty}")
        
        self.logger().debug(f"Processed bids and asks:"
                           f"\n  Number of bids: {len(bids)}"
                           f"\n  Number of asks: {len(asks)}"
                           f"\n  First bid: {bids[0] if bids else 'None'}"
                           f"\n  First ask: {asks[0] if asks else 'None'}")
        
        return bids, asks

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse order book diff message and put it to the queue."""
        try:
            if "timestamp" not in raw_message:
                raise ValueError("Incomplete message - missing timestamp")

            exchange_trading_pair = raw_message["product_id"]  # Get exchange format
            timestamp = raw_message.get("timestamp", int(time.time()))
            update_id = raw_message.get("seq", timestamp)

            # Handle snapshot message
            if (raw_message.get("feed") == CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC or raw_message.get("feed") == "book_snapshot") and "bids" in raw_message and "asks" in raw_message:
                self.logger().info(f"Processing full order book for {exchange_trading_pair}")
                bids, asks = self._get_bids_and_asks_from_ws_msg_data(raw_message)
                self.logger().info(f"Order book contains {len(bids)} bids and {len(asks)} asks")
                
                order_book_message_content = {
                    "trading_pair": exchange_trading_pair,  # Keep exchange format for snapshots
                    "update_id": update_id,
                    "bids": bids,
                    "asks": asks
                }
                message = OrderBookMessage(
                    message_type=OrderBookMessageType.SNAPSHOT,
                    content=order_book_message_content,
                    timestamp=timestamp
                )
                # Update sequence number and put message in queue
                self._last_sequence_numbers[exchange_trading_pair] = update_id
                await message_queue.put(message)
                self.logger().info(f"✓ Processed full order book for {exchange_trading_pair} - Sequence: {update_id}")
                return

            # Check if mapping is ready using the event
            if not self._mapping_initialized.is_set():
                self.logger().debug(f"Trading pair mapping not ready yet, queuing message for {exchange_trading_pair}")
                return

            # Try to get trading pair from connector mapping
            try:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(exchange_trading_pair)
                self.logger().debug(f"Successfully converted {exchange_trading_pair} to {trading_pair}")
            except KeyError:
                self.logger().warning(f"Trading pair mapping not found for {exchange_trading_pair}, skipping message")
                return
            except Exception as e:
                self.logger().error(f"Error converting trading pair {exchange_trading_pair}: {str(e)}")
                return

            # Skip out-of-order messages for diffs only
            if trading_pair in self._last_sequence_numbers:
                last_seq = self._last_sequence_numbers[trading_pair]
                if update_id <= last_seq:
                    self.logger().info(f"Skipping stale delta for {trading_pair} - Last seq: {last_seq}, Current seq: {update_id}")
                    return
                elif update_id != last_seq + 1:
                    self.logger().warning(f"Gap detected for {trading_pair}: expected {last_seq+1} but got {update_id}. Triggering resync.")
                    # Trigger resync: fetch a new snapshot and update the sequence number
                    snapshot_msg = await self._order_book_snapshot(trading_pair)
                    await message_queue.put(snapshot_msg)
                    self._last_sequence_numbers[trading_pair] = snapshot_msg.content.get('update_id', update_id)
                    return

            # Handle diff message
            bids = []
            asks = []
            side = raw_message.get("side", "unknown")
            price = raw_message.get("price", "unknown")
            qty = raw_message.get("qty", "unknown")
            
            if side == "buy":
                bids.append([float(price), float(qty)])
                self.logger().info(f"Processing BID delta for {trading_pair} - Price: {price}, Qty: {qty}")
            elif side == "sell":
                asks.append([float(price), float(qty)])
                self.logger().info(f"Processing ASK delta for {trading_pair} - Price: {price}, Qty: {qty}")

            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": bids,
                "asks": asks
            }
            message = OrderBookMessage(
                message_type=OrderBookMessageType.DIFF,
                content=order_book_message_content,
                timestamp=timestamp
            )

            # Update sequence number and put message in queue
            self._last_sequence_numbers[trading_pair] = update_id
            await message_queue.put(message)
            self.logger().info(f"✓ Processed {side.upper()} delta for {trading_pair} - Sequence: {update_id}")
        except Exception as e:
            self.logger().error(
                f"Error processing order book message: {raw_message}. Error: {str(e)}",
                exc_info=True
            )

    async def _parse_trade_message(self, message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse trade message and put it in the queue."""
        if message.get("feed") != CONSTANTS.WS_TRADES_TOPIC:
            return

        exchange_trading_pair = message["product_id"]  # Get exchange format
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(exchange_trading_pair)  # Convert to connector format
        timestamp = message["timestamp"] * 1e-3  # Convert to seconds
        price = Decimal(str(message["price"]))
        amount = Decimal(str(message["qty"]))
        trade_type = TradeType.BUY if message.get("side", "").lower() == "buy" else TradeType.SELL

        trade_msg = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,  # Using connector format
                "trade_type": trade_type,
                "trade_id": message.get("seq", int(timestamp * 1000)),
                "update_id": message.get("seq", int(timestamp * 1000)),
                "price": price,
                "amount": amount,
                "timestamp": timestamp
            },
            timestamp=timestamp
        )

        await message_queue.put(trade_msg)

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses a funding info message from the WebSocket stream and puts it into the message queue.
        :param raw_message: The raw message from the WebSocket
        :param message_queue: The queue to put the parsed message into
        """
        if raw_message["feed"] == CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC:
            try:
                # The product_id field contains the symbol
                if "product_id" not in raw_message:
                    self.logger().debug(f"No product_id in message: {raw_message}")
                    return
                
                symbol = raw_message["product_id"]
                
                # Check if mapping is initialized
                if not self._mapping_initialized.is_set():
                    self.logger().debug(f"Trading pair mapping not ready yet, skipping funding info for {symbol}")
                    return
                
                try:
                    # Use the connector's trading pair mapping
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
                except KeyError:
                    self.logger().debug(f"No trading pair mapping found for {symbol}, waiting for mapping to be ready")
                    return
                
                if not trading_pair:
                    self.logger().debug(f"No trading pair mapping found for {symbol}")
                    return
                
                self.logger().debug(f"Processing funding info for {symbol} -> {trading_pair}")
                
                # Extract required fields with proper error handling
                try:
                    info_update = FundingInfoUpdate(
                        trading_pair=trading_pair,
                        index_price=Decimal(str(raw_message.get("markPrice", "0"))),
                        mark_price=Decimal(str(raw_message.get("markPrice", "0"))),
                        next_funding_utc_timestamp=int(raw_message.get("next_funding_rate_time", time.time())),
                        rate=Decimal(str(raw_message.get("fundingRate", "0"))))
                    await message_queue.put(info_update)
                except (KeyError, ValueError) as e:
                    self.logger().debug(f"Error extracting funding info fields: {e} from message: {raw_message}")
            except Exception as e:
                self.logger().debug(f"Error processing funding info message for {raw_message.get('product_id', 'unknown symbol')}: {str(e)}")
                if self.logger().getEffectiveLevel() <= logging.DEBUG:
                    self.logger().debug("Full traceback:", exc_info=True)

    def _order_book_row_for_processing(self, message: OrderBookMessage) -> OrderBookMessage:
        """Convert OrderBookMessage to the format expected by the order book tracker."""
        return message

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Get order book snapshot for a trading pair.
        :param trading_pair: The trading pair for which to get the snapshot
        :return: OrderBookMessage containing the snapshot data
        """
        try:
            # self.logger().info(f"\n=== Getting order book snapshot for {trading_pair} ===")
            snapshot_response = await self._request_order_book_snapshot(trading_pair)
            # self.logger().info(f"Snapshot response: {snapshot_response}")

            if snapshot_response.get("result") != "success":
                raise IOError(f"Error getting order book snapshot: {snapshot_response.get('error', 'Unknown error')}")

            # Extract order book data
            order_book = snapshot_response.get("orderBook", {})
            server_time_str = snapshot_response.get("serverTime", "")
            
            try:
                # Parse ISO 8601 timestamp string
                timestamp = datetime.strptime(server_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()
            except (ValueError, TypeError):
                self.logger().warning(f"Could not parse timestamp {server_time_str}, using current time")
                timestamp = time.time()

            # Convert timestamp to milliseconds
            timestamp_ms = int(timestamp * 1e3)

            # Process bids and asks
            bids = []
            asks = []
            
            for bid in order_book.get("bids", []):
                bids.append([Decimal(str(bid[0])), Decimal(str(bid[1]))])
            for ask in order_book.get("asks", []):
                asks.append([Decimal(str(ask[0])), Decimal(str(ask[1]))])

            snapshot_msg = OrderBookMessage(
                message_type=OrderBookMessageType.SNAPSHOT,
                content={
                    "trading_pair": trading_pair,
                    "update_id": timestamp_ms,
                    "bids": bids,
                    "asks": asks,
                },
                timestamp=timestamp
            )
            
            # self.logger().info(f"Created order book snapshot message: {snapshot_msg}")
            return snapshot_msg

        except Exception as e:
            self.logger().error(f"Error getting order book snapshot for {trading_pair}: {str(e)}", exc_info=True)
            raise

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Request order book snapshot from REST API.
        :param trading_pair: The trading pair for which to get the snapshot
        :return: The response from the API
        """
        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        
        rest_assistant = await self._api_factory.get_rest_assistant()
        endpoint = CONSTANTS.ORDER_BOOK_ENDPOINT
        url = web_utils.get_rest_url_for_endpoint(endpoint=endpoint, domain=self._domain)
        
        params = {
            "symbol": exchange_symbol,
        }
        
        data = await rest_assistant.execute_request(
            url=url,
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=web_utils.GET_LIMIT_ID,
        )
        
        return data

    def _get_bids_and_asks_from_rest_msg_data(
        self,
        snapshot: Dict[str, Any]
    ) -> Tuple[List[List[float]], List[List[float]]]:
        """Extract bids and asks from REST snapshot message."""
        bids = []
        asks = []
        for bid in snapshot.get("bids", []):
            bids.append([float(bid[0]), float(bid[1])])
        for ask in snapshot.get("asks", []):
            asks.append([float(ask[0]), float(ask[1])])
        return bids, asks


    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Listen for trades."""
        message_queue = self._message_queue[self._trade_messages_queue_key]
        while True:
            try:
                message = await message_queue.get()

                # If message is already an OrderBookMessage, put it directly in the output queue
                if isinstance(message, OrderBookMessage):
                    await output.put(message)
                    continue

                # Handle string messages by attempting to parse them as JSON
                if isinstance(message, str):
                    try:
                        message = json.loads(message)
                    except json.JSONDecodeError:
                        self.logger().error(f"Failed to parse trade message as JSON: {message}")
                        continue

                # Validate message is a dict
                if not isinstance(message, dict):
                    self.logger().error(f"Received non-dict message: {message}")
                    continue

                # Skip subscription confirmation messages
                if message.get("event") == "subscribed":
                    continue

                # Validate feed type
                feed = message.get("feed")
                if not feed:
                    self.logger().error(f"Message missing feed field: {message}")
                    continue
                if feed != CONSTANTS.WS_TRADES_TOPIC:
                    continue  # Skip non-trade messages

                # Validate required fields
                required_fields = ["product_id", "price", "qty", "side", "seq"]
                missing_fields = [field for field in required_fields if field not in message]
                if missing_fields:
                    self.logger().error(f"Message missing required fields {missing_fields}: {message}")
                    continue

                # Convert message to order book row format
                msg = self._convert_trade_message_to_order_book_row(message)
                await output.put(msg)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                                self.logger().error(
                    f"Unexpected error when processing public trade updates from exchange: {str(e)}",
                    exc_info=True
                )
                                self.logger().error(f"Unexpected error in order book listener: {str(e)}", exc_info=True)
                                await self._sleep(5.0)