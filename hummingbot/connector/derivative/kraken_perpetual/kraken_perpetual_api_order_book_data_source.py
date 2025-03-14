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
        
        # Add diagnostic tracking
        self._message_processing_stats = {
            "trades_received": 0,
            "trades_processed": 0,
            "trades_failed": 0,
            "order_book_updates": 0,
            "order_book_errors": 0,
        }
        self._last_diagnostic_time = time.time()
        
        # Existing initialization
        self._funding_info = {}
        self._funding_info_event = asyncio.Event()
        self._mapping_initialized = asyncio.Event()
        self._funding_info_initialized = asyncio.Event()
        self._check_funding_info_initialized_task = asyncio.create_task(self._initialize_funding_info())

        # # Initialize message queues for different feed types
        self._message_queue = {
            self._snapshot_messages_queue_key: asyncio.Queue(),
            self._diff_messages_queue_key: asyncio.Queue(),
            self._trade_messages_queue_key: asyncio.Queue(),
            self._funding_info_messages_queue_key: asyncio.Queue(),
            "ticker": asyncio.Queue(),
            "ticker_lite": asyncio.Queue(),
            "heartbeat": asyncio.Queue(),
        }
        #
        # # Initialize order book queues for each trading pair
        self._order_book_snapshot_queues = defaultdict(asyncio.Queue)
        self._order_book_diff_queues = defaultdict(asyncio.Queue)

        # self.logger().info("Initializing Kraken Perpetual order book data source...")

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> Optional[FundingInfo]:
        """Get funding information for a specific trading pair."""
        try:
            exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
            rest_assistant = await self._api_factory.get_rest_assistant()
            endpoint = CONSTANTS.HISTORICAL_FUNDING_RATES_ENDPOINT
            params = {"symbol": exchange_symbol}

            # First get historical funding rates
            resp = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(endpoint=endpoint, domain=self._domain),
                method=RESTMethod.GET,
                params=params,
                throttler_limit_id=web_utils.PUBLIC_LIMIT_ID,
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

                    # Get mark price from ticker endpoint
                    ticker_endpoint = CONSTANTS.TICKER_SYMBOL_ENDPOINT.format(symbol=exchange_symbol)
                    ticker_resp = await rest_assistant.execute_request(
                        url=web_utils.public_rest_url(endpoint=ticker_endpoint, domain=self._domain),
                        method=RESTMethod.GET,
                        throttler_limit_id=web_utils.PUBLIC_LIMIT_ID,
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
        try:
            await self._mapping_initialized.wait()
            
            for trading_pair in self._trading_pairs:
                try:
                    funding_info = await self.get_funding_info(trading_pair)
                    if funding_info:
                        self._funding_info[trading_pair] = funding_info
                except Exception as e:
                    self.logger().error(f"Error getting funding info for {trading_pair}: {str(e)}", exc_info=True)
            
            if self._funding_info:
                self._funding_info_initialized.set()
            
        except Exception as e:
            self.logger().error(f"Error in funding info initialization: {str(e)}", exc_info=True)
            raise

    async def listen_for_subscriptions(self):
        """Subscribe to the order book, trade, and funding info channels."""
        try:
            ws = await self._api_factory.get_ws_assistant()
            await ws.connect(ws_url=self._ws_url)

            await self._subscribe_to_channels(ws, self._trading_pairs)

            # Track subscription states
            subscribed_feeds = set()
            expected_feeds = set(self._subscriptions)

            # Process incoming messages
            async for msg in ws.iter_messages():
                try:
                    if isinstance(msg.data, bytes):
                        msg_data = msg.data.decode('utf-8')
                    else:
                        msg_data = msg.data
                        
                    msg_json = json.loads(msg_data) if isinstance(msg_data, str) else msg_data
                    
                    # Handle subscription confirmations
                    if msg_json.get("event") == "subscribed":
                        feed = msg_json.get("feed")
                        subscribed_feeds.add(feed)
                        continue
                    elif msg_json.get("event") == "error":
                        continue
                    
                    await self._process_message(msg_json, self._message_queue)
                    
                except Exception as e:
                    self.logger().error(
                        f"Error processing message: {str(e)}",
                        exc_info=True
                    )
                    
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Unexpected error in WebSocket subscription loop: {str(e)}",
                exc_info=True
            )
            await self._sleep(5.0)

    async def _subscribe_to_channels(self, ws: WSAssistant, trading_pairs: List[str]):
        """Subscribe to all public WebSocket channels."""
        try:
            # Convert trading pairs to exchange symbols
            symbols = [await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                      for trading_pair in trading_pairs]
            
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
                    
                    subscribe_request = WSJSONRequest(payload=payload, is_auth_required=False)
                    await ws.send(subscribe_request)
                except Exception as e:
                    self.logger().error(f"Error subscribing to {feed}: {str(e)}", exc_info=True)
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Error subscribing to channels: {str(e)}",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.get_ws_url(self._domain),
            message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE
        )
        return ws

    async def _wait_for_mapping_ready(self, timeout: float = 30) -> bool:
        """
        Wait for the trading pair mapping to be initialized.
        :param timeout: The maximum time to wait in seconds
        :return: True if mapping is ready, False if timeout occurred
        """
        try:
            is_ready = await asyncio.wait_for(self._mapping_initialized.wait(), timeout=timeout)
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
                message_queue.put_nowait(data)

    async def _handle_subscription_message(self, message: Dict[str, Any]):
        """Handle subscription confirmation and error messages."""
        event = message.get("event", "")
        feed = message.get("feed", "")

        if event == "subscribed":
            self.logger().info(f"Successfully subscribed to {feed} feed")
        elif event == "error":
            self.logger().error(f"Error in {feed} subscription: {message.get('message', '')}")

    async def _process_message(self, msg: Dict[str, Any], message_queue: asyncio.Queue):
        """Process received WebSocket messages and routes them to appropriate queues."""
        try:
            feed = msg.get("feed")
            
            if feed in ["book", "book_snapshot"]:
                self._message_processing_stats["order_book_updates"] += 1
                self._message_queue[self._diff_messages_queue_key].put_nowait(msg)
            
            elif feed == CONSTANTS.WS_TRADES_TOPIC:
                try:
                    self._message_processing_stats["trades_received"] += 1
                    
                    # Basic validation
                    required_fields = ["product_id", "price", "qty", "side", "seq", "time"]
                    if not all(field in msg for field in required_fields):
                        missing = [f for f in required_fields if f not in msg]
                        self.logger().warning(f"Trade message missing fields {missing}: {msg}")
                        self._message_processing_stats["trades_failed"] += 1
                        return

                    # Convert trading pair
                    exchange_trading_pair = msg["product_id"]
                    trading_pair = utils.convert_from_exchange_trading_pair(exchange_trading_pair)
                    
                    # Process trade data
                    timestamp = msg["time"] * 1e-3  # Convert to seconds from Kraken's millisecond timestamp
                    price = Decimal(str(msg["price"]))
                    amount = Decimal(str(msg["qty"]))
                    trade_type = TradeType.BUY if msg.get("side", "").lower() == "buy" else TradeType.SELL
                    sequence = msg.get("seq", int(timestamp * 1000))

                    trade_msg = OrderBookMessage(
                        message_type=OrderBookMessageType.TRADE,
                        content={
                            "trading_pair": trading_pair,
                            "trade_type": trade_type,
                            "trade_id": sequence,
                            "update_id": sequence,
                            "price": price,
                            "amount": amount,
                            "timestamp": timestamp
                        },
                        timestamp=timestamp
                    )

                    self._message_queue[self._trade_messages_queue_key].put_nowait(trade_msg)
                    self._message_processing_stats["trades_processed"] += 1
                    
                except Exception as e:
                    self._message_processing_stats["trades_failed"] += 1
                    self.logger().error(f"Error processing trade message: {str(e)}", exc_info=True)
            
            elif feed == CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC:
                self._message_queue[self._funding_info_messages_queue_key].put_nowait(msg)
            
        except Exception as e:
            if feed in ["book", "book_snapshot"]:
                self._message_processing_stats["order_book_errors"] += 1
            self.logger().error(f"Error processing message: {str(e)}", exc_info=True)
            self.logger().error(f"Message: {msg}")
            raise

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Listen for order book diffs and process them."""
        diff_messages_queue = self._message_queue[self._diff_messages_queue_key]
        
        while True:
            try:
                message = await diff_messages_queue.get()
                
                if "timestamp" not in message:
                    self.logger().error("Missing timestamp in message")
                    continue

                symbol = message.get("product_id", "")
                if not symbol:
                    self.logger().error("Missing product_id in message")
                    continue

                try:
                    trading_pair = utils.convert_from_exchange_trading_pair(symbol)
                except Exception as e:
                    self.logger().error(f"Error converting trading pair {symbol}: {str(e)}")
                    continue

                timestamp = message.get("timestamp", int(time.time() * 1e3))
                update_id = timestamp
                kraken_sequence = message.get("seq")
                
                if kraken_sequence is None:
                    self.logger().error(f"Missing sequence number in message: {message}")
                    continue

                if message.get("feed") == "book_snapshot" or ("bids" in message and "asks" in message):
                    try:
                        bids, asks = self._get_bids_and_asks_from_ws_msg_data(message)
                        
                        snapshot_msg = OrderBookMessage(
                            message_type=OrderBookMessageType.SNAPSHOT,
                            content={
                                "trading_pair": trading_pair,
                                "update_id": update_id,
                                "bids": bids,
                                "asks": asks,
                            },
                            timestamp=timestamp * 1e-3
                        )
                        
                        self._last_sequence_numbers[trading_pair] = kraken_sequence
                        output.put_nowait(snapshot_msg)
                        continue
                    except Exception as e:
                        self.logger().error(f"Error processing snapshot: {str(e)}")
                        continue

                try:
                    if trading_pair in self._last_sequence_numbers:
                        last_seq = self._last_sequence_numbers[trading_pair]
                        if kraken_sequence <= last_seq:
                            continue
                        elif kraken_sequence != last_seq + 1:
                            self.logger().warning(f"Gap detected: expected {last_seq+1} but got {kraken_sequence}. Triggering resync.")
                            snapshot_msg = await self._order_book_snapshot(trading_pair)
                            output.put_nowait(snapshot_msg)
                            self._last_sequence_numbers[trading_pair] = snapshot_msg.content.get('kraken_sequence', kraken_sequence)
                            continue

                    side = message.get("side", "").lower()
                    price = float(message.get("price", 0))
                    quantity = float(message.get("qty", 0))
                    
                    bids = [[price, quantity]] if side == "buy" else []
                    asks = [[price, quantity]] if side == "sell" else []
                    
                    diff_msg = OrderBookMessage(
                        message_type=OrderBookMessageType.DIFF,
                        content={
                            "trading_pair": trading_pair,
                            "update_id": update_id,
                            "bids": bids,
                            "asks": asks,
                        },
                        timestamp=timestamp * 1e-3
                    )
                    
                    self._last_sequence_numbers[trading_pair] = kraken_sequence
                    output.put_nowait(diff_msg)
                    
                except Exception as e:
                    self.logger().error(f"Error processing delta: {str(e)}")
                    continue
                    
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
        
        if "bids" in message:
            for bid in message["bids"]:
                price = float(bid["price"])
                qty = float(bid["qty"])
                bids.append([price, qty])
        
        if "asks" in message:
            for ask in message["asks"]:
                price = float(ask["price"])
                qty = float(ask["qty"])
                asks.append([price, qty])
        
        return bids, asks

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse order book diff message and put it to the queue."""
        try:
            if "timestamp" not in raw_message:
                self.logger().error("Missing timestamp in message")
                raise ValueError("Incomplete message - missing timestamp")

            exchange_trading_pair = raw_message["product_id"]  # Get exchange format
            timestamp = raw_message.get("timestamp", int(time.time()))
            update_id = raw_message.get("seq", timestamp)

            # Handle snapshot message
            if (raw_message.get("feed") == CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC or 
                raw_message.get("feed") == "book_snapshot") and "bids" in raw_message and "asks" in raw_message:
                
                bids, asks = self._get_bids_and_asks_from_ws_msg_data(raw_message)
                
                order_book_message_content = {
                    "trading_pair": exchange_trading_pair,
                    "update_id": update_id,
                    "bids": bids,
                    "asks": asks
                }
                
                message = OrderBookMessage(
                    message_type=OrderBookMessageType.SNAPSHOT,
                    content=order_book_message_content,
                    timestamp=timestamp
                )
                
                message_queue.put_nowait(message)
                return

            # Handle delta message
            try:
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(exchange_trading_pair)
            except KeyError:
                return
            except Exception as e:
                return

            # Skip out-of-order messages for diffs only
            if trading_pair in self._last_sequence_numbers:
                last_seq = self._last_sequence_numbers[trading_pair]
                if update_id <= last_seq:
                    return
                elif update_id != last_seq + 1:
                    snapshot_msg = await self._order_book_snapshot(trading_pair)
                    message_queue.put_nowait(snapshot_msg)
                    self._last_sequence_numbers[trading_pair] = snapshot_msg.content.get('update_id', update_id)
                    return

            # Process delta update
            bids = []
            asks = []
            side = raw_message.get("side", "unknown")
            price = raw_message.get("price", "unknown")
            qty = raw_message.get("qty", "unknown")
            
            if side == "buy":
                bids.append([float(price), float(qty)])
            elif side == "sell":
                asks.append([float(price), float(qty)])

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

            self._last_sequence_numbers[trading_pair] = update_id
            message_queue.put_nowait(message)

        except Exception as e:
            self.logger().error(f"Error processing order book message: {str(e)}", exc_info=True)
            self.logger().error(f"Raw message: {raw_message}")
            raise

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Parses a funding info message from the WebSocket stream and puts it into the message queue.
        :param raw_message: The raw message from the WebSocket
        :param message_queue: The queue to put the parsed message into
        """
        if raw_message["feed"] == CONSTANTS.WS_INSTRUMENTS_INFO_TOPIC:
            try:
                if "product_id" not in raw_message:
                    return
                
                symbol = raw_message["product_id"]
                
                try:
                    trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
                except KeyError:
                    return
                
                if not trading_pair:
                    return

                try:
                    info_update = FundingInfoUpdate(
                        trading_pair=trading_pair,
                        index_price=Decimal(str(raw_message.get("markPrice", "0"))),
                        mark_price=Decimal(str(raw_message.get("markPrice", "0"))),
                        next_funding_utc_timestamp=int(raw_message.get("next_funding_rate_time", time.time())),
                        rate=Decimal(str(raw_message.get("fundingRate", "0"))))
                    message_queue.put_nowait(info_update)
                except (KeyError, ValueError) as e:
                    self.logger().warning(f"Error extracting funding info fields: {e} from message: {raw_message}")
            except Exception as e:
                self.logger().error(f"Error processing funding info message: {str(e)}", exc_info=True)

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
            snapshot_response = await self._request_order_book_snapshot(trading_pair)
            
            if snapshot_response.get("result") != "success":
                raise IOError(f"Error getting order book snapshot: {snapshot_response.get('error', 'Unknown error')}")

            # Extract order book data
            order_book = snapshot_response.get("orderBook", {})
            server_time_str = snapshot_response.get("serverTime", "")
            kraken_sequence = snapshot_response.get("seq")  # Keep track of Kraken's sequence
            
            try:
                # Parse ISO 8601 timestamp string
                timestamp = datetime.strptime(server_time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()
            except (ValueError, TypeError):
                self.logger().warning(f"Could not parse timestamp {server_time_str}, using current time")
                timestamp = time.time()

            # Convert timestamp to milliseconds for update_id
            update_id = int(timestamp * 1e3)

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
                    "update_id": update_id,  # Use timestamp
                    "kraken_sequence": kraken_sequence,  # Store Kraken's sequence for internal tracking
                    "bids": bids,
                    "asks": asks,
                },
                timestamp=timestamp
            )
            
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
            throttler_limit_id=web_utils.PUBLIC_LIMIT_ID,
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
                
                if isinstance(message, OrderBookMessage):
                    output.put_nowait(message)
                else:
                    self.logger().warning(f"Received unexpected message type: {type(message)}")
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in trade listener: {str(e)}", exc_info=True)
                await self._sleep(5.0)