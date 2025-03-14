import asyncio
import json
import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

from bidict import bidict

import hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_utils as kraken_utils
from hummingbot.connector.derivative.kraken_perpetual import kraken_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_api_order_book_data_source import (
    KrakenPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_auth import KrakenPerpetualAuth
from hummingbot.connector.derivative.kraken_perpetual.kraken_perpetual_user_stream_data_source import (
    KrakenPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.event.events import AssignmentFillEvent, MarketEvent

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_decimal_NaN = Decimal("nan")
s_decimal_0 = Decimal(0)


class KrakenPerpetualDerivative(PerpetualDerivativePyBase):

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        kraken_perpetual_api_key: str = None,
        kraken_perpetual_secret_key: str = None,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._logger = logging.getLogger(__name__)
        self.kraken_perpetual_api_key = kraken_perpetual_api_key
        self.kraken_perpetual_secret_key = kraken_perpetual_secret_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._last_trade_history_timestamp = None
        self._total_collateral_value = Decimal("0")  # Add this line to store total collateral value

        # Order history cache related attributes
        self._order_history_cache = {}
        self._last_order_history_fetch_ts = 0
        self._order_history_cache_ttl = 30
        self._order_history_lock = asyncio.Lock()
        self._fetching_order_history = False

        super().__init__(client_config_map)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> KrakenPerpetualAuth:
        return KrakenPerpetualAuth(
            api_key=self.kraken_perpetual_api_key,
            secret_key=self.kraken_perpetual_secret_key,
            time_provider=self._time_provider
        )

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return web_utils.build_rate_limits()

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_BROKER_ID

    @property
    def supported_position_modes(self) -> List[PositionMode]:
        """
        Returns the list of position modes supported by the exchange.
        Kraken Perpetual only supports one-way positions.
        """
        return [PositionMode.ONEWAY]  # Kraken only supports one-way positions

    def set_position_mode(self, position_mode: PositionMode):
        """
        Sets the position mode for the exchange.
        :param position_mode: The position mode to set.
        :raises: ValueError if the position mode is not supported
        """
        if position_mode != PositionMode.ONEWAY:
            raise ValueError(f"Position mode {position_mode} is not supported by {self.name}. Only ONEWAY position mode is supported.")

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        """
        Since Kraken only supports one-way positions, this always returns success for ONEWAY mode.conn
        """
        if mode != PositionMode.ONEWAY:
            return False, "Kraken only supports the ONEWAY position mode."
        return True, ""

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.QUERY_SYMBOL_ENDPOINT

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.LATEST_SYMBOL_INFORMATION_ENDPOINT

    async def _make_trading_pairs_request(self) -> Any:
        """Make a request to get the list of trading pairs from the exchange."""
        try:
            exchange_info_response = await self._api_get(
                path_url=self.trading_pairs_request_path,
                limit_id=web_utils.PUBLIC_LIMIT_ID,
            )

            if isinstance(exchange_info_response, dict):
                if exchange_info_response.get("result") != "success":
                    error_msg = exchange_info_response.get("error", "Unknown error")
                    raise IOError(f"Error fetching trading pairs: {error_msg}")

                # Return the instruments list directly
                return exchange_info_response.get("instruments", [])
            else:
                self._logger.error(f"Unexpected response type: {type(exchange_info_response)}")
                raise IOError("Unexpected response format for trading pairs")
        except Exception as e:
            self._logger.error(f"Error in _make_trading_pairs_request: {str(e)}", exc_info=True)
            raise

    async def _make_trading_rules_request(self) -> Any:
        """Make a request to get the trading rules from the exchange."""
        trading_rules_response = await self._api_get(
            path_url=self.trading_rules_request_path,
            limit_id=web_utils.PUBLIC_LIMIT_ID,
        )

        if isinstance(trading_rules_response, list):
            return trading_rules_response
        elif isinstance(trading_rules_response, dict):
            if trading_rules_response.get("result") != "success":
                error_msg = trading_rules_response.get("error", "Unknown error")
                raise IOError(f"Error fetching trading rules: {error_msg}")
            return trading_rules_response.get("instruments", [])
        else:
            self._logger.error(f"Unexpected response type: {type(trading_rules_response)}")
            raise IOError("Unexpected response format for trading rules")

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.MARKET]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Get the token used for collateral when buying."""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Get the token used for collateral when selling."""
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def start(self, clock: Clock, timestamp: float):
        super().start(clock, timestamp)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        """
        Kraken Perpetual doesn't require time synchronization.
        This method is kept for compatibility with the base class.
        """
        return False

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
        """
        Kraken Perpetual doesn't require time synchronization.
        This method is kept for compatibility with the base class.
        """
        pass

    def _time_provider(self) -> float:
        """
        Simple time provider function that returns current timestamp in milliseconds.
        """
        return time.time() * 1e3

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """
        Determines if an exception was raised because an order was not found during a status update.
        :param status_update_exception: The exception that was raised.
        :return: True if the exception indicates the order was not found, False otherwise.
        """
        error_message = str(status_update_exception)
        if "error" in error_message:
            try:
                error_json = json.loads(error_message)
                return error_json.get("error", "").lower() == "order does not exist"
            except Exception:
                pass
        return "order does not exist" in error_message.lower()

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """
        Determines if an exception was raised because an order was not found during a cancellation request.
        :param cancelation_exception: The exception that was raised.
        :return: True if the exception indicates the order was not found, False otherwise.
        """
        error_message = str(cancelation_exception)
        return "error cancelling order: order not found" in error_message.lower()

    def _get_process_before_timestamp(self, seconds_into_future: int = 3) -> str:
        """
        Helper method to get a properly formatted processBefore timestamp.
        :param seconds_into_future: Number of seconds into the future (default 3)
        :return: Formatted timestamp string in ISO 8601 format with UTC timezone (Z)
        """
        current_time = self._time_provider() / 1e3  # Convert milliseconds to seconds
        future_time = current_time + seconds_into_future
        return datetime.fromtimestamp(future_time).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """Place an order cancellation request."""
        # self.logger().info(f"\n=== Placing Cancel Request ===\nOrder ID: {order_id}\nCurrent State: {tracked_order.current_state}")

        # Extract the actual order ID from the exchange_order_id if it's a JSON string
        exchange_order_id = tracked_order.exchange_order_id
        if exchange_order_id and isinstance(exchange_order_id, str):
            try:
                # If it's a JSON string, try to parse it and extract the order_id
                order_data = json.loads(exchange_order_id)
                if isinstance(order_data, dict):
                    if "sendStatus" in order_data and "order_id" in order_data["sendStatus"]:
                        exchange_order_id = order_data["sendStatus"]["order_id"]
                    elif "orderEvents" in order_data and order_data["orderEvents"] and "order" in order_data["orderEvents"][0]:
                        exchange_order_id = order_data["orderEvents"][0]["order"]["orderId"]
            except json.JSONDecodeError:
                # If it's not JSON, use it as is
                pass

        data = {
            "processBefore": str(self._get_process_before_timestamp()),
            "orderId": exchange_order_id,
            "cliOrdId": tracked_order.client_order_id,
        }

        self.logger().info(f"Cancel request data: {data}")

        cancel_result = await self._api_post(
            path_url=CONSTANTS.CANCEL_ACTIVE_ORDER_PATH_URL,
            data=data,
            is_auth_required=True,
            trading_pair=tracked_order.trading_pair,
        )

        self.logger().info(f"Cancel result: {cancel_result}")

        if cancel_result["result"] != "success":
            error_msg = cancel_result.get("error", "Unknown error")
            if "order not found" in error_msg.lower():
                self.logger().info(f"Order not found during cancellation - Order ID: {order_id}")
                await self._order_tracker.process_order_not_found(tracked_order.client_order_id)
                self.logger().info(f"Order state after not found:"
                                 f"\n  - State: {tracked_order.current_state}"
                                 f"\n  - Is Done: {tracked_order.is_done}"
                                 f"\n  - Is Cancelled: {tracked_order.is_cancelled}")
                return True
            self.logger().warning(f"Failed to cancel order {order_id} ({error_msg})")
            raise IOError(f"Error cancelling order: {error_msg}")

        # Process the cancellation response
        cancel_status = cancel_result.get("cancelStatus", {})
        if cancel_status.get("status") == "cancelled":
            # Create an order event message to process
            order_data = cancel_status["orderEvents"][0]["order"]
            order_data["status"] = "CANCELED"  # Make sure this matches your ORDER_STATE mapping
            order_event = {
                "order": order_data
            }
            self.logger().debug(f"Processing cancellation event: {order_event}")
            self._process_order_event_message(order_event)

        # self.logger().info(f"Cancellation request sent successfully for order {order_id}")
        return True

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:

        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

        data = {
            "processBefore": str(self._get_process_before_timestamp()),
            "orderType": CONSTANTS.ORDER_TYPE_MAP[order_type].lower(),
            "symbol": exchange_symbol,
            "side": trade_type.name.lower(),
            "size": float(amount),
            "cliOrdId": order_id,
            "reduceOnly": "true" if position_action == PositionAction.CLOSE else "false",
        }

        if order_type.is_limit_type():
            data["limitPrice"] = float(price)

        order_result = await self._api_post(
            path_url=CONSTANTS.PLACE_ACTIVE_ORDER_PATH_URL,
            data=data,
            is_auth_required=True,
            headers={"referer": CONSTANTS.HBOT_BROKER_ID},
            **kwargs,
        )

        # Add detailed debugging of the API response
        self.logger().info(f"Kraken API response for {order_type.name} order: {order_result}")

        if order_result.get("result") != "success":
            error_msg = order_result.get("error", "Unknown error")
            raise IOError(f"Error placing order: {error_msg}")

        # Extract the actual order ID from the response
        exchange_order_id = None
        if "sendStatus" in order_result and "order_id" in order_result["sendStatus"]:
            exchange_order_id = order_result["sendStatus"]["order_id"]
        elif "sendStatus" in order_result and "orderEvents" in order_result["sendStatus"]:
            order_events = order_result["sendStatus"]["orderEvents"]
            self.logger().info(f"Order events in response: {order_events}")
            if order_events and len(order_events) > 0:
                if "order" in order_events[0]:
                    exchange_order_id = order_events[0]["order"]["orderId"]
                # Also check for other possible formats
                elif "orderPriorExecution" in order_events[0]:
                    exchange_order_id = order_events[0]["orderPriorExecution"]["orderId"]
                    self.logger().info(f"Found order ID in orderPriorExecution: {exchange_order_id}")
                elif "executionId" in order_events[0]:
                    exchange_order_id = order_events[0]["executionId"]
                    self.logger().info(f"Using executionId as order ID: {exchange_order_id}")

        # Log the complete response for market orders to help debug
        if order_type == OrderType.MARKET:
            self.logger().info(f"MARKET ORDER RESPONSE DEBUG - Full response: {order_result}")
            self.logger().info(f"MARKET ORDER RESPONSE DEBUG - Order ID extracted: {exchange_order_id}")
            self.logger().info(f"MARKET ORDER RESPONSE DEBUG - Order parameters: {data}")

        if not exchange_order_id:
            # If we still can't find an order ID, try to use any ID in the response as fallback
            if "sendStatus" in order_result:
                if "status" in order_result["sendStatus"] and order_result["sendStatus"]["status"] == "placed":
                    # For successful orders that don't have a clear ID, generate one based on timestamp
                    fallback_id = f"fallback-{order_id}-{self.current_timestamp}"
                    self.logger().warning(f"Could not extract order ID from response, using fallback ID: {fallback_id}")
                    return fallback_id, self.current_timestamp
            
            # If all else fails, raise the exception
            raise IOError("Could not extract order ID from response")

        return str(exchange_order_id), self.current_timestamp

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None,
                 position_action: PositionAction = None) -> TradeFeeBase:
        is_maker = is_maker or False
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        pass

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return KrakenPerpetualAPIOrderBookDataSource(
            self.trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return KrakenPerpetualUserStreamDataSource(
            auth=self._auth,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_trade_history(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_trade_history(self):
        """Calls REST API to get trade history (order fills)"""
        trade_history_tasks = []

        for trading_pair in self._trading_pairs:
            exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
            body_params = {
            }
            if self._last_trade_history_timestamp:
                dt = datetime.strptime(self._last_trade_history_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                timestamp_ms = int(dt.timestamp() * 1000)
                body_params["startTime"] = timestamp_ms

            trade_history_tasks.append(
                asyncio.create_task(self._api_get(
                    path_url=CONSTANTS.USER_TRADE_RECORDS_PATH_URL,
                    params=body_params,
                    is_auth_required=True
                )))

        raw_responses: List[Dict[str, Any]] = await safe_gather(*trade_history_tasks, return_exceptions=True)

        # Initial parsing of responses. Joining all the responses
        parsed_history_resps: List[Dict[str, Any]] = []
        for trading_pair, resp in zip(self._trading_pairs, raw_responses):
            if not isinstance(resp, Exception):
                self._last_trade_history_timestamp = resp.get("fillTime")

                trade_entries = resp["fills"] if "fills" in resp else []
                if trade_entries:
                    parsed_history_resps.extend(trade_entries)
            else:
                self.logger().network(
                    f"Error fetching status update for {trading_pair}: {resp}.",
                    app_warning_msg=f"Failed to fetch status update for {trading_pair}."
                )

        # Trade updates must be handled before any order status updates.
        for trade in parsed_history_resps:
            self._process_trade_event_message(trade)

    async def _update_order_status(self):
        """
        Calls REST API to get order status
        """
        # self.logger().info("\n=== Updating Order Status via REST ===")
        active_orders: List[InFlightOrder] = list(self.in_flight_orders.values())
        # self.logger().info(f"Found {len(active_orders)} active orders to update")

        tasks = []
        for active_order in active_orders:
            # self.logger().info(f"\nCreating status check task for order:"
            #                  f"\n  Client order ID: {active_order.client_order_id}"
            #                  f"\n  Exchange order ID: {active_order.exchange_order_id}"
            #                  f"\n  Trading pair: {active_order.trading_pair}"
            #                  f"\n  Current state: {active_order.current_state}")
            tasks.append(asyncio.create_task(self._request_order_status_data(tracked_order=active_order)))

        # self.logger().info(f"Awaiting {len(tasks)} order status check tasks...")
        raw_responses: List[Dict[str, Any]] = await safe_gather(*tasks, return_exceptions=True)

        for resp, active_order in zip(raw_responses, active_orders):
            if not isinstance(resp, Exception):
                # self.logger().info(f"\nProcessing response for order {active_order.client_order_id}")
                if resp.get("result") == "success":
                    orders = resp.get("orders", [])
                    if orders:
                        # self.logger().info(f"Order found in active orders - processing update: {orders[0]}")
                        # Order found in active orders - process normally
                        self._process_order_event_message({"order": orders[0]})
                    else:
                        # Order not found in active orders - check history as fallback
                        try:
                            # self.logger().info(f"Order {active_order.client_order_id} not found in active orders - checking history")
                            history_resp = await self._request_order_history_data(active_order)
                            # self.logger().info(f"\n=== Historical Order Response ===\nOrder ID: {active_order.client_order_id}\nResponse: {history_resp}")
                            if history_resp.get("result") == "success":
                                events = history_resp.get("orders", [])
                                # self.logger().info(f"Found {len(events)} events in history for order {active_order.client_order_id}")
                                for event in events:
                                    # self.logger().info(f"Processing historical event: {event}")
                                    # Format the event to match expected structure
                                    formatted_event = {"order": event}
                                    self._process_historical_order_event_message(formatted_event)
                            else:
                                # self.logger().info(f"Order {active_order.client_order_id} not found in history - marking as not found")
                                self.logger().warning(f"History response error: {history_resp.get('error', 'Unknown error')}")
                                await self._order_tracker.process_order_not_found(active_order.client_order_id)
                        except Exception as e:
                            self.logger().error(f"Error checking history for order {active_order.client_order_id}: {str(e)}")
                            # Don't mark as not found if we failed to check history
                else:
                    self.logger().error(f"Error response for {active_order.client_order_id}: {resp.get('error', 'Unknown error')}")
            else:
                self.logger().error(f"Exception checking {active_order.client_order_id}: {str(resp)}")

    async def _update_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        try:
            # For GET requests, parameters should be in the params field
            # Include timestamp in milliseconds as nonce
            nonce = str(int(time.time() * 1000))
            params = {
                "nonce": nonce,
            }

            wallet_balance_response = await self._api_get(
                path_url=CONSTANTS.GET_WALLET_BALANCE_PATH_URL,
                method=RESTMethod.GET,
                params=params,
                is_auth_required=True,
            )

            if wallet_balance_response.get("result") != "success":
                raise IOError(f"Error fetching balance: {wallet_balance_response.get('error', 'Unknown error')}")

            self._account_available_balances.clear()
            self._account_balances.clear()

            flex_account = wallet_balance_response.get("accounts", {}).get("flex", {})

            if flex_account:
                # Update total collateral value
                self._total_collateral_value = Decimal(str(flex_account.get("collateralValue", "0")))
                # self.logger().debug(f"Updated total collateral value: {self._total_collateral_value}")

                currencies = flex_account.get("currencies", {})
                for currency, balance_data in currencies.items():
                    # Convert XBT to BTC if needed
                    normalized_currency = "BTC" if currency == "XBT" else currency

                    # Get total and available balances, ensuring we use string values for Decimal conversion
                    total_balance = Decimal(str(balance_data.get("quantity", "0")))
                    available_balance = Decimal(str(balance_data.get("available", "0")))

                    # Only update if there's a non-zero balance
                    if total_balance > Decimal("0"):
                        self._account_balances[normalized_currency] = total_balance
                        self._account_available_balances[normalized_currency] = available_balance
                        # self.logger().debug(f"Updated {normalized_currency} balance - Total: {total_balance}, Available: {available_balance}")

                # Log portfolio summary if available
                portfolio_value = Decimal(str(flex_account.get("portfolioValue", "0")))
                # self.logger().debug(f"Portfolio Value: {portfolio_value}, Collateral Value: {self._total_collateral_value}")
        except Exception as e:
            self.logger().error(f"Error updating balances: {str(e)}", exc_info=True)
            raise

    async def _update_positions(self):
        """Retrieves all positions using the REST API."""
        position_tasks = []

        for trading_pair in self._trading_pairs:
            ex_trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair)
            body_params = {"symbol": ex_trading_pair}
            position_tasks.append(
                asyncio.create_task(self._api_get(
                    path_url=CONSTANTS.GET_POSITIONS_PATH_URL,
                    params=body_params,
                    is_auth_required=True,
                )))

        raw_responses: List[Dict[str, Any]] = await safe_gather(*position_tasks, return_exceptions=True)

        # Initial parsing of responses. Joining all the responses
        parsed_resps: List[Dict[str, Any]] = []
        for resp, trading_pair in zip(raw_responses, self._trading_pairs):
            if not isinstance(resp, Exception):
                positions = resp["positions"] if "positions" in resp else []
                if positions:
                    parsed_resps.extend(positions)
            else:
                self.logger().error(f"Error fetching positions for {trading_pair}. Response: {resp}")

        for position in parsed_resps:
            ex_trading_pair = position["symbol"]
            amount = Decimal(str(position["size"]))
            hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(ex_trading_pair)
            position_side = PositionSide.LONG if position["side"] == "buy" else PositionSide.SHORT
            pos_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)
            if amount != s_decimal_0:
                unrealized_pnl = Decimal(str(position["unrealizedPnl"]))
                entry_price = Decimal(str(position["price"]))
                leverage = Decimal(str(position["leverage"]))
                position = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount * (Decimal("-1.0") if position_side == PositionSide.SHORT else Decimal("1.0")),
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(pos_key, position)
            else:
                self._perpetual_trading.remove_position(pos_key)

        # Trigger balance update because Kraken doesn't have balance updates through the websocket
        safe_ensure_future(self._update_balances())

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                self.logger().debug(f"\n=== Fetching trade updates for order {order.client_order_id} ===")
                self.logger().debug(f"Exchange Order ID: {order.exchange_order_id}")
                self.logger().debug(f"Trading Pair: {order.trading_pair}")

                all_fills_response = await self._request_order_fills(order=order)
                self.logger().debug(f"Raw fills response: {all_fills_response}")
                self.logger().debug(f"Response type: {type(all_fills_response)}")

                # Handle string response (error case)
                if isinstance(all_fills_response, str):
                    self.logger().debug(f"Unexpected string response for order fills: {all_fills_response}")
                    return trade_updates

                # Handle non-dict response
                if not isinstance(all_fills_response, dict):
                    self.logger().debug(f"Unexpected response type: {type(all_fills_response)}. Expected dict.")
                    return trade_updates

                # Try to parse fills data
                try:
                    self.logger().debug("Attempting to extract fills data...")
                    result = all_fills_response.get("result", {})
                    self.logger().debug(f"Result field: {result}")

                    fills_data = result.get("fills", []) if isinstance(result, dict) else []
                    self.logger().debug(f"Extracted fills data: {fills_data}")
                    self.logger().debug(f"Fills data type: {type(fills_data)}")

                    if fills_data is not None:
                        for fill_data in fills_data:
                            self.logger().debug(f"Processing fill data: {fill_data}")
                            trade_update = self._parse_trade_update(trade_msg=fill_data, tracked_order=order)
                            self.logger().debug(f"Created trade update: {trade_update}")
                            trade_updates.append(trade_update)
                    else:
                        self.logger().debug("No fills data found in response")

                except AttributeError as ae:
                    self.logger().error(f"Error accessing fills data: {str(ae)}", exc_info=True)
                    return trade_updates

            except IOError as ex:
                if not self._is_request_exception_related_to_time_synchronizer(request_exception=ex):
                    raise
            except Exception as e:
                self.logger().error(f"Unexpected error processing trade updates: {str(e)}", exc_info=True)

        self.logger().debug(f"Returning {len(trade_updates)} trade updates")
        return trade_updates

    async def _request_order_fills(self, order: InFlightOrder) -> Dict[str, Any]:
        """Request order fills data from the exchange."""
        try:
            self.logger().debug(f"\n=== Requesting Order Fills ===\nOrder ID: {order.client_order_id}\nExchange Order ID: {order.exchange_order_id}")

            exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            self.logger().debug(f"Exchange Symbol: {exchange_symbol}")

            body_params = {
                "orderId": order.exchange_order_id,
                "symbol": exchange_symbol,
            }
            self.logger().debug(f"Request Parameters: {body_params}")

            res = await self._api_get(
                path_url=CONSTANTS.USER_TRADE_RECORDS_PATH_URL,
                params=body_params,
                is_auth_required=True,
                trading_pair=order.trading_pair,
            )

            self.logger().debug(f"Raw API Response: {res}")
            self.logger().debug(f"Response Type: {type(res)}")

            # Handle string response (usually error message)
            if isinstance(res, str):
                self.logger().warning(f"Received string response from fills request: {res}")
                return {"result": "error", "fills": [], "error": res}

            # Handle non-dict response
            if not isinstance(res, dict):
                self.logger().warning(f"Received unexpected response type: {type(res)}")
                return {"result": "error", "fills": [], "error": f"Unexpected response type: {type(res)}"}

            # Log response structure
            if "fills" in res:
                self.logger().debug(f"Number of fills in response: {len(res['fills'])}")
                if res['fills']:
                    self.logger().debug(f"Sample fill data: {res['fills'][0]}")
            else:
                self.logger().debug("No 'fills' key in response")

            # Ensure the response has the expected structure
            if "fills" not in res:
                self.logger().debug("Converting response to standard format")
                res = {"result": "success", "fills": res if isinstance(res, list) else []}

            self.logger().debug(f"Final processed response: {res}")
            return res

        except Exception as e:
            self.logger().error(f"Error requesting order fills: {str(e)}", exc_info=True)
            if hasattr(e, 'response'):
                try:
                    error_response = await e.response.text()
                    self.logger().error(f"Error response text: {error_response}")
                except:
                    pass
            return {"result": "error", "fills": [], "error": str(e)}

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        try:
            order_status_data = await self._request_order_status_data(tracked_order=tracked_order)
            order_msg = order_status_data["orders"][0]
            client_order_id = str(order_msg["order"]["cliOrdId"])

            order_update: OrderUpdate = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=CONSTANTS.ORDER_STATE[order_msg["status"]],
                client_order_id=client_order_id,
                exchange_order_id=order_msg["order"]["orderId"],
            )
        except IOError as ex:
            if self._is_request_exception_related_to_time_synchronizer(request_exception=ex):
                order_update = OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=tracked_order.current_state,
                )
            else:
                raise

        return order_update

    async def _request_order_status_data(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        """
        Request order status data from the exchange.
        Only checks active orders - history is checked separately as fallback.
        """
        order_id = tracked_order.exchange_order_id
        if order_id and isinstance(order_id, str):
            try:
                # Handle JSON string order IDs
                order_data = json.loads(order_id)
                if isinstance(order_data, dict):
                    if "sendStatus" in order_data and "order_id" in order_data["sendStatus"]:
                        order_id = order_data["sendStatus"]["order_id"]
                    elif "orderEvents" in order_data and order_data["orderEvents"] and "order" in order_data["orderEvents"][0]:
                        order_id = order_data["orderEvents"][0]["order"]["orderId"]
            except json.JSONDecodeError:
                pass

        active_order_data = {
            "processBefore": str(self._get_process_before_timestamp()),
            "orderId": order_id,
            "cliOrdId": tracked_order.client_order_id,
        }

        return await self._api_post(
            path_url=CONSTANTS.QUERY_ACTIVE_ORDER_PATH_URL,
            data=active_order_data,
            is_auth_required=True,
            limit_id=web_utils.DERIVATIVES_LIMIT_ID,  # Use derivatives pool with appropriate cost
            trading_pair=tracked_order.trading_pair,
        )

    async def _request_order_history_data(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        """
        Request order history data from the exchange.
        Uses the history rate limit pool.
        """
        try:
            history_params = {
                "limit": 1000,  # Limit to last 100 events
                "sort": "desc",  # Most recent first
            }

            history_resp = await self._api_get(
                path_url=CONSTANTS.HISTORY_ORDERS_ENDPOINT,
                params=history_params,
                is_auth_required=True,
                limit_id=web_utils.HISTORY_LIMIT_ID,  # Use history pool with appropriate cost
                trading_pair=tracked_order.trading_pair
            )

            if not history_resp:
                self.logger().warning(f"Empty response received for order history request")
                return {"result": "success", "orders": []}

            if isinstance(history_resp, dict):
                events = history_resp.get("elements", [])
                formatted_orders = []

                for event in events:
                    event_data = event.get("event", {})
                    if not event_data:
                        continue

                    event_type = next(iter(event_data)) if event_data else None
                    if not event_type:
                        continue

                    event_details = event_data[event_type]
                    order_data = None

                    if event_type == "OrderCancelled":
                        order_data = event_details.get("order", {})
                        if order_data.get("clientId") == tracked_order.client_order_id:
                            formatted_orders.append({
                                "order": {
                                    "orderId": order_data.get("uid"),
                                    "cliOrdId": order_data.get("clientId"),
                                    "status": "CANCELLED"
                                }
                            })
                    elif event_type == "OrderUpdated":
                        order_data = event_details.get("newOrder", {})
                        if order_data.get("clientId") == tracked_order.client_order_id:
                            reason = event_details.get("reason", "")
                            status = "FILLED" if reason == "full_fill" else "PARTIALLY_FILLED"
                            formatted_orders.append({
                                "order": {
                                    "orderId": order_data.get("uid"),
                                    "cliOrdId": order_data.get("clientId"),
                                    "status": status
                                }
                            })

            return {"result": "success", "orders": formatted_orders}

        except Exception as e:
            self.logger().error(f"Error requesting order history for {tracked_order.client_order_id}: {str(e)}", exc_info=True)
            return {"result": "success", "orders": []}

    def clear_order_history_cache(self):
        """
        Clears the order history cache and resets the last fetch timestamp.
        This can be called when we want to force a fresh fetch of order history.
        """
        self._order_history_cache = {}
        self._last_order_history_fetch_ts = 0
        # self.logger().info("Order history cache cleared")



    async def _user_stream_event_listener(self):
        """
        Listens to messages from _user_stream.
        """
        required_feeds = {
            "open_positions",
            "open_orders_verbose",
            "fills",
            "balances",
            "notifications_auth",
            "account_log"
        }
        confirmed_feeds = set()
        
        self.logger().info("\n=== Starting User Stream Event Listener ===")
        self.logger().info(f"Required feeds: {required_feeds}")

        async for event_message in self._iter_user_event_queue():
            try:
                # self.logger().debug(f"\nReceived user stream message: {event_message}")
                #
                # Handle subscription messages
                if isinstance(event_message, dict) and event_message.get("event") == "subscribed":
                    feed = event_message.get("feed")
                    if feed:
                        confirmed_feeds.add(feed)
                        self.logger().info(f"✓ Confirmed subscription to {feed}")
                        self.logger().info(f"Subscribed feeds: {len(confirmed_feeds)}/{len(required_feeds)} - {sorted(confirmed_feeds)}")
                        
                        # Check if all required feeds are confirmed
                        if confirmed_feeds.issuperset(required_feeds):
                            self.logger().info("✓ All required feeds confirmed!")
                            self._user_stream_tracker.data_source._user_stream_event.set()
                    continue

                # Process formatted messages based on event_type
                if isinstance(event_message, dict):
                    event_type = event_message.get("event_type")
                    data = event_message.get("data", {})
                    feed = data.get("feed") if isinstance(data, dict) else None
                    
                    if event_type == "position":
                        self.logger().info("Processing position update")
                        await self._process_account_position_event(data)
                    elif event_type == "order":
                        self.logger().info("Processing order update")
                        self._process_order_event_message(data)
                    elif event_type == "trade":
                        self.logger().info("Processing trade update")
                        self._process_trade_event_message(data)
                    elif event_type == "balance":
                        # self.logger().info("Processing balance update")
                        self._process_wallet_event_message(data)
                    elif event_type == "notification":
                        # Only log notifications that contain actual notifications
                        notifications = data.get("notifications", [])
                        if notifications:  # Only log if there are actual notifications
                            self.logger().info(f"Received notifications: {notifications}")
                    elif event_type == "account_log":
                        self.logger().debug(f"Received account log: {data}")
                    else:
                        self.logger().warning(f"Received message with unhandled event_type: {event_type}")
                        # self.logger().debug(f"Message content: {event_message}")
                else:
                    self.logger().warning(f"Received non-dict message: {event_message}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop: {str(e)}", exc_info=True)
                await self._sleep(5.0)

    async def _process_account_position_event(self, position_msg: Dict[str, Any]):
        """
        Updates position
        :param position_msg: The position event message payload
        """
        try:
            # Extract positions array from the message
            positions = []
            
            # Check if positions are directly in the message or nested in data field
            if "positions" in position_msg:
                positions = position_msg["positions"]
            elif "data" in position_msg and isinstance(position_msg["data"], dict):
                data = position_msg["data"]
                if "positions" in data:
                    positions = data["positions"]
            
            if not positions:
                self.logger().debug("No positions in message")
                return
            
            for position in positions:
                ex_trading_pair = position.get("instrument")
                if not ex_trading_pair:
                    self.logger().error(f"No instrument in position data: {position}")
                    continue
                
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=ex_trading_pair)
                
                # Get the raw balance amount
                amount = Decimal(str(position.get("balance", "0")))
                
                # Determine position side based on amount sign
                # Negative amount = SHORT position
                # Positive amount = LONG position
                position_side = PositionSide.SHORT if amount < Decimal("0") else PositionSide.LONG
                
                # Take absolute value of amount since we determine side by sign
                amount = abs(amount)
                
                pos_key = self._perpetual_trading.position_key(trading_pair, position_side)
                
                if amount != s_decimal_0:
                    entry_price = Decimal(str(position.get("entry_price", "0")))
                    unrealized_pnl = Decimal(str(position.get("pnl", "0")))
                    leverage = Decimal(str(position.get("effective_leverage", "1")))
                    
                    position_instance = Position(
                        trading_pair=trading_pair,
                        position_side=position_side,
                        unrealized_pnl=unrealized_pnl,
                        entry_price=entry_price,
                        amount=amount,  # Use absolute amount here
                        leverage=leverage,
                    )
                    self._perpetual_trading.set_position(pos_key, position_instance)
                    self.logger().info(f"Updated position: {trading_pair} {position_side} - Amount: {amount}, Entry: {entry_price}, PnL: {unrealized_pnl}")
                else:
                    self._perpetual_trading.remove_position(pos_key)
                    self.logger().info(f"Removed position: {trading_pair} {position_side}")
            
            # Trigger balance update because Kraken doesn't have balance updates through the websocket
            safe_ensure_future(self._update_balances())
        except Exception as e:
            self.logger().error(f"Error processing position event: {str(e)}", exc_info=True)
            self.logger().error(f"Position message: {position_msg}")

    def _process_order_event_message(self, order_msg: Dict[str, Any]):
        """
        Process order event message from WebSocket.
        :param order_msg: The order event message.
        """
        try:
            if not isinstance(order_msg, dict):
                self.logger().error(f"Unexpected order message format: {order_msg}")
                return
            
            # Extract the actual order data from the message
            actual_order_data = order_msg
            
            # Check if this is a formatted message with data field
            if "data" in order_msg and isinstance(order_msg["data"], dict):
                actual_order_data = order_msg["data"]
                self.logger().debug(f"Extracted order data from formatted message: {actual_order_data}")
            
            # Check if this is a batch of orders
            orders = []
            if "orders" in actual_order_data:
                orders = actual_order_data.get("orders", [])
                self.logger().info(f"Processing {len(orders)} orders from order feed")
            else:
                # Single order message
                orders = [actual_order_data]
            
            for order in orders:
                self._process_single_order(order)
                
        except Exception as e:
            self.logger().error(f"Error processing order event: {str(e)}", exc_info=True)
            self.logger().error(f"Order message: {order_msg}")
    
    def _process_single_order(self, order_data: Dict[str, Any]):
        """
        Process a single order from WebSocket.
        :param order_data: The order data.
        """
        # Skip if no client order ID
        client_order_id = str(order_data.get("cli_ord_id", ""))
        if not client_order_id:
            self.logger().debug(f"No client order ID in order data: {order_data}")
            return
        
        # Skip if order not found in tracker
        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked_order is None:
            self.logger().debug(f"Order {client_order_id} not found in order tracker")
            return
        
        # Get order status
        order_status = order_data.get("status", "")
        
        # Map Kraken status to Hummingbot status
        new_state = CONSTANTS.ORDER_STATE.get(order_status, None)
        if new_state is None:
            self.logger().warning(f"Unknown order status: {order_status}")
            return
        
        # Get filled amount
        filled_amount = Decimal(str(order_data.get("filled", "0")))
        total_amount = Decimal(str(order_data.get("size", "0")))
        
        # Get exchange order ID
        exchange_order_id = str(order_data.get("order_id", ""))
        
        # Get timestamp
        timestamp = order_data.get("last_update_time", int(time.time() * 1000))
        
        self.logger().info(f"\nCreating order update:"
                          f"\n  Client Order ID: {client_order_id}"
                          f"\n  Exchange Order ID: {exchange_order_id}"
                          f"\n  Status: {order_status}"
                          f"\n  Filled Amount: {filled_amount}/{total_amount}"
                          f"\n  New State: {new_state}")
        
        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=timestamp,
            new_state=new_state,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
        
        self._order_tracker.process_order_update(order_update)

    def _process_historical_order_event_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers order update event for order message received through the history API.
        :param order_msg: The order event message payload
        """
        # self.logger().info(f"\n=== Processing Historical Order Event ===\nMessage: {order_msg}")

        # Extract order data and event type
        order_data = None
        event_type = None
        order_update_reason = None

        # Handle the case where we receive the event structure directly
        if "event" in order_msg:
            event = order_msg["event"]
            event_type = next(iter(event)) if event else None
            if event_type == "OrderCancelled":
                order_data = event[event_type].get("order", {})
                order_update_reason = "cancelled_by_user"
            elif event_type == "OrderUpdated":
                order_data = event[event_type].get("newOrder", {})
                order_update_reason = event[event_type].get("reason", "")
            elif event_type == "OrderPlaced":
                order_data = event[event_type].get("order", {})
                order_update_reason = "placed"
        else:
            # Handle the case where we receive the order data directly
            if isinstance(order_msg.get("order"), dict) and isinstance(order_msg["order"].get("order"), dict):
                # Handle double-nested structure
                order_data = order_msg["order"]["order"]
            else:
                # Handle single-nested structure
                order_data = order_msg.get("order", {})

        if not order_data:
            self.logger().error(f"No order data found in message: {order_msg}")
            return

        # Extract client order ID from the order data
        client_order_id = order_data.get("cliOrdId")

        if not client_order_id:
            self.logger().error(f"No client order ID found in order data structure: {order_data}")
            return

        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if not tracked_order:
            self.logger().debug(f"Order {client_order_id} not found in order tracker")
            return

        exchange_order_id = order_data.get("orderId") or order_data.get("uid", "")
        timestamp = self.current_timestamp

        # Map the order status and check fills
        order_status = order_data.get("status", "UNKNOWN")
        filled_amount = Decimal(str(order_data.get("filled", "0")))
        total_amount = Decimal(str(order_data.get("size", "0")))

        # Determine the new state based on status and fills
        if order_status == "CANCELLED":
            new_state = CONSTANTS.ORDER_STATE["CANCELLED"]
        elif filled_amount >= total_amount and total_amount > 0:
            new_state = CONSTANTS.ORDER_STATE["FILLED"]
        elif filled_amount > 0:
            new_state = CONSTANTS.ORDER_STATE["PARTIALLY_FILLED"]
        elif order_status == "FILLED":
            new_state = CONSTANTS.ORDER_STATE["FILLED"]
        else:
            new_state = CONSTANTS.ORDER_STATE.get(order_status, CONSTANTS.ORDER_STATE["UNKNOWN"])

        self.logger().info(f"\nCreating order update:"
                          f"\n  Client Order ID: {client_order_id}"
                          f"\n  Exchange Order ID: {exchange_order_id}"
                          f"\n  Status: {order_status}"
                          f"\n  Filled Amount: {filled_amount}/{total_amount}"
                          f"\n  New State: {new_state}")

        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=timestamp,
            new_state=new_state,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )

        self._order_tracker.process_order_update(order_update)

    def _process_trade_event_message(self, trade_msg: Dict[str, Any]):
        """
        Process trade event message from WebSocket or REST API.
        :param trade_msg: The trade event message.
        """
        try:
            if not isinstance(trade_msg, dict):
                self.logger().error(f"Unexpected trade message format: {trade_msg}")
                return
            
            # Extract the actual trade data from the message
            actual_trade_data = trade_msg
            
            # Check if this is a formatted message with data field
            if "data" in trade_msg and isinstance(trade_msg["data"], dict):
                actual_trade_data = trade_msg["data"]
                self.logger().debug(f"Extracted trade data from formatted message: {actual_trade_data}")
            
            # Check if this is a live WebSocket message by looking for WebSocket-specific fields
            is_ws_message = "feed" in actual_trade_data or "cli_ord_id" in actual_trade_data
            
            # Check if this is from a fills_snapshot
            is_snapshot = actual_trade_data.get("feed") == "fills_snapshot"
            
            # Check if this is a fills feed message with multiple fills
            if is_ws_message and "fills" in actual_trade_data:
                fills = actual_trade_data.get("fills", [])
                self.logger().info(f"Processing {len(fills)} fills from {'fills_snapshot' if is_snapshot else 'fills feed'}")
                
                for fill in fills:
                    # Add source information to the fill
                    if is_snapshot:
                        fill["source"] = "fills_snapshot"
                    
                    # Check if this is an assignment fill
                    if fill.get("fill_type") == "assignment" or fill.get("fill_type") == "assignee":
                        self.logger().info(f"Processing {'historical' if is_snapshot else 'real-time'} assignment fill: {fill}")
                        self._process_assignment_fill(fill)
                    else:
                        # Process regular fill
                        self.logger().info(f"Processing regular fill: {fill}")
                        self._process_single_trade(fill, is_ws_message=True)
                return
            
            # Process single trade
            self._process_single_trade(actual_trade_data, is_ws_message)
            
        except Exception as e:
            self.logger().error(f"Error processing trade event: {str(e)}", exc_info=True)
            self.logger().error(f"Trade message: {trade_msg}")
    
    def _process_single_trade(self, trade_data: Dict[str, Any], is_ws_message: bool = False):
        """
        Process a single trade from WebSocket or REST API.
        :param trade_data: The trade data.
        :param is_ws_message: Whether this is from WebSocket.
        """
        if is_ws_message:
            self.logger().info(f"\n=== Processing Trade ===\nTrade data: {trade_data}")
        
        # Handle both WebSocket (cli_ord_id) and REST (cliOrdId) formats
        client_order_id = str(trade_data.get("cli_ord_id") or trade_data.get("cliOrdId", ""))
        if is_ws_message:
            self.logger().info(f"Client Order ID: {client_order_id}")
        
        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
        if tracked_order is None:
            if is_ws_message:
                self.logger().debug(f"Order {client_order_id} not found in order tracker")
                self.logger().debug(f"Current orders in tracker: {list(self._order_tracker.all_fillable_orders.keys())}")
            return
        
        if is_ws_message:
            self.logger().info(
                f"\nOrder details before trade update:"
                f"\n  Order ID: {client_order_id}"
                f"\n  State: {tracked_order.current_state}"
                f"\n  Filled Amount: {tracked_order.executed_amount_base}/{tracked_order.amount}"
            )
        
        # Handle both WebSocket (fee_currency) and REST (feeCurrency) formats
        fee_currency = trade_data.get("fee_currency") or trade_data.get("feeCurrency", "")
        fee_paid = trade_data.get("fee_paid") or trade_data.get("fee", "0")
        
        # Remove any negative sign from fee currency and ensure it's not None
        fee_currency = fee_currency.replace("-", "") if fee_currency else ""
        
        # Get quote currency directly from the order
        quote_currency = tracked_order.quote_asset
        
        # Process fees
        fee_amount = Decimal(str(fee_paid))
        flat_fees = []
        
        if fee_currency and fee_amount > Decimal("0"):
            # Log the fee details for debugging
            self.logger().info(f"Processing fee: {fee_amount} {fee_currency}")
            flat_fees = [TokenAmount(amount=fee_amount, token=fee_currency)]
        
        # Create the fee object
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=tracked_order.position,
            percent_token=quote_currency,
            flat_fees=flat_fees,
        )
        
        # Handle both WebSocket (qty) and REST (size) formats
        fill_size = Decimal(str(trade_data.get("qty") or trade_data.get("size", "0")))
        fill_price = Decimal(str(trade_data.get("price", "0")))
        fill_quote_amount = fill_size * fill_price
        
        # Handle both WebSocket (fill_id, order_id) and REST (fillId, orderId) formats
        trade_id = str(trade_data.get("fill_id") or trade_data.get("fillId", ""))
        exchange_order_id = str(trade_data.get("order_id") or trade_data.get("orderId", ""))
        
        # Handle both WebSocket (time) and REST (fillTime) formats
        timestamp = trade_data.get("time")
        if timestamp is None:
            fill_time = trade_data.get("fillTime", "1970-01-01T00:00:00.000Z")
            timestamp = int(datetime.strptime(fill_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1e3)
        
        if is_ws_message:
            self.logger().info(
                f"\nProcessed trade details:"
                f"\n  Trade ID: {trade_id}"
                f"\n  Exchange Order ID: {exchange_order_id}"
                f"\n  Fill Size: {fill_size}"
                f"\n  Fill Price: {fill_price}"
                f"\n  Fee: {fee_paid} {fee_currency}"
            )
        
        trade_update = TradeUpdate(
            trade_id=trade_id,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fee=fee,
            fill_base_amount=fill_size,
            fill_quote_amount=fill_quote_amount,
            fill_price=fill_price,
            fill_timestamp=timestamp,
        )
        
        self._order_tracker.process_trade_update(trade_update)

    def _parse_trade_update(self, trade_msg: Dict, tracked_order: InFlightOrder) -> TradeUpdate:
        """Parse trade update message from the exchange."""
        trade_id = str(trade_msg["fill_id"])
        exchange_order_id = str(trade_msg["order_id"])

        # Remove any negative sign from fee currency
        fee_currency = trade_msg["fee_currency"].replace("-", "") if trade_msg.get("fee_currency") else ""
        fee_amount = Decimal(str(trade_msg["fee_paid"]))

        position_action = (PositionAction.OPEN
                           if (tracked_order.trade_type is TradeType.BUY and trade_msg["buy"]
                               or tracked_order.trade_type is TradeType.SELL and not trade_msg["buy"])
                           else PositionAction.CLOSE)

        # Get quote currency directly from the order
        quote_currency = tracked_order.quote_asset
        
        # Process fees
        flat_fees = []
        if fee_currency and fee_amount > Decimal("0"):
            # Log the fee details
            self.logger().info(f"Processing fee in trade update: {fee_amount} {fee_currency}")
            flat_fees = [TokenAmount(amount=fee_amount, token=fee_currency)]

        # Create the fee object
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent_token=quote_currency,
            flat_fees=flat_fees,
        )

        fill_price = Decimal(str(trade_msg["price"]))
        fill_size = Decimal(str(trade_msg["qty"]))

        trade_update = TradeUpdate(
            trade_id=trade_id,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fill_timestamp=int(trade_msg["time"]) * 1e-3,  # Convert milliseconds to seconds
            fill_price=fill_price,
            fill_base_amount=fill_size,
            fill_quote_amount=fill_price * fill_size,
            fee=fee,
        )

        return trade_update

    def _process_wallet_event_message(self, wallet_msg: Dict[str, Any]) -> None:
        """
        Process wallet balance update message.
        :param wallet_msg: The wallet balance update message.
        """
        try:
            # Extract flex_futures data from the message
            flex_futures_data = None
            
            # Check if this is a direct message or nested in data field
            if "flex_futures" in wallet_msg:
                flex_futures_data = wallet_msg["flex_futures"]
            elif "data" in wallet_msg and isinstance(wallet_msg["data"], dict):
                data = wallet_msg["data"]
                if "flex_futures" in data:
                    flex_futures_data = data["flex_futures"]
                elif "feed" in data and data["feed"] == "balances":
                    flex_futures_data = data.get("flex_futures", {})
            
            if flex_futures_data is None:
                self.logger().debug(f"No flex_futures data in wallet message: {wallet_msg}")
                return
            
            # Process currencies in flex_futures account
            currencies = flex_futures_data.get("currencies", {})
            for currency, details in currencies.items():
                # Get quantity and collateral value
                quantity = details.get("quantity", 0)
                collateral_value = details.get("collateral_value", 0)
                
                # Convert values to Decimal
                available_balance = Decimal(str(quantity))
                total_balance = Decimal(str(quantity))
                
                # Update balance for this currency
                self._account_balances[currency] = total_balance
                self._account_available_balances[currency] = available_balance
                
                # self.logger().debug(f"Updated {currency} balance - Total: {total_balance}, Available: {available_balance}")
            
            # Log portfolio summary
            portfolio_value = Decimal(str(flex_futures_data.get("portfolio_value", 0)))
            collateral_value = Decimal(str(flex_futures_data.get("collateral_value", 0)))
            self._total_collateral_value = collateral_value
            # self.logger().info(f"Portfolio Value: {portfolio_value}, Collateral Value: {collateral_value}")
            
        except Exception as e:
            self.logger().error(f"Error processing wallet event: {str(e)}", exc_info=True)
            self.logger().error(f"Wallet message: {wallet_msg}")

    async def _get_max_order_size(self, symbol: str) -> Decimal:
        """
        Fetches the maximum order size for a given symbol from the exchange.
        :param symbol: The exchange symbol to get max order size for
        :returns: The maximum order size as a Decimal
        """
        try:
            params = {
                "orderType": "mkt",  # Using "limit" instead of "lmt" for production environment
                "symbol": symbol,
            }

            self.logger().info(f"\n=== Getting Max Order Size ===\nSymbol: {symbol}\nParams: {params}")

            max_order_info = await self._api_get(
                path_url=CONSTANTS.MAX_ORDER_SIZE_ENDPOINT,
                params=params,
                method=RESTMethod.GET,
                is_auth_required=True
            )

            self.logger().info(f"Max order size response: {max_order_info}")

            if max_order_info.get("result") == "success":
                # Get raw values and handle None cases
                max_buy_size_raw = max_order_info.get("maxBuySize")
                max_sell_size_raw = max_order_info.get("maxSellSize")

                # Convert to Decimal, using 0 for None values
                max_buy_size = Decimal(str(max_buy_size_raw)) if max_buy_size_raw is not None else Decimal("0")
                max_sell_size = Decimal(str(max_sell_size_raw)) if max_sell_size_raw is not None else Decimal("0")

                self.logger().info(f"Parsed sizes - Buy: {max_buy_size}, Sell: {max_sell_size}")

                # If both are 0, return 0
                if max_buy_size == Decimal("0") and max_sell_size == Decimal("0"):
                    self.logger().info("Both buy and sell sizes are 0, returning 0")
                    return Decimal("0")

                # If one is 0, return the other
                if max_buy_size == Decimal("0"):
                    self.logger().info(f"Buy size is 0, returning sell size: {max_sell_size}")
                    return max_sell_size
                if max_sell_size == Decimal("0"):
                    self.logger().info(f"Sell size is 0, returning buy size: {max_buy_size}")
                    return max_buy_size

                # If both have values, return the minimum
                result = min(max_buy_size, max_sell_size)
                self.logger().info(f"Returning minimum of buy/sell sizes: {result}")
                return result
            else:
                error_msg = max_order_info.get("error", "Unknown error")
                self.logger().warning(f"Error getting max order size for {symbol}: {error_msg}")
                return Decimal("0")
        except Exception as e:
            self.logger().error(f"Error fetching max order size for {symbol}: {str(e)}", exc_info=True)
            return Decimal("0")

    async def _format_trading_rules(self, instruments_info: List[Dict[str, Any]]) -> List[TradingRule]:
        """Format the trading rules response into TradingRule instances"""
        self.logger().info("\n=== Formatting Trading Rules ===")
        trading_rules = {}
        symbol_map = await self.trading_pair_symbol_map()
        self.logger().info(f"Symbol map: {symbol_map}")
        
        # Process only the tradeable instruments that are in our symbol map
        valid_instruments = [
            instrument for instrument in instruments_info
            if instrument.get("symbol").startswith("PF_") and instrument.get("symbol").endswith("USD") and instrument["symbol"] in symbol_map
        ]
        self.logger().info(f"Found {len(valid_instruments)} valid instruments")
        
        # Get ticker data for all instruments in one call
        ticker_tasks = []
        for instrument in valid_instruments:
            params = {"symbol": instrument["symbol"]}
            ticker_tasks.append(
                self._api_get(
                    path_url=CONSTANTS.TICKER_PRICE_ENDPOINT,
                    params=params,
                    limit_id=web_utils.PUBLIC_LIMIT_ID,
                )
            )
        ticker_responses = await safe_gather(*ticker_tasks, return_exceptions=True)
        self.logger().info(f"Received {len(ticker_responses)} ticker responses")

        # Create a map of symbol to ticker data
        ticker_map = {}
        for instrument, ticker_response in zip(valid_instruments, ticker_responses):
            exchange_symbol = instrument["symbol"]
            self.logger().debug(f"Processing ticker for {exchange_symbol}")
            
            if not isinstance(ticker_response, Exception) and ticker_response.get("result") == "success":
                tickers = ticker_response.get("tickers", [])
                self.logger().debug(f"Response contains {len(tickers)} tickers")
                
                # Find the ticker that matches our instrument symbol
                matching_ticker = None
                for ticker in tickers:
                    ticker_symbol = ticker.get("symbol")
                    if ticker_symbol == exchange_symbol:
                        matching_ticker = ticker
                        self.logger().debug(f"Found matching ticker: {ticker}")
                        break
                
                if matching_ticker:
                    ticker_map[exchange_symbol] = matching_ticker
                else:
                    self.logger().warning(f"No matching ticker found for {exchange_symbol}")
            else:
                error = str(ticker_response) if isinstance(ticker_response, Exception) else ticker_response.get("error", "Unknown error")
                self.logger().error(f"Failed to get ticker for {exchange_symbol}: {error}")

        for instrument in valid_instruments:
            try:
                exchange_symbol = instrument["symbol"]
                trading_pair = symbol_map[exchange_symbol]
                self.logger().info(f"\nProcessing trading rules for {trading_pair} ({exchange_symbol})")

                # Calculate min_order_size based on contractValueTradePrecision
                precision_str = str(instrument["contractValueTradePrecision"])
                try:
                    precision = int(float(precision_str))  # Handles both int and float strings
                except ValueError:
                    self.logger().error(f"Error parsing precision for {exchange_symbol}: {precision_str}")
                    continue

                min_order_size = Decimal(str("1" + "0" * abs(precision))) if precision < 0 else Decimal(
                    ("0." + "0" * (precision - 1) + "1") if precision > 0 else "1")
                self.logger().info(f"Calculated min order size: {min_order_size}")

                # Get max position size from instrument info
                max_position_size = Decimal(str(instrument.get("maxPositionSize", "0")))
                self.logger().info(f"Max position size from instrument: {max_position_size}")

                # Calculate max order size based on ticker data and initial margin
                ticker_data = ticker_map.get(exchange_symbol)
                if ticker_data:
                    # Get best bid/ask prices for this specific trading pair
                    best_bid = Decimal(str(ticker_data.get("bid", "0")))
                    best_ask = Decimal(str(ticker_data.get("ask", "0")))
                    mid_price = (best_bid + best_ask) / Decimal("2")
                    self.logger().info(f"Price data - Bid: {best_bid}, Ask: {best_ask}, Mid: {mid_price}")

                    # Get initial margin requirement from the instrument data
                    margin_levels = instrument.get("marginLevels", [])
                    if margin_levels:
                        # Use the first level's margin requirement (smallest position size)
                        initial_margin = Decimal(str(margin_levels[0].get("initialMargin", "0.02")))
                        self.logger().info(f"Using initial margin {initial_margin} from margin levels")
                    else:
                        # Fallback to default margin if no levels defined
                        initial_margin = Decimal("0.02")  # Default to 2% if no margin levels found
                        self.logger().warning(f"No margin levels found for {trading_pair}, using default {initial_margin}")

                    # Calculate max order size using this pair's specific prices
                    if mid_price > 0 and initial_margin > 0:
                        max_order_size = (self._total_collateral_value / mid_price) / initial_margin
                        # Apply a safety factor (e.g., 0.99) to account for price movements
                        max_order_size = max_order_size * Decimal("0.99")
                        self.logger().info(f"Calculated max order size: {max_order_size}")
                    else:
                        self.logger().warning(
                            f"Invalid price or margin for {trading_pair}. Using max position size."
                            f"\n    Mid price: {mid_price}"
                            f"\n    Initial margin: {initial_margin}"
                        )
                        max_order_size = max_position_size
                else:
                    self.logger().warning(f"No ticker data available for {exchange_symbol}, using max position size")
                    max_order_size = max_position_size

                # Convert numeric values directly to Decimal
                min_price_increment = Decimal(str(instrument["tickSize"]))
                min_base_amount_increment = min_order_size

                # For Kraken Perpetual, the collateral token is always the quote currency
                quote_currency = "USD"

                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    max_order_size=max_order_size,
                    min_price_increment=min_price_increment,
                    min_base_amount_increment=min_base_amount_increment,
                    min_notional_size=Decimal("0"),  # Kraken doesn't specify this
                    min_order_value=Decimal("0"),  # Kraken doesn't specify this
                    buy_order_collateral_token=quote_currency,
                    sell_order_collateral_token=quote_currency,
                )

                trading_rules[trading_pair] = trading_rule
                self.logger().info(f"\nCreated trading rule for {trading_pair}:"
                                 f"\n  Min Order Size: {trading_rule.min_order_size}"
                                 f"\n  Max Order Size: {trading_rule.max_order_size}"
                                 f"\n  Min Price Increment: {trading_rule.min_price_increment}"
                                 f"\n  Min Base Amount Increment: {trading_rule.min_base_amount_increment}")

            except Exception as e:
                self.logger().error(f"Error parsing trading rule for instrument: {instrument}. Error: {str(e)}")
                continue

        return list(trading_rules.values())

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        """Initialize trading pair symbols from exchange info."""
        try:
            mapping = bidict()
            # seen_trading_pairs = {}  # Track trading pairs and their exchange symbols

            # exchange_info is already a list of instruments
            for symbol_data in filter(kraken_utils.is_exchange_information_valid, exchange_info):
                exchange_symbol = symbol_data["symbol"]
                # self.logger().debug(f"Processing exchange symbol: {exchange_symbol}")

                # Skip if not a perpetual futures symbol
                if not exchange_symbol.startswith("PF_") or not exchange_symbol.endswith("USD"):
                    # self.logger().debug(f"Skipping non-perpetual futures symbol: {exchange_symbol}")
                    continue

                # Extract base currency with special handling for USDT and T
                base = exchange_symbol[3:].replace("USD", "")  # Remove PF_ prefix and USD suffix
                
                # Special handling for USDT vs T
                if exchange_symbol == "PF_USDTUSD":
                    base = "USDT"
                elif exchange_symbol == "PF_TUSD":
                    base = "T"
                
                quote = "USD"

                # Convert XBT to BTC if needed
                if base == "XBT":
                    base = "BTC"

                trading_pair = combine_to_hb_trading_pair(base, quote)
                # self.logger().debug(f"Converted to trading pair: {trading_pair}")

                # Add new mapping
                mapping[exchange_symbol] = trading_pair
                # self.logger().debug(f"Added mapping: {exchange_symbol} -> {trading_pair}")

            # self.logger().info(f"Initialized {len(mapping)} trading pair symbols")
            self._set_trading_pair_symbol_map(mapping)

        except Exception as e:
            self.logger().error(f"Error initializing trading pair symbols: {str(e)}", exc_info=True)
            raise

    def _resolve_trading_pair_symbols_duplicate(self, mapping: bidict, new_exchange_symbol: str) -> None:
        """
        Resolves name conflicts for perpetual contracts.
        Uses the mapping to compare the current and new symbols, keeping the one that follows PF_ or PI_ format.
        """
        # Get the trading pair and current symbol from the mapping
        expected_exchange_symbol = f"PF_{mapping.exchange_symbol}"
        trading_pair = expected_exchange_symbol
        current_exchange_symbol = mapping.inverse[trading_pair]
        if current_exchange_symbol == expected_exchange_symbol:
            pass
        elif new_exchange_symbol == expected_exchange_symbol:
            mapping.pop(current_exchange_symbol)
            mapping[new_exchange_symbol] = trading_pair
        else:
            self.logger().error(f"Could not resolve the exchange symbols {new_exchange_symbol} and {current_exchange_symbol}")
            mapping.pop(current_exchange_symbol)

        # self.logger().info(f"Could not resolve the exchange symbols {new_exchange_symbol} and {current_exchange_symbol}")

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Fetches the last traded price from the exchange API for the specified trading pair.
        :param trading_pair: The trading pair to fetch the price for
        :return: The last traded price
        """
        try:
            exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

            resp_json = await self._api_get(
                path_url=CONSTANTS.TICKER_PRICE_ENDPOINT,
                limit_id=web_utils.PUBLIC_LIMIT_ID,
            )

            if resp_json.get("result") != "success":
                raise IOError(f"Error fetching ticker price: {resp_json.get('error', 'Unknown error')}")

            tickers = resp_json.get("tickers", [])
            if not tickers:
                raise IOError(f"No ticker data found for {trading_pair}")

            # Find the ticker for our symbol
            ticker = next((t for t in tickers if t.get("symbol") == exchange_symbol), None)
            if not ticker:
                raise IOError(f"No ticker found for symbol {exchange_symbol}")

            if "last" not in ticker:
                raise IOError(f"Last price not found in ticker data for {trading_pair}")

            return float(ticker["last"])

        except Exception as e:
            self.logger().network(
                f"Error getting last traded price for {trading_pair}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Error getting last traded price for {trading_pair}. Check API key and network connection."
            )
            raise

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        data = {
            "symbol": exchange_symbol,
            "leverage": str(leverage)
        }
        try:
            resp: Dict[str, Any] = await self._api_put(
                path_url=CONSTANTS.SET_LEVERAGE_PATH_URL,
                data=data,
                is_auth_required=True,
            )

            if resp["result"] == "success":
                return True, ""
            else:
                error_msg = resp.get("error", "Unknown error")
                self.logger().network(
                    f"Error setting leverage {leverage} for {trading_pair}: {error_msg}",
                    app_warning_msg=f"Error setting leverage {leverage} for {trading_pair}: {error_msg}",
                )
                return False, error_msg
        except Exception as e:
            error_msg = str(e)
            self.logger().network(
                f"Error setting leverage {leverage} for {trading_pair}: {error_msg}",
                app_warning_msg=f"Error setting leverage {leverage} for {trading_pair}: {error_msg}",
                exc_info=True
            )
            return False, error_msg

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

        try:
            response = await self._api_get(
                path_url=CONSTANTS.HISTORICAL_FUNDING_RATES_ENDPOINT,
                params={"symbol": exchange_symbol},
                is_auth_required=False,
                limit_id=web_utils.PUBLIC_LIMIT_ID,
            )

            rates = response.get("rates", [])

            if not rates:
                self._logger.error("No rates found")
                raise IOError(f"No funding rates found for {trading_pair}")

            # Get the most recent rate (last element since they're in ascending order)
            last_funding = rates[-1]

            # Parse ISO8601 timestamp directly to Unix timestamp
            timestamp_str = last_funding.get("timestamp")
            if not timestamp_str:
                raise IOError(f"Missing timestamp in funding rate data for {trading_pair}")

            try:
                timestamp = int(datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
            except ValueError:
                # Try alternative format without microseconds
                timestamp = int(datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").timestamp())
            self._logger.info(f"Converted timestamp: {timestamp}")

            # Extract funding rate and payment with safer dictionary access
            funding_rate = Decimal(str(last_funding.get("fundingRate", "0")))
            payment = Decimal(str(last_funding.get("relativeFundingRate", "0")))

            return timestamp, funding_rate, payment

        except Exception as e:
            self._logger.error(f"Error in _fetch_last_fee_payment: {str(e)}")
            raise

    @staticmethod
    def _format_ret_code_for_print(ret_code: Union[str, int]) -> str:
        return f"ret_code <{ret_code}>"

    async def _api_request(
            self,
            path_url,
            method: RESTMethod = RESTMethod.GET,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            limit_id: Optional[str] = None,
            is_auth_required: bool = False,
            **kwargs) -> Dict[str, Any]:

        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        if limit_id is None:
            limit_id = web_utils.get_rest_api_limit_id_for_endpoint(endpoint=path_url)
        url = web_utils.get_rest_url_for_endpoint(endpoint=path_url, domain=self._domain)

        response = await rest_assistant.execute_request(
            url=url,
            params=params,
            data=data,
            method=method,
            is_auth_required=is_auth_required,
            throttler_limit_id=limit_id if limit_id else path_url,
            )
        return response


    def _get_position_idx(self, trade_type: TradeType, position_action: PositionAction = None) -> int:
        """
        Returns the position index for the given trade type and position action.
        Since Kraken only supports one-way positions, it always returns 0.
        """
        if position_action == PositionAction.NIL:
            raise NotImplementedError(f"Invalid position action {position_action}. Must be one of {[PositionAction.OPEN, PositionAction.CLOSE]}")
        if trade_type not in [TradeType.BUY, TradeType.SELL]:
            raise NotImplementedError(f"Unsupported trade type {trade_type}")
        return 0  # Kraken only supports one-way positions

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """Convert a trading pair to the exchange symbol."""
        return kraken_utils.convert_to_exchange_trading_pair(trading_pair)

    async def set_assignment_program(
        self,
        contract_type: str,
        contract: Optional[str] = None,
        max_size: Optional[float] = None,
        max_position: Optional[float] = None,
        accept_long: bool = True,
        accept_short: bool = True,
        time_frame: str = "all",
        enabled: bool = True,
    ) -> Tuple[bool, str]:
        """
        Sets the assignment program preferences for the exchange.

        :param contract_type: Type of contract for the assignment program preference
        :param contract: A specific contract for this assignment program preference (optional)
        :param max_size: The maximum size for an assignment (optional)
        :param max_position: The maximum position (optional)
        :param accept_long: Accept to take long positions (default True)
        :param accept_short: Accept to take short positions (default True)
        :param time_frame: When the program preference is valid (default "all")
        :param enabled: Enable/disable assignment (default True)
        :returns: A tuple of (success: bool, error_message: str)
        """
        valid_time_frames = ["all", "weekdays", "weekends"]
        if time_frame not in valid_time_frames:
            return False, f"Invalid time_frame. Must be one of: {', '.join(valid_time_frames)}"

        data = {
            "contractType": contract_type,
            "acceptLong": accept_long,
            "acceptShort": accept_short,
            "timeFrame": time_frame,
            "enabled": enabled,
        }

        # Add optional parameters only if they are provided
        if contract is not None:
            data["contract"] = contract
        if max_size is not None:
            data["maxSize"] = max_size
        if max_position is not None:
            data["maxPosition"] = max_position

        try:
            response = await self._api_post(
                path_url=CONSTANTS.ASSIGNMENT_ADD_PATH_URL,
                data=data,
                is_auth_required=True,
            )

            if response.get("result") == "success":
                return True, ""
            else:
                error_msg = response.get("error", "Unknown error")
                self.logger().network(
                    f"Error setting assignment program: {error_msg}",
                    app_warning_msg=f"Error setting assignment program: {error_msg}",
                )
                return False, error_msg

        except Exception as e:
            error_msg = str(e)
            self.logger().network(
                f"Error setting assignment program: {error_msg}",
                app_warning_msg=f"Error setting assignment program: {error_msg}",
                exc_info=True
            )
            return False, error_msg

    async def delete_assignment_program(self, program_id: int) -> Tuple[bool, str]:
        """
        Deletes an assignment program by its ID.

        :param program_id: The ID of the program to delete
        :returns: A tuple of (success: bool, error_message: str)
        """
        try:
            data = {"id": program_id}
            response = await self._api_post(
                path_url=CONSTANTS.ASSIGNMENT_DELETE_PATH_URL,
                data=data,
                is_auth_required=True,
            )

            if response.get("result") == "success":
                return True, ""
            else:
                error_msg = response.get("error", "Unknown error")
                self.logger().network(
                    f"Error deleting assignment program {program_id}: {error_msg}",
                    app_warning_msg=f"Error deleting assignment program {program_id}: {error_msg}",
                )
                return False, error_msg

        except Exception as e:
            error_msg = str(e)
            self.logger().network(
                f"Error deleting assignment program {program_id}: {error_msg}",
                app_warning_msg=f"Error deleting assignment program {program_id}: {error_msg}",
                exc_info=True
            )
            return False, error_msg

    async def get_current_assignment_programs(self) -> Tuple[bool, Union[str, List[Dict[str, Any]]]]:
        """
        Gets the current assignment programs.

        :returns: A tuple of (success: bool, error_message: str) or (success: bool, programs: List[Dict])
        """
        try:
            response = await self._api_get(
                path_url=CONSTANTS.ASSIGNMENT_CURRENT_PATH_URL,
                is_auth_required=True,
            )

            if response.get("result") == "success":
                return True, response.get("programs", [])
            else:
                error_msg = response.get("error", "Unknown error")
                self.logger().network(
                    f"Error getting current assignment programs: {error_msg}",
                    app_warning_msg=f"Error getting current assignment programs: {error_msg}",
                )
                return False, error_msg

        except Exception as e:
            error_msg = str(e)
            self.logger().network(
                f"Error getting current assignment programs: {error_msg}",
                app_warning_msg=f"Error getting current assignment programs: {error_msg}",
                exc_info=True
            )
            return False, error_msg

    async def get_assignment_program_history(self) -> Tuple[bool, Union[str, List[Dict[str, Any]]]]:
        """
        Gets the history of assignment programs.

        :returns: A tuple of (success: bool, error_message: str) or (success: bool, history: List[Dict])
        """
        try:
            response = await self._api_get(
                path_url=CONSTANTS.ASSIGNMENT_HISTORY_PATH_URL,
                is_auth_required=True,
            )

            if response.get("result") == "success":
                return True, response.get("history", [])
            else:
                error_msg = response.get("error", "Unknown error")
                self.logger().network(
                    f"Error getting assignment program history: {error_msg}",
                    app_warning_msg=f"Error getting assignment program history: {error_msg}",
                )
                return False, error_msg

        except Exception as e:
            error_msg = str(e)
            self.logger().network(
                f"Error getting assignment program history: {error_msg}",
                app_warning_msg=f"Error getting assignment program history: {error_msg}",
                exc_info=True
            )
            return False, error_msg

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        Returns a dictionary of statuses of various connector's components
        """
        status_d = super().status_dict
        status_d["funding_info"] = self._perpetual_trading.is_funding_info_initialized()

        # Log overall readiness
        is_ready = all(status_d.values())
        # self.logger().info(f"\nOverall connector ready: {is_ready}")
        if not is_ready:
            not_ready = [k for k, v in status_d.items() if not v]
            # self.logger().info(f"Waiting for: {', '.join(not_ready)}")

        return status_d

    def _process_assignment_fill(self, fill: Dict[str, Any]) -> None:
        """
        Process an assignment fill event. This method extracts and formats the assignment information
        for use in strategies.

        :param fill: The assignment fill data from the WebSocket feed
        """
        try:
            # Extract basic fill information
            instrument = fill["instrument"]
            timestamp_ms = fill["time"]  # Timestamp in milliseconds
            price = Decimal(str(fill["price"]))
            quantity = Decimal(str(fill["qty"]))
            is_buy = fill["buy"]
            fill_id = fill["fill_id"]
            order_id = fill["order_id"]
            
            # Check if this is from a historical snapshot or real-time
            is_historical = "source" in fill and fill.get("source") == "fills_snapshot"
            
            # Convert instrument to trading pair
            trading_pair = kraken_utils.convert_from_exchange_trading_pair(instrument)
            
            # Convert timestamp from milliseconds to seconds for datetime
            timestamp_sec = timestamp_ms / 1000.0 if timestamp_ms > 10000000000 else timestamp_ms
            
            # Format timestamp for display
            try:
                formatted_time = datetime.fromtimestamp(timestamp_sec).strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, OverflowError):
                self.logger().warning(f"Invalid timestamp value: {timestamp_ms}, using current time instead")
                formatted_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Create assignment info dictionary
            assignment_info = {
                "trading_pair": trading_pair,
                "timestamp": timestamp_ms,
                "price": price,
                "quantity": quantity,
                "side": "BUY" if is_buy else "SELL",
                "fill_id": fill_id,
                "order_id": order_id,
                "position_side": PositionSide.LONG if is_buy else PositionSide.SHORT,
                "is_historical": is_historical
            }

            # Log with appropriate prefix
            prefix = "Historical" if is_historical else "Real-time"
            self.logger().info(
                f"\n=== {prefix} Assignment Fill Received ==="
                f"\nTrading Pair: {trading_pair}"
                f"\nSide: {'BUY' if is_buy else 'SELL'}"
                f"\nQuantity: {quantity}"
                f"\nPrice: {price}"
                f"\nTimestamp: {formatted_time}"
                f"\nFill ID: {fill_id}"
                f"\nOrder ID: {order_id}"
            )

            # Only emit event for real-time fills, not historical ones
            if not is_historical:
                self.logger().info("Emitting assignment fill event")
                # Create the event object once
                event = AssignmentFillEvent(
                    timestamp=self.current_timestamp,
                    trading_pair=trading_pair,
                    price=price,
                    amount=quantity,
                    position_side=PositionSide.LONG if is_buy else PositionSide.SHORT,
                    fill_id=fill_id,
                    order_id=order_id,
                )
                # Only emit the MarketEvent enum event
                try:
                    self.trigger_event(MarketEvent.AssignmentFill, event)
                except Exception as e:
                    self.logger().error(f"Error triggering assignment fill event: {e}", exc_info=True)
                    
                # # For backward compatibility, notify listeners directly
                # for listener in self._event_listeners.get("assignment_fill", []):
                #     try:
                #         listener(event)
                #     except Exception as e:
                #         self.logger().error(f"Error in assignment fill listener: {e}", exc_info=True)
            else:
                self.logger().info("Historical assignment fill - not emitting event")
                
        except Exception as e:
            self.logger().error(f"Error processing assignment fill: {str(e)}", exc_info=True)
            self.logger().error(f"Assignment fill data: {fill}")

    def _split_trading_pair(self, trading_pair: str) -> Tuple[str, str]:
        """
        Extract the base and quote currency from a trading pair.
        :param trading_pair: A trading pair in the format BASE-QUOTE
        :return: A tuple of (base_currency, quote_currency)
        """
        try:
            base, quote = trading_pair.split("-")
            return base, quote
        except ValueError:
            # If there's no hyphen, try to infer base and quote from known patterns
            # For BTC/USD trading pairs, Kraken often uses XBTUSD format
            if trading_pair.endswith("USD"):
                if trading_pair.startswith("XBT"):
                    return "BTC", "USD"
                else:
                    # Assume the last 3 characters are the quote currency
                    return trading_pair[:-3], "USD"
            self.logger().warning(f"Could not split trading pair {trading_pair} into base and quote")
            return trading_pair, trading_pair  # Return as fallback


