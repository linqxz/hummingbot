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

    async def set_position_mode(self, position_mode: PositionMode):
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
        Since Kraken only supports one-way positions, this always returns success for ONEWAY mode.
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
            exchange_info_response = await self._api_get(path_url=self.trading_pairs_request_path)

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
        trading_rules_response = await self._api_get(path_url=self.trading_rules_request_path)

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

        # self.logger().info(f"Cancel request data: {data}")

        cancel_result = await self._api_post(
            path_url=CONSTANTS.CANCEL_ACTIVE_ORDER_PATH_URL,
            data=data,
            is_auth_required=True,
            trading_pair=tracked_order.trading_pair,
        )

        # self.logger().info(f"Cancel result: {cancel_result}")

        if cancel_result["result"] != "success":
            error_msg = cancel_result.get("error", "Unknown error")
            if "order not found" in error_msg.lower():
                # self.logger().info(f"Order not found during cancellation - Order ID: {order_id}")
                await self._order_tracker.process_order_not_found(tracked_order.client_order_id)
                # self.logger().info(f"Order state after not found:"
                #                  f"\n  - State: {tracked_order.current_state}"
                #                  f"\n  - Is Done: {tracked_order.is_done}"
                #                  f"\n  - Is Cancelled: {tracked_order.is_cancelled}")
                return True
            self.logger().warning(f"Failed to cancel order {order_id} ({error_msg})")
            raise IOError(f"Error cancelling order: {error_msg}")

        # Process the cancellation response
        cancel_status = cancel_result.get("cancelStatus", {})
        if cancel_status.get("status") == "cancelled":
            # Create an order event message to process
            order_data = cancel_status["orderEvents"][0]["order"]
            order_data["status"] = cancel_status["status"].upper()  # Add the status field and convert to uppercase
            order_event = {
                "order": order_data
            }
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

        if order_result.get("result") != "success":
            error_msg = order_result.get("error", "Unknown error")
            raise IOError(f"Error placing order: {error_msg}")

        # Extract the actual order ID from the response
        exchange_order_id = None
        if "sendStatus" in order_result and "order_id" in order_result["sendStatus"]:
            exchange_order_id = order_result["sendStatus"]["order_id"]
        elif "sendStatus" in order_result and "orderEvents" in order_result["sendStatus"]:
            order_events = order_result["sendStatus"]["orderEvents"]
            if order_events and "order" in order_events[0]:
                exchange_order_id = order_events[0]["order"]["orderId"]

        if not exchange_order_id:
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
                "symbol": exchange_symbol,
                "limit": 200,
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
        self.logger().info("\n=== Updating Order Status via REST ===")
        active_orders: List[InFlightOrder] = list(self.in_flight_orders.values())
        
        tasks = []
        for active_order in active_orders:
            tasks.append(asyncio.create_task(self._request_order_status_data(tracked_order=active_order)))

        raw_responses: List[Dict[str, Any]] = await safe_gather(*tasks, return_exceptions=True)

        for resp, active_order in zip(raw_responses, active_orders):
            if not isinstance(resp, Exception):
                if resp.get("result") == "success":
                    orders = resp.get("orders", [])
                    if orders:
                        # Order found in active orders - process normally
                        self._process_order_event_message({"order": orders[0]})
                    else:
                        # Order not found in active orders - check history as fallback
                        try:
                            self.logger().info(f"Order {active_order.client_order_id} not found in active orders - checking history")
                            history_resp = await self._request_order_history_data(active_order)
                            if history_resp.get("result") == "success" and history_resp.get("orders", []):
                                self.logger().info(f"Found order {active_order.client_order_id} in history")
                                # Use the historical processor since message format is different
                                self._process_historical_order_event_message(history_resp["orders"][0])
                            else:
                                self.logger().info(f"Order {active_order.client_order_id} not found in history - marking as not found")
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
            if flex_account:
                portfolio_value = Decimal(str(flex_account.get("portfolioValue", "0")))
                collateral_value = Decimal(str(flex_account.get("collateralValue", "0")))
                # self.logger().debug(f"Portfolio Value: {portfolio_value}, Collateral Value: {collateral_value}")
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

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                all_fills_response = await self._request_order_fills(order=order)
                fills_data = all_fills_response["result"].get("fills", [])

                if fills_data is not None:
                    for fill_data in fills_data:
                        trade_update = self._parse_trade_update(trade_msg=fill_data, tracked_order=order)
                        trade_updates.append(trade_update)
            except IOError as ex:
                if not self._is_request_exception_related_to_time_synchronizer(request_exception=ex):
                    raise

        return trade_updates

    async def _request_order_fills(self, order: InFlightOrder) -> Dict[str, Any]:
        """Request order fills data from the exchange."""
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        body_params = {
            "orderId": order.exchange_order_id,
            "symbol": exchange_symbol,
        }
        res = await self._api_get(
            path_url=CONSTANTS.USER_TRADE_RECORDS_PATH_URL,
            params=body_params,
            is_auth_required=True,
            trading_pair=order.trading_pair,
        )
        return res

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
            trading_pair=tracked_order.trading_pair,
        )

    async def _request_order_history_data(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        """
        Request order history data from the exchange.
        Filters events to only process those matching our client ID.
        Also fetches and processes fills for filled orders.
        
        :param tracked_order: The InFlightOrder being tracked
        :return: Dictionary containing the order history response
        """
        try:
            self.logger().info(f"\n=== Requesting Order History ===\nLooking for order: {tracked_order.client_order_id}")
            
            # Prepare history request parameters
            history_params = {
                "limit": 1000,  # Limit to last 1000 events
                "sort": "desc"  # Most recent first
            }
            
            self.logger().info(f"Requesting order history with params: {history_params}")

            history_resp = await self._api_get(
                path_url=CONSTANTS.HISTORY_ORDERS_ENDPOINT,
                params=history_params,
                is_auth_required=True,
                trading_pair=tracked_order.trading_pair
            )

            # Check if we have a valid response
            if not isinstance(history_resp, dict) or "elements" not in history_resp:
                self.logger().error(f"Unexpected response format: {type(history_resp)}")
                return {"result": "success", "orders": []}

            # Extract events from the response
            events = history_resp.get("elements", [])
            
            if not events:
                self.logger().info("No events found in response")
                return {"result": "success", "orders": []}

            self.logger().info(f"\n=== Processing {len(events)} events ===")
            self.logger().info(f"Looking for client order ID: {tracked_order.client_order_id}")
            
            # Filter and store matching events
            matching_events = []
            
            for event_data in events:
                event = event_data.get("event", {})
                if not event:
                    continue
                    
                # Extract the inner event type and data
                event_type = next(iter(event)) if event else None
                event_details = event.get(event_type, {}) if event_type else {}
                
                # Skip OrderNotFound events
                if event_type == "OrderNotFound":
                    continue
                
                # Extract order data based on event type
                order_data = None
                if event_type == "OrderPlaced":
                    order_data = event_details.get("order", {})
                elif event_type == "OrderUpdated":
                    order_data = event_details.get("newOrder", {})
                elif event_type == "OrderCancelled":
                    order_data = event_details.get("order", {})
                
                if not order_data:
                    continue
                    
                # Check if this event matches our order's client ID
                client_id = order_data.get("clientId")
                
                self.logger().info(f"\nChecking event match:"
                                 f"\n  Event Client ID: {client_id}"
                                 f"\n  Our Client ID: {tracked_order.client_order_id}")
                
                if client_id == tracked_order.client_order_id:
                    self.logger().info(f"Found matching event: {event_type}")
                    matching_events.append((
                        event_data.get("timestamp", 0),
                        event_type,
                        event_details,
                        order_data
                    ))
            
            # If we found matching events
            if matching_events:
                self.logger().info(f"\n=== Found {len(matching_events)} matching events ===")
                # Sort events by timestamp (most recent first)
                matching_events.sort(key=lambda x: x[0], reverse=True)
                
                # Get the most recent event
                _, event_type, event_details, order_data = matching_events[0]
                
                self.logger().info(f"\nMost recent matching event:"
                                 f"\n  Type: {event_type}"
                                 f"\n  Details: {json.dumps(event_details, indent=2)}"
                                 f"\n  Order Data: {json.dumps(order_data, indent=2)}")
                
                # Determine status based on event type and details
                status = "OPEN"  # Default status
                
                if event_type == "OrderCancelled":
                    status = "CANCELLED"
                elif event_type == "OrderUpdated":
                    reason = event_details.get("reason", "")
                    if reason == "full_fill":
                        status = "FILLED"
                        # For filled orders, fetch and process the fills
                        try:
                            self.logger().info(f"Order is filled, fetching fill information...")
                            fills_response = await self._request_order_fills(tracked_order)
                            fills = fills_response.get("fills", [])
                            if fills:
                                self.logger().info(f"Found {len(fills)} fills for order {tracked_order.client_order_id}")
                                # Process each fill
                                for fill in fills:
                                    self.logger().info(f"Processing fill: {fill}")
                                    self._process_trade_event_message(fill)
                            else:
                                self.logger().warning(f"No fills found for filled order {tracked_order.client_order_id}")
                        except Exception as e:
                            self.logger().error(f"Error fetching fills for filled order: {str(e)}")
                    elif reason in ["cancelled_by_user", "cancelled_by_system", "cancelled_by_trigger"]:
                        status = "CANCELLED"
                    elif Decimal(str(order_data.get("filled", "0"))) > Decimal("0"):
                        status = "PARTIALLY_FILLED"
                elif event_type == "OrderPlaced":
                    if Decimal(str(order_data.get("filled", "0"))) > Decimal("0"):
                        status = "PARTIALLY_FILLED"
                
                self.logger().info(f"\n=== Final Order Status ===\nStatus: {status}")
                
                return {
                    "result": "success",
                    "orders": [{
                        "order": {
                            "orderId": order_data.get("uid"),
                            "cliOrdId": order_data.get("clientId"),
                            "status": status
                        }
                    }]
                }
            
            # If no matching events found
            self.logger().info(f"\n=== No matching events found for order {tracked_order.client_order_id} ===")
            return {"result": "success", "orders": []}

        except Exception as e:
            self.logger().error(f"Error fetching order history: {str(e)}")
            if hasattr(e, 'response'):
                response_text = await e.response.text()
                self.logger().debug(f"Response text: {response_text}")
            return {"result": "success", "orders": []}
        
        finally:
            # Log the current state of all tracked orders
            self.logger().info("\n=== Current Order Tracker State ===")
            
            # Log active orders
            active_orders = self._order_tracker.active_orders
            self.logger().info(f"\nActive Orders ({len(active_orders)}):")
            for order in active_orders.values():
                self.logger().info(
                    f"  - Order ID: {order.client_order_id}"
                    f"\n    Exchange ID: {order.exchange_order_id}"
                    f"\n    Trading Pair: {order.trading_pair}"
                    f"\n    State: {order.current_state}"
                    f"\n    Amount: {order.amount}"
                    f"\n    Executed: {order.executed_amount_base}"
                    f"\n    Price: {order.price}"
                    f"\n    Type: {order.order_type}"
                    f"\n    Is Done: {order.is_done}"
                    f"\n    Is Cancelled: {order.is_cancelled}"
                )
            
            # Log completed orders
            completed_orders = [o for o in self._order_tracker.all_fillable_orders.values() if o.is_done]
            self.logger().info(f"\nCompleted Orders ({len(completed_orders)}):")
            for order in completed_orders:
                self.logger().info(
                    f"  - Order ID: {order.client_order_id}"
                    f"\n    Exchange ID: {order.exchange_order_id}"
                    f"\n    Trading Pair: {order.trading_pair}"
                    f"\n    Final State: {order.current_state}"
                    f"\n    Amount: {order.amount}"
                    f"\n    Executed: {order.executed_amount_base}"
                    f"\n    Price: {order.price}"
                    f"\n    Type: {order.order_type}"
                )
            
            # Log lost orders
            lost_orders = self._order_tracker.lost_orders
            self.logger().info(f"\nLost Orders ({len(lost_orders)}):")
            for order in lost_orders.values():
                self.logger().info(
                    f"  - Order ID: {order.client_order_id}"
                    f"\n    Exchange ID: {order.exchange_order_id}"
                    f"\n    Trading Pair: {order.trading_pair}"
                    f"\n    State: {order.current_state}"
                    f"\n    Amount: {order.amount}"
                    f"\n    Price: {order.price}"
                    f"\n    Type: {order.order_type}"
                )

    def clear_order_history_cache(self):
        """
        Clears the order history cache and resets the last fetch timestamp.
        This can be called when we want to force a fresh fetch of order history.
        """
        self._order_history_cache = {}
        self._last_order_history_fetch_ts = 0
        self.logger().info("Order history cache cleared")
        


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

        async for event_message in self._iter_user_event_queue():
            try:
                # Handle subscription messages
                if event_message.get("event") == "subscribed":
                    feed = event_message.get("feed")
                    if feed:
                        self.logger().debug(f"Successfully subscribed to {feed} feed")
                        confirmed_feeds.add(feed)
                        # Check if all required feeds are confirmed
                        if confirmed_feeds.issuperset(required_feeds):
                            self._user_stream_tracker.data_source._user_stream_event.set()
                            self.logger().info("User stream initialized!")
                    continue
                
                # Process messages based on feed type
                feed = event_message.get("feed")
                if feed == "open_positions":
                    await self._process_account_position_event(event_message)
                elif feed == "open_orders_verbose":
                    self._process_order_event_message(event_message)
                elif feed == "fills":
                    self._process_trade_event_message(event_message)
                elif feed == "balances":
                    self._process_wallet_event_message(event_message)
                elif feed == "notifications_auth":
                    # Only log notifications that contain actual notifications
                    notifications = event_message.get("data", {}).get("notifications", [])
                    if notifications:  # Only log if there are actual notifications
                        self.logger().info(f"Received notifications: {notifications}")
                elif feed == "account_log":
                    self.logger().debug(f"Received account log: {event_message}")
                else:
                    self.logger().debug(f"Received message from unhandled feed: {feed}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop: {str(e)}")
                await self._sleep(5.0)

    async def _process_account_position_event(self, position_msg: Dict[str, Any]):
        """
        Updates position
        :param position_msg: The position event message payload
        """
        try:
            # Extract positions array from the message
            positions = position_msg.get("positions", [])
            if not positions:
                self.logger().debug("No positions in message")
                return

            for position in positions:
                ex_trading_pair = position.get("instrument")
                if not ex_trading_pair:
                    self.logger().error(f"No instrument in position data: {position}")
                    continue

                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=ex_trading_pair)
                
                # For determining position side, check if pnl_currency exists and matches quote asset
                base_asset, quote_asset = trading_pair.split("-")
                position_side = PositionSide.LONG
                if "pnl_currency" in position:
                    position_side = PositionSide.LONG if position["pnl_currency"] == quote_asset else PositionSide.SHORT
                
                amount = Decimal(str(position.get("balance", "0")))
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
                        amount=amount * (Decimal("-1.0") if position_side == PositionSide.SHORT else Decimal("1.0")),
                        leverage=leverage,
                    )
                    self._perpetual_trading.set_position(pos_key, position_instance)
                else:
                    self._perpetual_trading.remove_position(pos_key)

            # Trigger balance update because Kraken doesn't have balance updates through the websocket
            safe_ensure_future(self._update_balances())
        except Exception as e:
            self.logger().error(f"Error processing position event: {str(e)}", exc_info=True)
            self.logger().error(f"Position message: {position_msg}")

    def _process_order_event_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.
        :param order_msg: The order event message payload
        """
        # self.logger().info(f"\n=== Processing Order Event ===\nMessage: {order_msg}")
        
        # Check if this is a historical order update
        if isinstance(order_msg, dict) and "order" in order_msg and isinstance(order_msg["order"], dict) and "order" in order_msg["order"]:
            # self.logger().info("Detected historical order update - forwarding to historical processor")
            return self._process_historical_order_event_message(order_msg["order"])

        # Handle live order updates
        if "order" not in order_msg:
            # self.logger().info("No order data in message")
            return

        order_data = order_msg.get("order", {})
        client_order_id = str(order_data.get("cliOrdId", ""))
        
        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked_order is None:
            # Check if this is a lost order before logging not found
            lost_order = self._order_tracker.lost_orders.get(client_order_id)
            if lost_order is not None:
                # self.logger().info(f"Order {client_order_id} was previously marked as lost - Updating final state")
                pass
            else:
                # self.logger().info(f"Order {client_order_id} not found in order tracker")
                # self.logger().debug(f"Current orders in tracker: {self._order_tracker.all_updatable_orders}")
                pass
            return

        # Get order status
        order_status = order_data.get("status", "UNKNOWN")
        # self.logger().info(f"\nOrder Details:"
        #                   f"\n  - Client Order ID: {client_order_id}"
        #                   f"\n  - Current State: {tracked_order.current_state}"
        #                   f"\n  - New Status: {order_status}"
        #                   f"\n  - Is Done: {tracked_order.is_done}"
        #                   f"\n  - Is Cancelled: {tracked_order.is_cancelled}")

        # Get exchange order ID
        exchange_order_id = str(order_data.get("orderId", ""))

        # Get timestamp
        timestamp = self.current_timestamp

        order_update = OrderUpdate(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=timestamp,
            new_state=CONSTANTS.ORDER_STATE[order_status],
        )

        # self.logger().info(f"\nCreated Order Update:"
        #                   f"\n  - New State: {CONSTANTS.ORDER_STATE[order_status]}"
        #                   f"\n  - Timestamp: {timestamp}")

        self._order_tracker.process_order_update(order_update)

        # self.logger().info(f"\nOrder State After Update:"
        #                   f"\n  - New State: {tracked_order.current_state}"
        #                   f"\n  - Is Done: {tracked_order.is_done}"
        #                   f"\n  - Is Filled: {tracked_order.is_filled}"
        #                   f"\n  - Is Cancelled: {tracked_order.is_cancelled}"
        #                   f"\n  - Final Fills: {tracked_order.executed_amount_base}")

    def _process_historical_order_event_message(self, order_msg: Dict[str, Any]):
        """
        Process order updates from the order history endpoint.
        This is only called as a fallback when an order is not found in active orders.
        """
        # self.logger().info(f"\n=== Processing Historical Order Event (Fallback) ===\nMessage: {order_msg}")
        
        # Extract order data (keeping existing logic since message format is different)
        order_data = None
        if isinstance(order_msg, dict):
            if "order" in order_msg and isinstance(order_msg["order"], dict):
                if "order" in order_msg["order"]:
                    order_data = order_msg["order"]["order"]
                else:
                    order_data = order_msg["order"]
            else:
                order_data = order_msg

        if not order_data:
            self.logger().error(f"Could not extract order data from historical message")
            return

        # Get client order ID
        client_order_id = str(order_data.get("cliOrdId") or order_data.get("clientId", ""))
        if not client_order_id:
            self.logger().error("No client order ID found in historical order data")
            return
        
        # Find the tracked order
        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked_order is None:
            # self.logger().info(f"Order {client_order_id} not found in order tracker")
            return

        # Get exchange order ID and status
        exchange_order_id = str(order_data.get("orderId") or order_data.get("uid", ""))
        order_status = order_data.get("status", "UNKNOWN")
        timestamp = self.current_timestamp

        # self.logger().info(f"\nProcessing Historical Order Update:"
        #                   f"\n  Client Order ID: {client_order_id}"
        #                   f"\n  Current State: {tracked_order.current_state}"
        #                   f"\n  New Status: {order_status}")

        try:
            # Don't update the state if the order is already filled
            if tracked_order.is_filled and tracked_order.is_done:
                # self.logger().info(f"Order {client_order_id} is already filled, skipping state update")
                return

            # Create order update using the CONSTANTS.ORDER_STATE mapping
            order_update = OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=timestamp,
                new_state=CONSTANTS.ORDER_STATE[order_status],
            )

            # self.logger().info(f"\nCreated Historical Order Update:"
            #                   f"\n  New State: {CONSTANTS.ORDER_STATE[order_status]}"
            #                   f"\n  Timestamp: {timestamp}")

            self._order_tracker.process_order_update(order_update)

            # self.logger().info(f"\nOrder State After Historical Update:"
            #                   f"\n  New State: {tracked_order.current_state}"
            #                   f"\n  Is Done: {tracked_order.is_done}"
            #                   f"\n  Is Filled: {tracked_order.is_filled}"
            #                   f"\n  Is Cancelled: {tracked_order.is_cancelled}"
            #                   f"\n  Final Fills: {tracked_order.executed_amount_base}")

        except KeyError as e:
            self.logger().error(f"Invalid order status received: {order_status}. Error: {str(e)}")
        except Exception as e:
            self.logger().error(f"Error processing historical order update: {str(e)}")

    def _process_trade_event_message(self, trade_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers order filled event for trade message received.
        :param trade_msg: The trade event message payload
        """
        if not isinstance(trade_msg, dict):
            self.logger().error(f"Unexpected trade message format: {trade_msg}")
            return

        # Handle both WebSocket (cli_ord_id) and REST (cliOrdId) formats
        client_order_id = str(trade_msg.get("cli_ord_id") or trade_msg.get("cliOrdId", ""))
        # self.logger().info(f"Processing trade event - Order ID: {client_order_id}")

        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
        if tracked_order is None:
            # self.logger().debug(f"Order {client_order_id} not found in order tracker")
            # self.logger().debug(f"Current orders in tracker: {self._order_tracker.all_fillable_orders}")
            return

        # self.logger().info(f"Order before trade update - Order ID: {client_order_id}, State: {tracked_order.current_state}, Filled Amount: {tracked_order.executed_amount_base}/{tracked_order.amount}")

        # Handle both WebSocket (fee_currency) and REST (feeCurrency) formats
        fee_currency = trade_msg.get("fee_currency") or trade_msg.get("feeCurrency", "")
        fee_paid = trade_msg.get("fee_paid") or trade_msg.get("fee", "0")

        # Remove any negative sign from fee currency and ensure it's not None
        fee_currency = fee_currency.replace("-", "") if fee_currency else ""

        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=tracked_order.position,
            percent_token=fee_currency,
            flat_fees=[TokenAmount(amount=Decimal(str(fee_paid)), token=fee_currency)] if fee_currency else []
        )

        # Handle both WebSocket (qty) and REST (size) formats
        fill_size = Decimal(str(trade_msg.get("qty") or trade_msg.get("size", "0")))
        fill_price = Decimal(str(trade_msg.get("price", "0")))
        fill_quote_amount = fill_size * fill_price

        # Handle both WebSocket (fill_id, order_id) and REST (fillId, orderId) formats
        trade_id = str(trade_msg.get("fill_id") or trade_msg.get("fillId", ""))
        exchange_order_id = str(trade_msg.get("order_id") or trade_msg.get("orderId", ""))

        # Handle both WebSocket (time) and REST (fillTime) formats
        timestamp = trade_msg.get("time")
        if timestamp is None:
            fill_time = trade_msg.get("fillTime", "1970-01-01T00:00:00.000Z")
            timestamp = int(datetime.strptime(fill_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1e3)

        trade_update = TradeUpdate(
            trade_id=trade_id,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fee=fee,
            fill_base_amount=fill_size,
            fill_quote_amount=fill_quote_amount,
            fill_price=fill_price,
            fill_timestamp=int(timestamp) * 1e-3,  # Convert milliseconds to seconds
        )

        self._order_tracker.process_trade_update(trade_update)

        # After processing the trade update, check if the order is completely filled
        if tracked_order.is_done and tracked_order.is_filled:
            # Create an order update to set the state to FILLED
            order_update = OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=CONSTANTS.ORDER_STATE["FILLED"]
            )
            self._order_tracker.process_order_update(order_update)

        # self.logger().info(f"Order after trade update - Order ID: {client_order_id}, State: {tracked_order.current_state}, Filled Amount: {tracked_order.executed_amount_base}/{tracked_order.amount}, Is Done: {tracked_order.is_done}, Is Filled: {tracked_order.is_filled}, Is Cancelled: {tracked_order.is_cancelled}")
        # self.logger().info(f"Order completely filled event is set: {tracked_order.completely_filled_event.is_set()}")

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

        flat_fees = [] if fee_amount == Decimal("0") else [TokenAmount(amount=fee_amount, token=fee_currency)]

        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent_token=fee_currency,
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
        # Extract flex_futures data from either snapshot or regular update
        flex_futures_data = None
        if wallet_msg.get("feed") == "balances_snapshot":
            flex_futures_data = wallet_msg.get("flex_futures", {})
        elif "flex_futures" in wallet_msg:
            flex_futures_data = wallet_msg["flex_futures"]

        if flex_futures_data is None:
            self.logger().debug("No flex_futures data in wallet message")
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
            
            self.logger().debug(f"Updated {currency} balance - Total: {total_balance}, Available: {available_balance}")

        # Log portfolio summary
        portfolio_value = Decimal(str(flex_futures_data.get("portfolio_value", 0)))
        collateral_value = Decimal(str(flex_futures_data.get("collateral_value", 0)))
        self.logger().debug(f"Portfolio Value: {portfolio_value}, Collateral Value: {collateral_value}")

    async def _get_max_order_size(self, symbol: str) -> Decimal:
        """
        Fetches the maximum order size for a given symbol from the exchange.
        :param symbol: The exchange symbol to get max order size for
        :returns: The maximum order size as a Decimal
        """
        try:
            params = {
                "orderType": "mkt",
                "symbol": symbol,
            }

            max_order_info = await self._api_get(
                path_url=CONSTANTS.MAX_ORDER_SIZE_ENDPOINT,
                params=params,
                method=RESTMethod.GET,
                is_auth_required=True
            )

            if max_order_info.get("result") == "success":
                # Get raw values and handle None cases
                max_buy_size_raw = max_order_info.get("maxBuySize")
                max_sell_size_raw = max_order_info.get("maxSellSize")

                # Convert to Decimal, using 0 for None values
                max_buy_size = Decimal(str(max_buy_size_raw)) if max_buy_size_raw is not None else Decimal("0")
                max_sell_size = Decimal(str(max_sell_size_raw)) if max_sell_size_raw is not None else Decimal("0")

                # If both are 0, return 0
                if max_buy_size == Decimal("0") and max_sell_size == Decimal("0"):
                    return Decimal("0")

                # If one is 0, return the other
                if max_buy_size == Decimal("0"):
                    return max_sell_size
                if max_sell_size == Decimal("0"):
                    return max_buy_size

                # If both have values, return the minimum
                return min(max_buy_size, max_sell_size)
            else:
                error_msg = max_order_info.get("error", "Unknown error")
                self.logger().warning(f"Error getting max order size for {symbol}: {error_msg}")
                return Decimal("0")
        except Exception as e:
            self.logger().error(f"Error fetching max order size for {symbol}: {str(e)}", exc_info=True)
            return Decimal("0")

    async def _format_trading_rules(self, instruments_info: List[Dict[str, Any]]) -> List[TradingRule]:
        """
        Format the trading rules response from the exchange into TradingRule instances.
        :param instruments_info: The list of instruments information from the exchange
        :returns: A list of TradingRule instances
        """

        trading_rules = {}
        symbol_map = await self.trading_pair_symbol_map()

        # Process only the tradeable instruments that are in our symbol map
        valid_instruments = [
            instrument for instrument in instruments_info
            if instrument.get("symbol").startswith("PF_") and instrument.get("symbol").endswith("USD") and instrument["symbol"] in symbol_map
        ]


        for instrument in valid_instruments:
            try:
                exchange_symbol = instrument["symbol"]
                trading_pair = symbol_map[exchange_symbol]

                # Skip if we already have a trading rule for this pair
                if trading_pair in trading_rules:
                    self._logger.info(f"Trading rule already exists for {trading_pair}, skipping...")
                    continue

                # Calculate min_order_size based on contractValueTradePrecision
                precision_str = str(instrument["contractValueTradePrecision"])
                try:
                    precision = int(float(precision_str))  # Handles both int and float strings
                except ValueError:
                    self._logger.error(f"Error parsing precision for {exchange_symbol}: {precision_str}")
                    continue

                min_order_size = Decimal(str("1" + "0" * abs(precision))) if precision < 0 else Decimal(
                    ("0." + "0" * (precision - 1) + "1") if precision > 0 else "1")

                # Get max position size from instrument info
                max_position_size = Decimal(str(instrument.get("maxPositionSize", "0")))

                # Get max order size from API
                max_order_size = await self._get_max_order_size(exchange_symbol)

                # If max_order_size is 0 (API error or limit not available), fallback to max_position_size
                if max_order_size == Decimal("0"):
                    self._logger.info(f"Using max position size as fallback: {max_position_size}")
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

            except Exception as e:
                self.logger().error(f"Error parsing trading rule for instrument: {instrument}. Error: {str(e)}")
                continue

        return list(trading_rules.values())

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        """Initialize trading pair symbols from exchange info."""
        try:
            mapping = bidict()

            # exchange_info is already a list of instruments
            for symbol_data in filter(kraken_utils.is_exchange_information_valid, exchange_info):
                exchange_symbol = symbol_data["symbol"]
                # For PF_XBTUSD, we want to extract XBT and USD
                base = exchange_symbol[3:].replace("USD", "")  # Remove PF_ prefix and USD suffix
                quote = "USD"

                # Convert XBT to BTC if needed
                if base == "XBT":
                    base = "BTC"

                trading_pair = f"{base}-{quote}"
                mapping[exchange_symbol] = trading_pair

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

        self.logger().info(f"Could not resolve the exchange symbols {new_exchange_symbol} and {current_exchange_symbol}")

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        params = {"symbol": exchange_symbol}

        resp_json = await self._api_get(
            path_url=CONSTANTS.TICKER_PRICE_ENDPOINT,
            params=params,
        )

        price = float(resp_json["tickers"]["last"])
        return price

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
                params={"symbol": exchange_symbol},  # Removed count parameter since it doesn't work
                is_auth_required=False
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
        if trade_type not in [TradeType.BUY, TradeType.SELL]:
            raise NotImplementedError(f"Unsupported trade type {trade_type}")
        if position_action not in [PositionAction.OPEN, PositionAction.CLOSE]:
            raise NotImplementedError(f"Unsupported position action {position_action}")
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
            self.logger().info(f"Waiting for: {', '.join(not_ready)}")

        return status_d

