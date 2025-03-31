import asyncio
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.assignment_executor.data_types import AssignmentExecutorConfig
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class AssignmentExecutor(ExecutorBase):
    """
    Executor for handling positions received through assignments.
    Supports both market and limit orders.
    """
    _logger = None
    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = HummingbotLogger(__name__)
        return cls._logger
    
    def __init__(self, strategy: ScriptStrategyBase, config: AssignmentExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        """
        Initialize the AssignmentExecutor instance.

        :param strategy: The strategy to be used by the AssignmentExecutor.
        :param config: The configuration for the AssignmentExecutor, subclass of AssignmentExecutorConfig.
        :param update_interval: The interval at which the AssignmentExecutor should be updated, defaults to 1.0.
        :param max_retries: The maximum number of retries for the AssignmentExecutor, defaults to 5.
        """

        if config.triple_barrier_config.time_limit_order_type != OrderType.MARKET or \
                config.triple_barrier_config.stop_loss_order_type != OrderType.MARKET:
            error = "Only market orders are supported for time_limit and stop_loss"
            self.logger().error(error)
            raise ValueError(error)

        super().__init__(strategy=strategy, config=config, connectors=[config.connector_name],
                         update_interval=update_interval)

        self.config: AssignmentExecutorConfig = config
        self.trading_rules = self.get_trading_rules(self.config.connector_name, self.config.trading_pair)

        # Order tracking
        self._open_order: Optional[TrackedOrder] = None
        self._take_profit_limit_order: Optional[TrackedOrder] = None
        self._failed_orders: List[TrackedOrder] = []
        self._trailing_stop_trigger_pct: Optional[Decimal] = None

        # Assignment tracking
        self._assigned_amount: Decimal = config.amount  # The amount from the assignment
        self._entry_price: Decimal = config.entry_price  # The price from the assignment
        
        # IMPROVED ASSIGNMENT ID TRACKING
        # Make sure we get a valid assignment_id from the config
        
        # Store the assignment ID directly from config or extract it from relevant sources
        self._assignment_id = None
        assignment_id_found = False
        assignment_id_sources = []
        
        # Detailed logging about the config object for debugging
        self.logger().info(f"AssignmentExecutor initialization - Config type: {type(config)}")
        self.logger().info(f"AssignmentExecutor initialization - Config ID: {getattr(config, 'id', 'Not found')}")
        
        # Try all possible sources for assignment_id
        possible_attrs = ['assignment_id', 'assignmentid', 'fill_id', 'fillid', 'id', 'controller_id']
        for attr in possible_attrs:
            if hasattr(config, attr):
                value = getattr(config, attr)
                assignment_id_sources.append((attr, value))
                if value and not assignment_id_found and attr in ['assignment_id', 'assignmentid', 'fill_id', 'fillid']:
                    self._assignment_id = value
                    assignment_id_found = True
                    self.logger().info(f"AssignmentExecutor {config.id} - Found assignment_id in {attr}: {value}")
        
        # If not found in primary sources, check controller_id or id
        if not assignment_id_found:
            for attr, value in assignment_id_sources:
                if value and attr in ['controller_id', 'id'] and not self._assignment_id:
                    self._assignment_id = value 
                    self.logger().info(f"AssignmentExecutor {config.id} - Using {attr} as fallback assignment_id: {value}")
                    break
        
        # Final fallback to executor ID if nothing else is available
        if not self._assignment_id and hasattr(config, 'id'):
            self._assignment_id = config.id
            self.logger().warning(f"AssignmentExecutor {config.id} - No assignment_id sources found, using executor ID: {config.id}")
        
        # Log all attribute sources for debugging
        self.logger().info(f"AssignmentExecutor {config.id} - Assignment ID sources examined: {assignment_id_sources}")
        self.logger().info(f"AssignmentExecutor {config.id} - Final assignment_id selected: {self._assignment_id}")
        
        # Update the config to ensure consistency
        if self._assignment_id and hasattr(config, 'assignment_id'):
            if config.assignment_id != self._assignment_id:
                self.logger().info(f"AssignmentExecutor {config.id} - Updating config.assignment_id from {config.assignment_id} to {self._assignment_id}")
                config.assignment_id = self._assignment_id
        
        # Barrier tracking
        self._close_order: Optional[TrackedOrder] = None
        self._stop_loss_order: Optional[TrackedOrder] = None
        self._take_profit_order: Optional[TrackedOrder] = None
        self._trailing_stop_order: Optional[TrackedOrder] = None
        
        # Executor state
        self._total_executed_amount_backup: Decimal = Decimal("0")
        self._current_retries = 0
        self._max_retries = max_retries

        self.close_timestamp = None
        self.close_type = None

        self.logger().info(f"Initialized AssignmentExecutor for {self.config.trading_pair}: " +
                          f"{self.config.side.name} {self.config.amount} @ {self.config.entry_price} " +
                          f"using {self.config.order_type.name} order to {self.config.position_action.name} position" +
                          (f" (assignment_id: {self._assignment_id})" if self._assignment_id else ""))

    @property
    def is_perpetual(self) -> bool:
        """Check if this is a perpetual futures connector."""
        return self.is_perpetual_connector(self.config.connector_name)
    
    @property
    def is_closed(self) -> bool:
        """Returns True if the executor has completed its task."""
        return self.status == RunnableStatus.TERMINATED
    
    @property
    def is_trading(self) -> bool:
        """Returns True if the executor is actively trading."""
        return self.status == RunnableStatus.RUNNING and self.filled_amount > Decimal("0")
    
    @property
    def assigned_amount(self) -> Decimal:
        """Get the amount that was assigned to this executor."""
        self.logger().debug(f"AssignmentExecutor {self.config.id} - Current assigned amount: {self._assigned_amount}")
        return self._assigned_amount

    @property
    def filled_amount(self) -> Decimal:
        """
        Get the filled amount of the position.
        """
        return self.open_filled_amount + self.close_filled_amount

    @property
    def filled_amount_quote(self) -> Decimal:
        """
        Get the filled amount of the position in quote currency.
        """
        return self.open_filled_amount_quote + self.close_filled_amount_quote

    # @property
    # def is_expired(self) -> bool:
    #     """
    #     Check if the position is expired.
    #
    #     :return: True if the position is expired, False otherwise.
    #     """
    #     return self.end_time and self.end_time <= self._strategy.current_timestamp
    #
    # @property
    # def end_time(self) -> Optional[float]:
    #     """
    #     Calculate the end time of the position based on the time limit
    #
    #     :return: The end time of the position.
    #     """
    #     if not self.config.triple_barrier_config.time_limit:
    #         return None
    #     return self.config.timestamp + self.config.triple_barrier_config.time_limit
    
    def get_cum_fees_quote(self) -> Decimal:
        """
        Calculate the cumulative fees in quote asset
        :return: The cumulative fees in quote asset.
        """
        # Check if we have a recent cached value (within last 10 seconds)
        current_time = time.time()
        if hasattr(self, '_cached_fees') and hasattr(self, '_cached_fees_timestamp'):
            if current_time - self._cached_fees_timestamp < 10:  # Cache for 10 seconds
                return self._cached_fees
        
        # For more efficient logging, only log details occasionally
        should_log_details = (not hasattr(self, '_last_fee_calculation_log') or 
                             (current_time - self._last_fee_calculation_log > 300))  # Every 5 minutes
        
        if should_log_details:
            self._last_fee_calculation_log = current_time
            self.logger().debug(f"AssignmentExecutor {self.config.id} - Calculating cumulative fees")
        
        orders = [self._open_order, self._close_order]
        quote_asset = self.config.trading_pair.split("-")[1]
        
        # Get the connector instance to pass to the fee calculation method
        connector = self.connectors.get(self.config.connector_name)
        if not connector and should_log_details:
            self.logger().warning(f"AssignmentExecutor {self.config.id} - Connector {self.config.connector_name} not found for fee calculation")
        
        total_fees = Decimal("0")
        for idx, order in enumerate(orders):
            if order and hasattr(order, 'order') and order.order:
                if should_log_details:
                    order_type = "Open" if idx == 0 else "Close"
                    self.logger().debug(f"AssignmentExecutor {self.config.id} - Getting fees for {order_type} order {order.order_id}")
                
                # Only calculate fees in the quote currency
                if hasattr(order.order, 'cumulative_fee_paid'):
                    # Ensure we're explicitly using the quote currency from the trading pair
                    order_fees = order.order.cumulative_fee_paid(token=quote_asset, exchange=connector)
                    total_fees += order_fees
        
        # Cache the result with longer validity to minimize recalculations
        self._cached_fees = total_fees
        self._cached_fees_timestamp = current_time
        
        if should_log_details:
            self.logger().debug(f"AssignmentExecutor {self.config.id} - Total cumulative fees: {total_fees} {quote_asset}")
        
        return total_fees
        
    @property
    def cum_fees_quote(self) -> Decimal:
        """Throttled property to get cumulative fees - use this instead of get_cum_fees_quote() directly"""
        # Use the cached result from get_cum_fees_quote
        return self.get_cum_fees_quote()

    @property
    def current_market_price(self) -> Decimal:
        """Get current market price for reference."""
        price_type = PriceType.BestBid if self.config.side == TradeType.BUY else PriceType.BestAsk
        return self.get_price(self.config.connector_name, self.config.trading_pair, price_type=price_type)

    @property
    def entry_price(self) -> Decimal:
        """Get entry price - either from filled order or config."""
        return self.config.entry_price

    @property
    def close_price(self) -> Decimal:
        """Get close price if order is done, otherwise current market price."""
        if self._close_order and self._close_order.is_done:
            return self._close_order.average_executed_price
        else:
            return self.current_market_price

    @property
    def trade_pnl_pct(self) -> Decimal:
        """Calculate trade PnL percentage."""
        if self.filled_amount != Decimal("0") and self.close_type != CloseType.FAILED:
            if self.config.side == TradeType.BUY:
                return (self.close_price - self.entry_price) / self.entry_price
            else:
                return (self.entry_price - self.close_price) / self.entry_price
        else:
            return Decimal("0")

    @property
    def trade_pnl_quote(self) -> Decimal:
        """Calculate trade PnL in quote currency."""
        return self.trade_pnl_pct * self.filled_amount * self.entry_price

    def get_net_pnl_quote(self) -> Decimal:
        """Calculate net PNL in quote currency."""
        # Use cached property to avoid frequent recalculations 
        fees_quote = self.cum_fees_quote
        
        # Only log occasionally
        current_time = time.time()
        if not hasattr(self, '_last_pnl_calc_log') or (current_time - self._last_pnl_calc_log > 300):
            self._last_pnl_calc_log = current_time
            self.logger().debug(f"AssignmentExecutor {self.config.id} - PNL calculation with fees: {fees_quote}")
        
        return self.trade_pnl_quote - fees_quote

    def get_net_pnl_pct(self) -> Decimal:
        """Calculate net PnL percentage."""
        return self.get_net_pnl_quote() / (self.filled_amount * self.entry_price) if self.filled_amount > 0 else Decimal("0")

    async def control_shutdown(self):
        """Control the shutdown process.
        :return: None"""

        self.close_timestamp = self._strategy.current_timestamp
        order_execution_completed = self.open_and_close_volume_match()
        if order_execution_completed:
            self.stop()
        else:
            await self.control_close_order()
            self._current_retries += 1
        await self._sleep(5.0)

    async def control_close_order(self):
        """
        This method is responsible for controlling the close order. If the close order is filled and the open orders are
        completed, it stops the executor. If the close order is not placed, it places the close order. If the close order
        is not filled, it waits for the close order to be filled and requests the order information to the connector.
        """
        self.logger().info(f"AssignmentExecutor {self.config.id} - control_close_order called. Close order state: {'Present' if self._close_order else 'Not present'}")
        
        # First check if the position is already fully closed
        if self.open_and_close_volume_match() or self.close_filled_amount >= self._assigned_amount:
            self.logger().info(f"AssignmentExecutor {self.config.id} - Position already fully closed. No need for additional close orders.")
            # Always ensure we're in the terminated state and call stop to signal completion
            self._status = RunnableStatus.TERMINATED
            self.close_timestamp = self._strategy.current_timestamp
            self.stop()
            return
            
        # Verify with exchange if position still exists
        try:
            exchange = self.connectors[self.config.connector_name]
            connector = self.connectors[self.config.connector_name]

            positions = exchange.account_positions
            position_found = False
            
            for pos in positions:
                if pos.trading_pair == self.config.trading_pair:
                    position_size = abs(pos.amount)
                    if position_size > 0:
                        position_found = True
                        self.logger().info(f"AssignmentExecutor {self.config.id} - Found active position on exchange: {position_size} {self.config.trading_pair}")
                        break
            
            if not position_found:
                self.logger().info(f"AssignmentExecutor {self.config.id} - No active position found on exchange. Marking as completed.")
                self.close_timestamp = self._strategy.current_timestamp
                self._status = RunnableStatus.TERMINATED  # Set directly to TERMINATED
                self.stop()
                return
        except Exception as e:
            self.logger().error(f"AssignmentExecutor {self.config.id} - Error checking position status: {e}")
            
        # Log the attempt to place a close order with specific amount
        crypto_symbol = self.config.trading_pair.split('-')[0]  # Extract BTC from BTC-USD
        self.logger().info(f"AssignmentExecutor {self.config.id} - Preparing to place close order for {self.amount_to_close} {crypto_symbol}")
        
        # Warning if amount to close is too small or zero
        if self.amount_to_close <= Decimal("0"):
            self.logger().warning(f"AssignmentExecutor {self.config.id} - Amount to close is {self.amount_to_close}, which is zero or negative. Not placing close order.")
            return
            
        if hasattr(self, 'trading_rules') and self.amount_to_close < self.trading_rules.min_order_size:
            self.logger().warning(f"AssignmentExecutor {self.config.id} - Amount to close {self.amount_to_close} is less than min order size {self.trading_rules.min_order_size}. Order may be rejected.")

        # Final check - if filled amount is already >= assigned amount, the position appears to be fully closed
        if self.close_filled_amount >= self._assigned_amount:
            self.logger().info(f"AssignmentExecutor {self.config.id} - Position appears fully closed. Filled amount {self.close_filled_amount} >= assigned amount {self._assigned_amount}. Stopping executor.")
            self._status = RunnableStatus.TERMINATED
            self.stop()
            return

        if self._close_order:
            in_flight_order = self.get_in_flight_order(self.config.connector_name,
                                                       self._close_order.order_id) if not self._close_order.order else self._close_order.order
            if in_flight_order:
                self.logger().info(f"AssignmentExecutor {self.config.id} - Found in-flight order: {in_flight_order.client_order_id}. State: {in_flight_order.current_state}")
                self._close_order.order = in_flight_order
                self.logger().info(f"AssignmentExecutor {self.config.id} - Updating order with connector: {self.config.connector_name}")
                await connector._update_orders_with_error_handler(
                    orders=[in_flight_order],
                    error_handler=connector._handle_update_error_for_lost_order)
                self.logger().info(f"AssignmentExecutor {self.config.id} - Close order status: {in_flight_order.current_state}, Filled: {in_flight_order.executed_amount_base}/{in_flight_order.amount}")
                
                # If the order is done, check if we've fully closed the position
                if in_flight_order.is_done:
                    self.logger().info(f"AssignmentExecutor {self.config.id} - Close order is done. Checking if position is fully closed.")
                    if self.open_and_close_volume_match() or self.close_filled_amount >= self._assigned_amount:
                        self.logger().info(f"AssignmentExecutor {self.config.id} - Position fully closed with completed order. Stopping executor.")
                        self.close_timestamp = self._strategy.current_timestamp
                        self.stop()
                    elif in_flight_order.executed_amount_base > 0 and in_flight_order.executed_amount_base < in_flight_order.amount:
                        # Partial fill - we need to place another order for the remaining amount
                        self.logger().info(f"AssignmentExecutor {self.config.id} - Order partially filled. Need to place another order.")
                        self._close_order = None  # Reset close order so we place a new one
            else:
                self.logger().warning(f"AssignmentExecutor {self.config.id} - Close order {self._close_order.order_id} not found in flight orders. Marking as failed.")
                self._failed_orders.append(self._close_order)
                self._close_order = None
        else:
            self.logger().info(f"AssignmentExecutor {self.config.id} - No close order found. Checking amount to close: {self.amount_to_close}")
            # Double check exchange position status before placing a new order
            try:
                exchange = self.connectors[self.config.connector_name]
                positions = exchange.account_positions
                position_found = False
                
                for pos in positions:
                    if pos.trading_pair == self.config.trading_pair:
                        position_size = abs(pos.amount)
                        if position_size > 0:
                            position_found = True
                            self.logger().info(f"AssignmentExecutor {self.config.id} - Verified active position on exchange before placing order: {position_size} {self.config.trading_pair}")
                            break
                
                if not position_found:
                    self.logger().info(f"AssignmentExecutor {self.config.id} - No active position found on exchange before placing order. Stopping executor.")
                    self.close_timestamp = self._strategy.current_timestamp
                    self._status = RunnableStatus.SHUTTING_DOWN
                    self.stop()
                    return
                    
            except Exception as e:
                self.logger().error(f"AssignmentExecutor {self.config.id} - Error verifying position before placing order: {str(e)}", exc_info=True)
                # Continue with caution if exchange check fails
            
            # Check if there's still an amount to close before calling place_close_order_and_cancel_open_orders
            if self.amount_to_close > Decimal("0") and self.amount_to_close >= (self.trading_rules.min_order_size if hasattr(self, 'trading_rules') else Decimal("0")):
                self.logger().info(f"AssignmentExecutor {self.config.id} - Placing new close order for {self.amount_to_close} {self.config.trading_pair.split('-')[0]}")
                self.place_close_order_and_cancel_open_orders(close_type=self.close_type)
            else:
                self.logger().warning(f"AssignmentExecutor {self.config.id} - Amount to close ({self.amount_to_close}) is too small or zero. Not placing close order.")
                # If there's nothing to close, we should stop the executor
                if self.close_filled_amount >= self._assigned_amount:
                    self.logger().info(f"AssignmentExecutor {self.config.id} - Position appears to be fully closed. Stopping executor.")
                    self.close_timestamp = self._strategy.current_timestamp
                    self.stop()

    def evaluate_max_retries(self):
        """Check if max retries reached and update status.
        :return: None
        """
        if self._current_retries >= self.config.max_retries:
            self.close_type = CloseType.FAILED
            self.stop()

    def _is_within_activation_bounds(self, order_price: Decimal, side: TradeType, order_type: OrderType) -> bool:
        """
        This method is responsible for checking if the close price is within the activation bounds to place the open
        order. If the activation bounds are not set, it returns True. This makes the executor more capital efficient.

        :param close_price: The close price to be checked.
        :return: True if the close price is within the activation bounds, False otherwise.
        """
        activation_bounds = self.config.activation_bounds
        mid_price = self.get_price(self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        if activation_bounds:
            if order_type.is_limit_type():
                if side == TradeType.BUY:
                    return order_price >= mid_price * (1 - activation_bounds[0])
                else:
                    return order_price <= mid_price * (1 + activation_bounds[0])
            else:
                if side == TradeType.BUY:
                    min_price_to_buy = order_price * (1 - activation_bounds[0])
                    max_price_to_buy = order_price * (1 + activation_bounds[1])
                    return min_price_to_buy <= mid_price <= max_price_to_buy
                else:
                    min_price_to_sell = order_price * (1 - activation_bounds[1])
                    max_price_to_sell = order_price * (1 + activation_bounds[0])
                    return min_price_to_sell <= mid_price <= max_price_to_sell
        else:
            return True
    
    def calculate_limit_price(self) -> Decimal:
        """Calculate limit price with slippage buffer."""
        multiplier = (Decimal("1") + self.config.slippage_buffer) if self.config.side == TradeType.BUY \
            else (Decimal("1") - self.config.slippage_buffer)
        return self.config.entry_price * multiplier

    @property
    def take_profit_price(self):
        """
        This method is responsible for calculating the take profit price to place the take profit limit order.

        :return: The take profit price.
        """
        if self.config.side == TradeType.BUY:
            take_profit_price = self.entry_price * (1 + self.config.triple_barrier_config.take_profit)
            if self.config.triple_barrier_config.take_profit_order_type == OrderType.LIMIT_MAKER:
                take_profit_price = max(take_profit_price,
                                        self.get_price(self.config.connector_name, self.config.trading_pair,
                                                       PriceType.BestAsk))
            else:
                take_profit_price = self.entry_price * (1 - self.config.triple_barrier_config.take_profit)
            if self.config.triple_barrier_config.take_profit_order_type == OrderType.LIMIT_MAKER:
                take_profit_price = min(take_profit_price,
                                        self.get_price(self.config.connector_name, self.config.trading_pair,
                                                       PriceType.BestBid))
            return take_profit_price

    def update_tracked_orders_with_order_id(self, order_id: str):
        """
        This method is responsible for updating the tracked orders with the information from the InFlightOrder, using
        the order_id as a reference.

        :param order_id: The order_id to be used as a reference.
        :return: None
        """
        in_flight_order = self.get_in_flight_order(self.config.connector_name, order_id)
        if self._open_order and self._open_order.order_id == order_id:
            self._open_order.order = in_flight_order
        elif self._close_order and self._close_order.order_id == order_id:
            self._close_order.order = in_flight_order
        elif self._take_profit_limit_order and self._take_profit_limit_order.order_id == order_id:
            self._take_profit_limit_order.order = in_flight_order

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        """Handle order created event."""
        self.update_tracked_orders_with_order_id(event.order_id)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        """
        This method is responsible for processing the order completed event. Here we will check if the id is one of the
        tracked orders and update the state
        """
        self._total_executed_amount_backup += event.base_asset_amount
        self.update_tracked_orders_with_order_id(event.order_id)

        if self._take_profit_limit_order and self._take_profit_limit_order.order_id == event.order_id:
            self.close_type = CloseType.TAKE_PROFIT
            self._close_order = self._take_profit_limit_order
            self._status = RunnableStatus.SHUTTING_DOWN

        self.logger().info(f"AssignmentExecutor {self.config.id} - Order completed: {event.order_id}, Side: {event.order_type}, Base amount: {event.base_asset_amount}, Quote amount: {event.quote_asset_amount}")

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        """
        Handle order filled event. Updates the tracked order and amounts.
        For assignments, we track closing amounts separately.
        """
        # Update the tracked orders first
        self.update_tracked_orders_with_order_id(event.order_id)
        
        fill_price = event.price
        fill_amount = event.amount
        
        # Log fill event for debugging but don't add too much noise
        self.logger().debug(f"AssignmentExecutor {self.config.id} - Fill event received: {event.order_id}")
        
        # Capture the fee in its original currency without trying to convert it
        fee_details = []
        if event.trade_fee and event.trade_fee.flat_fees:
            for flat_fee in event.trade_fee.flat_fees:
                fee_details.append(f"{flat_fee.amount} {flat_fee.token}")
                self.logger().debug(f"Fee captured in original currency: {flat_fee.amount} {flat_fee.token}")
        
        # Track the fill for close orders (market orders)
        if self._close_order and event.order_id == self._close_order.order_id:
            self.logger().info(f"AssignmentExecutor {self.config.id} - Close order filled: {fill_amount} @ {fill_price}")
            
            # Calculate total filled so far
            total_filled = self.close_filled_amount
            self.logger().info(f"AssignmentExecutor {self.config.id} - Total filled so far: {total_filled} of {self._assigned_amount}")
            
            # Check if position is fully closed
            if self.open_and_close_volume_match() or total_filled >= self._assigned_amount:
                self.logger().info(f"AssignmentExecutor {self.config.id} - Position fully closed after this fill. Marking as completed.")
                # Set the status to shutting down, not directly to terminated
                self._status = RunnableStatus.SHUTTING_DOWN
                self.close_timestamp = self._strategy.current_timestamp
        else:
            self.logger().info(f"AssignmentExecutor {self.config.id} - Order {event.order_id} filled: {fill_amount} @ {fill_price}")

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        """
        This method is responsible for processing the order canceled event
        """
        if self._close_order and event.order_id == self._close_order.order_id:
            self._failed_orders.append(self._close_order)
            self._close_order = None
        elif self._take_profit_limit_order and event.order_id == self._take_profit_limit_order.order_id:
            self._failed_orders.append(self._take_profit_limit_order)
            self._take_profit_limit_order = None

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        """
        Process order failed event.
        
        :param _: Not used.
        :param market: The market where the event occurred.
        :param event: The event that was triggered.
        """
        self.logger().error(f"AssignmentExecutor {self.config.id} - Order failed event: {event.order_id}")
        self.logger().error(f"AssignmentExecutor {self.config.id} - Order failure details: type={event.order_type}, reason={event.error_description if hasattr(event, 'error_description') else 'Unknown'}")
        
        # Log additional diagnostic information
        self.logger().error(f"AssignmentExecutor {self.config.id} - Failed order context: trading_pair={self.config.trading_pair}, connector={self.config.connector_name}, side={self.close_order_side}")
        
        # Check exchange positions at time of failure
        try:
            exchange = self.connectors[self.config.connector_name]
            positions = exchange.account_positions
            self.logger().info(f"AssignmentExecutor {self.config.id} - Current positions at time of failure: {positions}")
            
            # Check for specific position
            position_found = False
            for pos in positions:
                if pos.trading_pair == self.config.trading_pair:
                    position_found = True
                    self.logger().info(f"AssignmentExecutor {self.config.id} - Found position for {self.config.trading_pair}: amount={pos.amount}, position_side={pos.position_side}")
            
            if not position_found:
                self.logger().warning(f"AssignmentExecutor {self.config.id} - No position found for {self.config.trading_pair} at time of failure - this may explain the failure")
        except Exception as e:
            self.logger().error(f"AssignmentExecutor {self.config.id} - Error checking positions during failure handling: {str(e)}", exc_info=True)
        
        # Check if this is our close order
        if self._close_order and self._close_order.order_id == event.order_id:
            self.logger().error(f"AssignmentExecutor {self.config.id} - Failed order was our close order. Marking as failed and will retry.")
            self._failed_orders.append(self._close_order)
            self._close_order = None
            self._current_retries += 1
        
        # Check if we've exceeded max retries
        self.evaluate_max_retries()

    def get_custom_info(self) -> Dict[str, Any]:
        """Get custom information about this executor."""
        # Start with the executor ID itself as a fallback value
        # This ensures we always have something to use for assignment_id and fill_id
        executor_id = getattr(self.config, 'id', None)
        if executor_id and executor_id.startswith("pending_"):
            executor_id = executor_id.replace("pending_", "")
        
        # Try to get assignment_id from stored value or config
        assignment_id = self._assignment_id or getattr(self.config, 'assignment_id', None)
        
        # If assignment_id is still not found, use executor_id as fallback
        if not assignment_id:
            assignment_id = executor_id
            self.logger().warning(f"AssignmentExecutor {self.config.id} - No assignment_id found, using executor_id: {executor_id}")
        
        # Clean up the assignment_id if it has pending_ prefix
        if isinstance(assignment_id, str) and assignment_id.startswith("pending_"):
            assignment_id = assignment_id.replace("pending_", "")
            self.logger().info(f"AssignmentExecutor {self.config.id} - Removed pending_ prefix from assignment_id: {assignment_id}")
        
        # Explicitly store the cleaned assignment_id back to the instance
        self._assignment_id = assignment_id
        
        # Ensure this value is also stored in the config
        if hasattr(self.config, 'assignment_id'):
            self.config.assignment_id = assignment_id
        
        # Log the final values we'll use
        self.logger().info(f"AssignmentExecutor {self.config.id} - Using assignment_id={assignment_id} for syncing")
        
        info = {
            # Always include these fields, using executor_id as absolute fallback
            "assignment_id": assignment_id or executor_id,
            "fill_id": assignment_id or executor_id,
            "executor_id": executor_id,
            "trading_pair": self.config.trading_pair,
            "side": self.config.side,
            "current_retries": self._current_retries,
            "max_retries": self.config.max_retries,
            "assigned_amount": str(self._assigned_amount),
            "closing_amount": str(self.amount_to_close),
            "cum_fees": str(self.cum_fees_quote),
            "close_type": self.close_type.name if hasattr(self, 'close_type') and self.close_type else None,
        }
        
        # Add time limit info if available
        if self.config.triple_barrier_config and self.config.triple_barrier_config.time_limit:
            info["time_barrier"] = {
                "time_limit": self.config.triple_barrier_config.time_limit,
                "remaining_time": max(0, self.end_time - time.time()) if self.end_time else None,
                "is_expired": self.is_expired,
            }
        
        # For debugging, log the complete info dictionary
        self.logger().info(f"AssignmentExecutor {self.config.id} - Returning custom info with assignment_id={info['assignment_id']}, fill_id={info['fill_id']}")
        
        return info
        
    def set_completed(self):
        """
        Mark this executor as completed to prevent further order placements.
        This sets the executor to the shutting down state, which will
        trigger the shutdown process in the control_task.
        """
        self.logger().info(f"AssignmentExecutor {self.config.id} - Setting executor to completed state (assignment_id: {self._assignment_id})")
        self._is_closed = True
        self.cancel_all_orders()
        self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN

    async def _sleep(self, delay: float):
        """
        This method is responsible for sleeping the executor for a specific time.

        :param delay: The time to sleep.
        :return: None
        """
        await asyncio.sleep(delay)

    async def on_start(self):
        """Initialize the executor."""
        try:
            self.logger().info(f"AssignmentExecutor {self.config.id} on_start called - starting initialization")
            
            # ENHANCED ASSIGNMENT ID EXTRACTION
            # Check if we already have an assignment_id from __init__
            if not self._assignment_id:
                self.logger().warning(f"AssignmentExecutor {self.config.id} - No assignment_id set from __init__, attempting recovery")
                
                # 1. Try config.assignment_id
                if hasattr(self.config, 'assignment_id') and self.config.assignment_id:
                    self._assignment_id = self.config.assignment_id
                    self.logger().info(f"AssignmentExecutor {self.config.id} - Recovered assignment_id from config: {self._assignment_id}")
                
                # 2. Try controller_id
                elif hasattr(self.config, 'controller_id') and self.config.controller_id:
                    self._assignment_id = self.config.controller_id
                    self.logger().info(f"AssignmentExecutor {self.config.id} - Using controller_id as assignment_id: {self._assignment_id}")
                
                # 3. Check if we're being created from a pending executor
                elif hasattr(self.config, 'id') and self.config.id:
                    # Extract from pending_prefix if present
                    if "pending_" in self.config.id:
                        self._assignment_id = self.config.id.replace("pending_", "")
                        self.logger().info(f"AssignmentExecutor {self.config.id} - Extracted assignment_id from pending prefix: {self._assignment_id}")
                    else:
                        # Just use the executor ID
                        self._assignment_id = self.config.id
                        self.logger().info(f"AssignmentExecutor {self.config.id} - Using executor ID as assignment_id: {self._assignment_id}")
                
                # 4. Last resort - use config id directly
                if not self._assignment_id:
                    self._assignment_id = self.config.id  
                    self.logger().warning(f"AssignmentExecutor {self.config.id} - No assignment_id sources found, forced to use executor ID")
            
            # Clean up the assignment_id (remove pending_ prefix if present)
            if isinstance(self._assignment_id, str) and "pending_" in self._assignment_id:
                self._assignment_id = self._assignment_id.replace("pending_", "")
                self.logger().info(f"AssignmentExecutor {self.config.id} - Cleaned assignment_id: {self._assignment_id}")
            
            # CRITICAL: Force assignment_id into config.assignment_id for synchronization
            if hasattr(self.config, 'assignment_id'):
                # Only update if needed to avoid unnecessary logs
                if self.config.assignment_id != self._assignment_id:
                    self.logger().info(f"AssignmentExecutor {self.config.id} - Updating config.assignment_id from {self.config.assignment_id} to {self._assignment_id}")
                    self.config.assignment_id = self._assignment_id
            
            # VERIFICATION: Check our custom_info to ensure it has assignment_id and fill_id
            custom_info = self.get_custom_info()
            self.logger().info(f"AssignmentExecutor {self.config.id} - VERIFICATION: custom_info has assignment_id={custom_info.get('assignment_id')}, fill_id={custom_info.get('fill_id')}")
            
            await super().on_start()
            self.logger().info(f"AssignmentExecutor {self.config.id} super().on_start() completed")
            self.logger().info(f"AssignmentExecutor {self.config.id} starting with assigned amount: {self._assigned_amount} @ {self._entry_price}")
            self.logger().info(f"Assignment details - Trading pair: {self.config.trading_pair}, Side: {self.config.side.name}, Position action: {self.config.position_action.name}")
            await self.validate_sufficient_balance()
            self.logger().info(f"AssignmentExecutor {self.config.id} validate_sufficient_balance completed")
            
            # Log the current state to check why control_close_order isn't being called
            self.logger().info(f"AssignmentExecutor {self.config.id} status after on_start: {self.status}")
            
            # Explicitly trigger the close order control to check if it's working
            self.logger().info(f"AssignmentExecutor {self.config.id} explicitly calling control_close_order()")
            await self.control_close_order()
            self.logger().info(f"AssignmentExecutor {self.config.id} control_close_order() completed")
        except Exception as e:
            self.logger().error(f"AssignmentExecutor {self.config.id} on_start ERROR: {str(e)}", exc_info=True)
            raise

    async def validate_sufficient_balance(self):
        """Validate if there is sufficient balance to execute the order."""
        # For assignments, we need to verify we have sufficient balance to CLOSE the position,
        # not to open it (since it's already assigned to us)
        close_side = self.close_order_side
        
        self.logger().info(f"AssignmentExecutor {self.config.id} - Validating balance for CLOSE order with side: {close_side}")
        
        if self.is_perpetual:
            order_candidate = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=False,  # We're using market orders for closing
                order_type=OrderType.MARKET,
                order_side=close_side,  # Use the close side, not the open side
                amount=self.config.amount,
                price=self.entry_price,
                leverage=self.config.leverage if hasattr(self.config, "leverage") else Decimal("1"),
            )
        else:
            order_candidate = OrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=False,  # We're using market orders for closing
                order_type=OrderType.MARKET,
                order_side=close_side,  # Use the close side, not the open side
                amount=self.config.amount,
                price=self.entry_price,
            )
        
        self.logger().info(f"AssignmentExecutor {self.config.id} - Checking balance with order candidate: {order_candidate}")
        adjusted_order_candidates = self.adjust_order_candidates(self.config.connector_name, [order_candidate])
        
        if adjusted_order_candidates[0].amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error(f"AssignmentExecutor {self.config.id} - Not enough balance to execute assignment closing. Side: {close_side}")
            self.stop()
        else:
            self.logger().info(f"AssignmentExecutor {self.config.id} - Sufficient balance found for closing order")

    @property
    def close_order_side(self) -> TradeType:
        """Get the order side for closing the position."""
        # For assignments, the controller has already determined the correct side for closing the position
        # For example, a LONG position will have side=SELL in the config to close it
        self.logger().info(f"AssignmentExecutor {self.config.id} - Determining close order side: Config side={self.config.side}, Position action={self.config.position_action}")
        
        # Use the side directly from the config, since it's already set correctly for closing
        return self.config.side

    @property
    def open_filled_amount(self) -> Decimal:
        """
        Get the filled amount of the open position.
        For assignments, this is always the assigned amount.
        """
        self.logger().debug(f"AssignmentExecutor {self.config.id} - open_filled_amount called, returning assigned amount: {self._assigned_amount}")
        return self._assigned_amount

    @property
    def open_filled_amount_quote(self) -> Decimal:
        """
        Get the filled amount of the open order in quote currency.

        :return: The filled amount of the open order in quote currency.
        """
        return self.open_filled_amount * self.entry_price

    @property
    def close_filled_amount(self) -> Decimal:
        """
        Get the filled amount of the close order.

        :return: The filled amount of the close order if it exists, otherwise 0.
        """
        amount = self._close_order.executed_amount_base if self._close_order else Decimal("0")
        self.logger().debug(f"AssignmentExecutor {self.config.id} - close_filled_amount called, returning: {amount}")
        return amount

    @property
    def close_filled_amount_quote(self) -> Decimal:
        """
        Get the filled amount of the close order in quote currency.

        :return: The filled amount of the close order in quote currency.
        """
        return self.close_filled_amount * self.close_price

    def place_close_order_and_cancel_open_orders(self, close_type: CloseType, price: Decimal = Decimal("NaN")):
        """
        Place a close order and cancel any open orders.
        For assignments, we only need to cancel the current order as we don't track close orders.
        
        :param close_type: The type of close order
        :param price: The price for the close order (not used in assignments)
        :return: None
        """
        # First check if the position is already fully closed to prevent placing unnecessary orders
        if self.open_and_close_volume_match() or self.close_filled_amount >= self._assigned_amount:
            self.logger().info(f"AssignmentExecutor {self.config.id} - Position is already fully closed. Not placing any close orders.")
            self.close_type = close_type
            self.close_timestamp = self._strategy.current_timestamp
            self._status = RunnableStatus.SHUTTING_DOWN
            return

        self.logger().info(f"AssignmentExecutor {self.config.id} - Attempting to place close order. Amount to close: {self.amount_to_close}, Min order size: {self.trading_rules.min_order_size if hasattr(self, 'trading_rules') else 'unknown'}")

        # Cancel any existing open orders first
        self.cancel_open_orders()

        # Check if there's enough amount to close
        if self.amount_to_close >= self.trading_rules.min_order_size:
            self.logger().info(f"AssignmentExecutor {self.config.id} - Placing close order - Side: {self.close_order_side}, Type: {OrderType.MARKET}, Amount: {self.amount_to_close}")
            
            # Place the order using the strategy's order placement method
            order_id = self.place_order(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_type=OrderType.MARKET,
                side=self.close_order_side,
                amount=self.amount_to_close,
                position_action=PositionAction.CLOSE,
                price=price if not price.is_nan() else Decimal("0")  # Use 0 as placeholder for market orders
            )

            self.logger().info(f"AssignmentExecutor {self.config.id} - Order placed successfully with ID: {order_id}")
            self._close_order = TrackedOrder(order_id=order_id)
        else:
            self.logger().warning(f"AssignmentExecutor {self.config.id} - Cannot place close order: amount to close {self.amount_to_close} is less than min order size {self.trading_rules.min_order_size if hasattr(self, 'trading_rules') else 'unknown'}")
            # Increment retries counter so we don't get stuck in an infinite loop
            self._current_retries += 1

        self.close_type = close_type
        self.close_timestamp = self._strategy.current_timestamp
        self.logger().info(f"AssignmentExecutor {self.config.id} - Setting status to SHUTTING_DOWN with close type: {close_type}")
        self._status = RunnableStatus.SHUTTING_DOWN

    def cancel_open_orders(self):
        """Cancel all open orders."""
        self.cancel_all_orders()

    def place_order(self,
                    connector_name: str,
                    trading_pair: str,
                    order_type: OrderType,
                    side: TradeType,
                    amount: Decimal,
                    position_action: PositionAction = PositionAction.NIL,
                    price=Decimal("NaN"),
                    ):
        """
        Override of the base place_order method to ensure market orders always have a valid price for tracking.
        
        :param connector_name: The name of the connector.
        :param trading_pair: The trading pair for the order.
        :param order_type: The type of the order.
        :param side: The side of the order (buy or sell).
        :param amount: The amount for the order.
        :param position_action: The position action for the order.
        :param price: The price for the order.
        :return: The result of the order placement.
        """
        # For market orders, just use 0 as a placeholder for order recording
        # The actual execution price will be recorded when the order fills
        if order_type == OrderType.MARKET and (price is None or price.is_nan()):
            price = Decimal("0")
            self.logger().info(f"Using placeholder price 0 for market order recording (actual price will come from fill)")
        
        # Call the parent method with the valid price
        if side == TradeType.BUY:
            return self._strategy.buy(connector_name, trading_pair, amount, order_type, price, position_action)
        else:
            return self._strategy.sell(connector_name, trading_pair, amount, order_type, price, position_action)

    def early_stop(self):
        """
        Handle early stop request from strategy.
        """
        self.close_type = CloseType.EARLY_STOP
        self.cancel_all_orders()
        self._status = RunnableStatus.SHUTTING_DOWN
        self.close_timestamp = self._strategy.current_timestamp

    def to_format_status(self) -> str:
        """
        Format the status of the assignment executor
        :return: The status list
        """
        lines = []
        current_price = self.get_price(self.config.connector_name, self.config.trading_pair)
        amount_in_quote = self.entry_price * (self.filled_amount if self.filled_amount > Decimal("0") else self.config.amount)
        quote_asset = self.config.trading_pair.split("-")[1]
        
        # When showing fees in status, use the cached property
        current_time = time.time()
        should_log_fee_details = not hasattr(self, '_last_status_fee_log') or (current_time - self._last_status_fee_log > 300)
        
        if should_log_fee_details:
            self._last_status_fee_log = current_time
            lines.append(f"Fees: {self.cum_fees_quote} {quote_asset}")
        
        if self.is_closed:
            lines.extend([f"""
| Trading Pair: {self.config.trading_pair} | Exchange: {self.config.connector_name} | Side: {self.config.side}
| Entry price: {self.entry_price:.6f} | Close price: {self.close_price:.6f} | Amount: {amount_in_quote:.4f} {quote_asset}
| Realized PNL: {self.trade_pnl_quote:.6f} {quote_asset} | Total Fee: {self.cum_fees_quote:.6f} {quote_asset}
| PNL (%): {self.get_net_pnl_pct() * 100:.2f}% | PNL (abs): {self.get_net_pnl_quote():.6f} {quote_asset} | Close Type: {self.close_type}
"""])
        else:
            lines.extend([f"""
| Trading Pair: {self.config.trading_pair} | Exchange: {self.config.connector_name} | Side: {self.config.side}
| Entry price: {self.entry_price:.6f} | Current price: {current_price:.6f} | Amount: {amount_in_quote:.4f} {quote_asset}
| Unrealized PNL: {self.trade_pnl_quote:.6f} {quote_asset} | Total Fee: {self.cum_fees_quote:.6f} {quote_asset}
| PNL (%): {self.get_net_pnl_pct() * 100:.2f}% | PNL (abs): {self.get_net_pnl_quote():.6f} {quote_asset}
"""])
        return "\n".join(lines)

    def control_open_order(self):
        """
        Control open order - For assignments, this is a no-op as we don't place open orders.
        The position is already opened through the assignment.
        """
        pass

    def place_open_order(self):
        """
        Place open order - For assignments, this is a no-op as we don't place open orders.
        The position is already opened through the assignment.
        """
        pass

    def cancel_open_order(self):
        """not in use for assignments"""
    pass

    @property
    def has_stop_loss(self) -> bool:
        """Check if stop loss order exists and is active."""
        return self._stop_loss_order is not None and not self._stop_loss_order.is_done

    @property
    def has_take_profit(self) -> bool:
        """Check if take profit order exists and is active."""
        return self._take_profit_order is not None and not self._take_profit_order.is_done

    @property
    def has_trailing_stop(self) -> bool:
        """Check if trailing stop order exists and is active."""
        return self._trailing_stop_order is not None and not self._trailing_stop_order.is_done

    def control_barriers(self):
        """
        This method is responsible for controlling the barriers. It controls the stop loss, take profit, time limit and
        trailing stop.

        :return: None
        """
        if self._open_order and self._open_order.is_filled and self.open_filled_amount >= self.trading_rules.min_order_size \
                and self.open_filled_amount_quote >= self.trading_rules.min_notional_size:
            self.control_stop_loss()
            self.control_trailing_stop()
            self.control_take_profit()
        self.control_time_limit()

    @property
    def is_expired(self) -> bool:
        """
        Check if the position is expired.

        :return: True if the position is expired, False otherwise.
        """
        return self.end_time and self.end_time <= self._strategy.current_timestamp

    @property
    def end_time(self) -> Optional[float]:
        """
        Calculate the end time of the position based on the time limit

        :return: The end time of the position.
        """
        if not self.config.triple_barrier_config.time_limit:
            return None
        return self.config.timestamp + self.config.triple_barrier_config.time_limit

    def control_time_limit(self):
        """
        This method is responsible for controlling the time limit. If the position is expired, it places the close order
        and cancels the open orders.

        :return: None
        """
        if self.is_expired:
            self.place_close_order_and_cancel_open_orders(close_type=CloseType.TIME_LIMIT)

    def control_stop_loss(self):
        """
        This method is responsible for controlling the stop loss. If the net pnl percentage is less than the stop loss
        percentage, it places the close order and cancels the open orders.

        :return: None
        """
        if self.config.triple_barrier_config.stop_loss:
            if self.net_pnl_pct <= -self.config.triple_barrier_config.stop_loss:
                self.place_close_order_and_cancel_open_orders(close_type=CloseType.STOP_LOSS)

    def control_take_profit(self):
        """
        This method is responsible for controlling the take profit. If the net pnl percentage is greater than the take
        profit percentage, it places the close order and cancels the open orders. If the take profit order type is limit,
        it places the take profit limit order. If the amount of the take profit order is different than the total amount
        executed in the open order, it renews the take profit order (can happen with partial fills).

        :return: None
        """
        if self.config.triple_barrier_config.take_profit:
            if self.config.triple_barrier_config.take_profit_order_type.is_limit_type():
                is_within_activation_bounds = self._is_within_activation_bounds(
                    self.take_profit_price, self.close_order_side,
                    self.config.triple_barrier_config.take_profit_order_type)
                if not self._take_profit_limit_order:
                    if is_within_activation_bounds:
                        self.place_take_profit_limit_order()
                else:
                    if self._take_profit_limit_order.is_open and not self._take_profit_limit_order.is_filled and \
                            not is_within_activation_bounds:
                        self.cancel_take_profit()
            elif self.net_pnl_pct >= self.config.triple_barrier_config.take_profit:
                self.place_close_order_and_cancel_open_orders(close_type=CloseType.TAKE_PROFIT)

    def place_take_profit_limit_order(self):
        """
        This method is responsible for placing the take profit limit order.

        :return: None
        """
        order_id = self.place_order(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            amount=self.amount_to_close,
            price=self.take_profit_price,
            order_type=self.config.triple_barrier_config.take_profit_order_type,
            position_action=PositionAction.CLOSE,
            side=self.close_order_side,
        )
        self._take_profit_limit_order = TrackedOrder(order_id=order_id)
        self.logger().debug(f"Executor ID: {self.config.id} - Placing take profit order {order_id}")

    def control_trailing_stop(self):
        if self.config.triple_barrier_config.trailing_stop:
            net_pnl_pct = self.get_net_pnl_pct()
            if not self._trailing_stop_trigger_pct:
                if net_pnl_pct > self.config.triple_barrier_config.trailing_stop.activation_price:
                    self._trailing_stop_trigger_pct = net_pnl_pct - self.config.triple_barrier_config.trailing_stop.trailing_delta
            else:
                if net_pnl_pct < self._trailing_stop_trigger_pct:
                    self.place_close_order_and_cancel_open_orders(close_type=CloseType.TRAILING_STOP)
                if net_pnl_pct - self.config.triple_barrier_config.trailing_stop.trailing_delta > self._trailing_stop_trigger_pct:
                    self._trailing_stop_trigger_pct = net_pnl_pct - self.config.triple_barrier_config.trailing_stop.trailing_delta

    async def cancel_stop_loss(self):
        """Cancel stop loss order if it exists."""
        if self._stop_loss_order and not self._stop_loss_order.is_done:
            self._strategy.cancel(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_id=self._stop_loss_order.order_id
            )
            self._stop_loss_order = None

    async def place_take_profit_order(self):
        """
        Place a take profit order.
        For assignments, this is a no-op as we don't use take profit.
        """
        self.logger().debug("Take profit orders are not used for assignments.")
        return

    async def cancel_take_profit(self):
        """Cancel take profit order if it exists."""
        if self._take_profit_order and not self._take_profit_order.is_done:
            self._strategy.cancel(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_id=self._take_profit_order.order_id
            )
            self._take_profit_order = None

    async def place_trailing_stop_order(self):
        """
        Place a trailing stop order.
        For assignments, this is a no-op as we don't use trailing stop.
        """
        self.logger().debug("Trailing stop orders are not used for assignments.")
        return
            
    async def cancel_trailing_stop(self):
        """Cancel trailing stop order if it exists."""
        if self._trailing_stop_order and not self._trailing_stop_order.is_done:
            self._strategy.cancel(
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                order_id=self._trailing_stop_order.order_id
            )
            self._trailing_stop_order = None

    def cancel_all_orders(self):
        """Cancel all orders associated with this executor."""
        asyncio.create_task(self.cancel_stop_loss())
        asyncio.create_task(self.cancel_take_profit())
        asyncio.create_task(self.cancel_trailing_stop())

    @property
    def amount_to_close(self) -> Decimal:
        """
        Returns the amount that needs to be closed.
        Takes into account both filled amounts and active close orders.
        """
        # If we already have an active close order, consider it as being in the process of closing
        # to prevent placing additional orders
        pending_amount = Decimal("0")
        if self._close_order and self._close_order.order_id:
            # For market orders that are still pending, consider the entire amount as pending
            if self._close_order.order:
                if not self._close_order.order.is_done:
                    # For any order that's not done, consider its remaining amount as pending
                    pending_amount = self._close_order.order.amount - self._close_order.executed_amount_base
                
                self.logger().debug(f"AssignmentExecutor {self.config.id} - Active close order pending amount: {pending_amount}")
            
        # Calculate remaining amount: assigned - (already closed + pending)
        amount = self.assigned_amount - (self.close_filled_amount + pending_amount)
        
        # Ensure we don't return negative values
        amount = max(Decimal("0"), amount)
        
        self.logger().debug(f"AssignmentExecutor {self.config.id} - Amount to close calculation: "
                          f"assigned_amount({self.assigned_amount}) - "
                          f"close_filled_amount({self.close_filled_amount}) - "
                          f"pending_amount({pending_amount}) = {amount}")
        return amount

    def open_orders_completed(self) -> bool:
        """Check if all orders are completed."""
        return not self._close_order or (self._close_order.order and self._close_order.order.is_done)

    def open_and_close_volume_match(self) -> bool:
        """
        Check if the closing amount matches the assigned amount.
        For assignments, we compare closing amount to assigned amount.
        """
        if self._assigned_amount == Decimal("0"):
                    return True
        else:
            return self.close_filled_amount >= self._assigned_amount

    async def update(self):
        """Update the executor."""
        try:
            self.logger().debug(f"AssignmentExecutor {self.config.id} update called, status: {self.status}")
            
            # Check for status and log key metrics to diagnose why orders aren't being placed
            if self.status == RunnableStatus.RUNNING:
                self.logger().debug(f"AssignmentExecutor {self.config.id} metrics: " +
                                   f"amount_to_close={self.amount_to_close}, " +
                                   f"assigned_amount={self.assigned_amount}, " +
                                   f"open_filled_amount={self.open_filled_amount}, " +
                                   f"close_filled_amount={self.close_filled_amount}, " +
                                   f"close_order_exists={self._close_order is not None}")
                
                if not self._close_order:
                    self.logger().info(f"AssignmentExecutor {self.config.id} No close order exists during update, calling control_close_order")
                    await self.control_close_order()
            
            await super().update()
        except Exception as e:
            self.logger().error(f"AssignmentExecutor {self.config.id} update ERROR: {str(e)}", exc_info=True)

    async def control_task(self):
        """
        The main task executed in the control loop.
        For AssignmentExecutor, it primarily needs to handle closing the assigned position.
        """
        try:
            self.logger().debug(f"AssignmentExecutor {self.config.id} control_task executing, status: {self.status}")
            
            # First, check if position is already closed to prevent any further orders
            if self.open_and_close_volume_match() or self.close_filled_amount >= self._assigned_amount:
                self.logger().info(f"AssignmentExecutor {self.config.id} Position already fully closed (assignment_id: {self._assignment_id}), stopping executor")
                # Set to shutting down instead of terminated directly
                self._status = RunnableStatus.SHUTTING_DOWN
                self.close_timestamp = self._strategy.current_timestamp
                await self.control_shutdown()
                return  # Return immediately to prevent any more processing

            # Additional safeguard: periodically check with exchange for position status
            # This helps catch cases where our local state might be out of sync with the exchange
            try:
                exchange = self.connectors[self.config.connector_name]
                positions = exchange.account_positions
                position_found = False
                
                for pos in positions:
                    if pos.trading_pair == self.config.trading_pair:
                        position_size = abs(pos.amount)
                        if position_size > 0:
                            position_found = True
                            self.logger().debug(f"AssignmentExecutor {self.config.id} - Found active position: {position_size} {self.config.trading_pair}")
                            break
                
                # If no position found but we think we still have one, this suggests the position
                # was closed outside of our executor or through the exchange UI
                if not position_found and self.assigned_amount > 0 and self.close_filled_amount < self.assigned_amount:
                    self.logger().info(f"AssignmentExecutor {self.config.id} - No position found on exchange but we expected one - " +
                                     f"assuming position was externally closed.")
                    self._status = RunnableStatus.SHUTTING_DOWN
                    self.close_timestamp = self._strategy.current_timestamp
                    await self.control_shutdown()
                    return
            except Exception as e:
                self.logger().error(f"AssignmentExecutor {self.config.id} - Error checking position status: {e}")

            # Check if we need to update processed data
            if self._executors_update_event.is_set():
                self.logger().debug(f"AssignmentExecutor {self.config.id} Received update event")
                await self._strategy.update_processed_data()
                self._executors_update_event.clear()

            # Determine what actions this executor should take
            if self.status == RunnableStatus.RUNNING:
                self.logger().debug(f"AssignmentExecutor {self.config.id} Executor is running")
                await self.control_close_order()
            elif self.status == RunnableStatus.SHUTTING_DOWN:
                self.logger().debug(f"AssignmentExecutor {self.config.id} Executor is shutting down")
                await self.control_shutdown()
                
        except Exception as e:
            self.logger().error(f"AssignmentExecutor {self.config.id} control_task ERROR: {str(e)}", exc_info=True)

    def stop(self):
        """
        Override the stop method to ensure proper termination and cleanup.
        """
        self.logger().info(f"AssignmentExecutor {self.config.id} - Stopping executor")
        
        # Cancel any pending orders to ensure clean termination
        self.cancel_all_orders()
        
        # Call the parent class stop method
        super().stop()
