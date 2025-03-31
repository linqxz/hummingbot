import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.assignment_adapter_executor.data_types import AssignmentAdapterExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


# Create a minimal mock InFlightOrder class for our adapter
class MockInFlightOrder(InFlightOrder):
    """
    A minimal mock implementation of InFlightOrder that always reports as complete
    Used to simulate an already filled order for assignments
    """
    
    def __init__(self, client_order_id: str, trading_pair: str, amount: Decimal, price: Decimal, creation_timestamp: float):
        super().__init__(
            client_order_id=client_order_id,
            trading_pair=trading_pair,
            order_type=OrderType.MARKET,
            trade_type=TradeType.BUY,  # Side doesn't matter for our mock
            amount=amount,
            price=price,
            creation_timestamp=creation_timestamp
        )
        # Set values that indicate this order is already completely filled
        self._executed_amount_base = amount
        self._executed_amount_quote = amount * price
        self._order_fills = {}  # Empty fill dictionary
        self._last_state = "FILLED"

    @property
    def is_done(self) -> bool:
        return True

    @property
    def is_filled(self) -> bool:
        return True

    @property
    def is_open(self) -> bool:
        return False


class AssignmentAdapterExecutor(PositionExecutor):
    """
    Adapter that makes PositionExecutor handle assignments.
    
    This adapter overrides the minimum necessary behavior to treat assignments
    as already-opened positions, allowing all the position management logic
    of PositionExecutor to work without modification.
    """
    
    def __init__(self, strategy: ScriptStrategyBase, config: AssignmentAdapterExecutorConfig,
                 update_interval: float = 1.0, max_retries: int = 10):
        """
        Initialize the AssignmentAdapterExecutor instance.
        
        Args:
            strategy: The strategy to be used by the executor
            config: Configuration for the executor
            update_interval: The interval at which the executor should be updated
            max_retries: Maximum number of retries for the executor
        """
        # Log initialization start
        logging.getLogger(__name__).info(f"[ASSIGNMENT_ADAPTER] Creating executor ID={config.id} for assignment_id={config.assignment_id}")
        
        # Log detailed configuration
        config_details = {
            "id": config.id,
            "assignment_id": config.assignment_id,
            "connector_name": config.connector_name,
            "trading_pair": config.trading_pair,
            "side": config.side,
            "amount": config.amount,
            "entry_price": config.entry_price,
            "leverage": config.leverage,
            "time_limit": getattr(config.triple_barrier_config, "time_limit", None),
            "position_action": config.position_action,
            "timestamp": getattr(config, "timestamp", time.time()),
        }
        logging.getLogger(__name__).info(f"[ASSIGNMENT_ADAPTER] Config details: {config_details}")
        
        # Call parent's constructor
        super().__init__(strategy=strategy, config=config, 
                         update_interval=update_interval, max_retries=max_retries)
        
        # Initialize progress tracking
        self._last_progress_timestamp = time.time()
        
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Initializing for {config.trading_pair}: " +
                           f"{config.side.name} {config.amount} @ {config.entry_price} " +
                           f"(assignment_id: {config.assignment_id})")
        
        # Create a fake order ID for the assignment
        order_id = f"assignment_{config.assignment_id or 'unknown'}"
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Created fake order ID: {order_id}")
        
        # Use the config timestamp as the order creation timestamp, or current timestamp if not available
        creation_timestamp = getattr(config, "timestamp", time.time())
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Using creation timestamp: {creation_timestamp}")
        
        # Create a mock InFlightOrder that's already filled
        mock_order = MockInFlightOrder(
            client_order_id=order_id,
            trading_pair=config.trading_pair,
            amount=config.amount,
            price=config.entry_price,
            creation_timestamp=creation_timestamp
        )
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Created mock order with ID: {order_id}")
        
        # Create a TrackedOrder with our mock order
        self._open_order = TrackedOrder(order_id=order_id)
        self._open_order.order = mock_order
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Created and assigned tracked order: {order_id}")
        
        # Store assignment details for reference
        self._assignment_id = config.assignment_id
        self._assigned_amount = config.amount
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Stored assignment details - ID: {self._assignment_id}, Amount: {self._assigned_amount}")
        
        # Log that we're treating this as an already-open position
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Assignment treated as already-open position: {config.amount} {config.trading_pair} " +
                          f"at price {config.entry_price}")
        
        # If time_limit is 0, log that we'll close immediately
        if config.triple_barrier_config.time_limit == 0:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Time limit set to 0, position will close immediately upon start")

    async def on_start(self):
        """
        Override on_start to skip opening a position and validate we can close it.
        """
        self.logger().info(f"[ASSIGNMENT_ADAPTER] on_start called for executor {self.config.id}, assignment: {self._assignment_id}")
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Status at start: {self.status}, position_action: {self.config.position_action}")
        
        # Log start of operation
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Starting for assignment: {self._assignment_id}")
        
        # If time_limit is 0, immediately attempt to close the position without
        # waiting for additional validations
        if self.config.triple_barrier_config.time_limit == 0:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Time limit is 0, immediately closing position for assignment: {self._assignment_id}")
            
            # Close the position immediately and return
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Calling close_position_safely for immediate execution")
            await self.close_position_safely()
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Returned from close_position_safely call, status: {self.status}")
            return
        
        # For non-immediate closures, perform normal validations
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Time limit is not 0, performing normal validation")
        
        # Check if the position actually exists on the exchange
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking if position exists on exchange")
        await self.check_exchange_position()
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Position check completed, status: {self.status}")
        
        # Only validate balance if we haven't already completed or failed
        if self.status == RunnableStatus.RUNNING:
            # Validate we have enough balance to close the position
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Validating sufficient balance for closing")
            await self.validate_sufficient_balance_for_closing()
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Balance validation completed, status: {self.status}")
        else:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Skipping balance validation, executor no longer in RUNNING state, current status: {self.status}")

    async def check_exchange_position(self):
        """
        Check if the position actually exists on the exchange and log details
        """
        connector = self.connectors[self.config.connector_name]
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Starting position check for {self.config.trading_pair} on {self.config.connector_name}")
        
        try:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking if position exists on exchange for {self.config.trading_pair}")
            
            # Perpetual exchanges store positions in account_positions
            # Check if we have the PerpetualTrading mixin behavior
            if hasattr(connector, "account_positions"):
                positions = connector.account_positions
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Found {len(positions)} positions on exchange")
                
                # Add debug logging for all position keys
                self.logger().info(f"[ASSIGNMENT_ADAPTER] All position keys: {list(positions.keys())}")
                
                # Output all positions for debugging
                if len(positions) > 0:
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Current positions on exchange:")
                    for pos_key, pos in positions.items():
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Position key: {pos_key}, amount: {pos.amount}, side: {pos.position_side}")
                else:
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] No positions currently exist on exchange")
                
                # Look for our position - it could be stored with different key formats
                position = None
                trading_pair = self.config.trading_pair
                
                # Try different position keys based on position mode
                # In HEDGE mode, positions are stored as "{trading_pair}{side.name}"
                # In ONEWAY mode, positions are stored just as the trading pair
                if hasattr(connector, "position_key") and callable(connector.position_key):
                    # First try using position_key helper if available
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Attempting to find position using position_key method")
                    for side in [PositionSide.LONG, PositionSide.SHORT]:
                        pos_key = connector.position_key(trading_pair, side)
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with key: {pos_key}")
                        if pos_key in positions:
                            position = positions[pos_key]
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Found position using position_key: {pos_key}")
                            break
                
                # If still not found, try direct lookup
                if position is None:
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Position not found with position_key, trying alternative lookup methods")
                    
                    # Try direct lookup with trading pair (ONEWAY mode)
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with direct trading pair: {trading_pair}")
                    if trading_pair in positions:
                        position = positions[trading_pair]
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Found position using direct trading pair lookup")
                    # Try appending position sides (HEDGE mode)
                    else:
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with appended side keys")
                        for side in ["LONG", "SHORT"]:
                            pos_key = f"{trading_pair}{side}"
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with key: {pos_key}")
                            if pos_key in positions:
                                position = positions[pos_key]
                                self.logger().info(f"[ASSIGNMENT_ADAPTER] Found position using {pos_key}")
                                break
                
                # Log found position
                if position:
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Found position on exchange: {position}")
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Position amount: {position.amount}, side: {position.position_side}")
                    
                    # Update our internal amount if necessary
                    if abs(position.amount) > Decimal("0"):
                        if position.amount != self._assigned_amount:
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Updating assigned amount from {self._assigned_amount} to {position.amount}")
                            self._assigned_amount = abs(position.amount)
                        # We've found a valid position - continue closing it
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Valid position found, will proceed with closing")
                    else:
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Position found but amount is zero for {self.config.trading_pair}")
                        # Position exists but has zero amount (likely already closed)
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Setting close_type to COMPLETED and stopping executor")
                        self.close_type = CloseType.COMPLETED
                        self.stop()
                else:
                    # If the initial assignment happened, we should still try to close the position
                    # using the amount from the assignment event
                    self.logger().warning(f"[ASSIGNMENT_ADAPTER] No position found on exchange for {self.config.trading_pair}")
                    
                    # If the assignment is very recent, it's possible the position hasn't shown up in the account yet
                    # So we'll continue with the original amount and retry
                    time_since_assignment = time.time() - getattr(self.config, "timestamp", time.time())
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Time since assignment: {time_since_assignment:.2f} seconds")
                    
                    if time_since_assignment < 5:
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Assignment is recent, will use original amount: {self._assigned_amount}")
                        # Don't mark as completed, we'll try to close it anyway
                    else:
                        # If it's an older assignment and we still can't find the position, it might already be closed
                        self.logger().warning(f"[ASSIGNMENT_ADAPTER] Assignment is not recent and position not found, likely already closed")
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Setting close_type to COMPLETED and stopping executor")
                        self.close_type = CloseType.COMPLETED  # Already closed or non-existent
                        self.stop()
            else:
                self.logger().warning(f"[ASSIGNMENT_ADAPTER] Connector {self.config.connector_name} does not have account_positions attribute")
        except Exception as e:
            self.logger().error(f"[ASSIGNMENT_ADAPTER] Error checking position: {e}", exc_info=True)
            # Don't fail the executor on position check failure
            # We'll still try to close the position with the original amount
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Will attempt to close with original amount despite error: {self._assigned_amount}")
    
    async def _check_if_position_already_closed(self):
        """
        Check if the position is already closed based on exchange data.
        This helps avoid endless retries when a position has been closed but we didn't track it.
        """
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking if position is already closed for {self.config.trading_pair}")
        connector = self.connectors[self.config.connector_name]
        
        try:
            # Check if position exists in account positions
            if hasattr(connector, "account_positions"):
                positions = connector.account_positions
                
                # Add debug logging for all position keys
                self.logger().info(f"[ASSIGNMENT_ADAPTER] All position keys in _check_if_position_already_closed: {list(positions.keys())}")
                
                position_exists = False
                trading_pair = self.config.trading_pair
                
                # Try all possible ways the position could be tracked
                if hasattr(connector, "position_key") and callable(connector.position_key):
                    for side in [PositionSide.LONG, PositionSide.SHORT]:
                        pos_key = connector.position_key(trading_pair, side)
                        if pos_key in positions:
                            position = positions[pos_key]
                            if abs(position.amount) > Decimal("0"):
                                position_exists = True
                                self.logger().info(f"[ASSIGNMENT_ADAPTER] Position still exists with amount {position.amount}")
                                break
                
                # Try direct lookup
                if not position_exists and trading_pair in positions:
                    position = positions[trading_pair]
                    if abs(position.amount) > Decimal("0"):
                        position_exists = True
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Position still exists with amount {position.amount}")
                
                # Try other formats
                if not position_exists:
                    for side in ["LONG", "SHORT"]:
                        pos_key = f"{trading_pair}{side}"
                        if pos_key in positions and abs(positions[pos_key].amount) > Decimal("0"):
                            position_exists = True
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Position still exists with amount {positions[pos_key].amount}")
                            break
                
                # If no position found with non-zero amount, it's likely already closed
                if not position_exists:
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] No active position found for {trading_pair}, considering it already closed")
                    return True
            
            return False
        except Exception as e:
            self.logger().error(f"[ASSIGNMENT_ADAPTER] Error checking if position is closed: {e}", exc_info=True)
            return False

    async def close_position_safely(self):
        """
        Close the position after checking for its existence
        """
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Starting close_position_safely for assignment: {self._assignment_id}")
        
        # Check the actual position amount from the exchange once more
        connector = self.connectors[self.config.connector_name]
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Using connector: {self.config.connector_name}")
        
        try:
            # Get position from account_positions
            position = None
            trading_pair = self.config.trading_pair
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Looking for position to close for {trading_pair}")
            
            # Position side to use if we can't find the position
            default_position_side = None
            # Default amount to use if we can't find the position
            amount_to_close = self._assigned_amount
            
            if hasattr(connector, "account_positions"):
                positions = connector.account_positions
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Found {len(positions)} positions on exchange")
                
                # Add debug logging for all position keys
                self.logger().info(f"[ASSIGNMENT_ADAPTER] All position keys in close_position_safely: {list(positions.keys())}")
                
                # Try different position keys based on position mode
                if hasattr(connector, "position_key") and callable(connector.position_key):
                    # First try using position_key helper if available
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Attempting to find position using position_key method")
                    for side in [PositionSide.LONG, PositionSide.SHORT]:
                        pos_key = connector.position_key(trading_pair, side)
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with key: {pos_key}")
                        if pos_key in positions:
                            position = positions[pos_key]
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Found position to close using position_key: {pos_key}")
                            break
                
                # If still not found, try direct lookup
                if position is None:
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Position not found with position_key, trying alternative lookup methods")
                    
                    # Try direct lookup with trading pair (ONEWAY mode)
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with direct trading pair: {trading_pair}")
                    if trading_pair in positions:
                        position = positions[trading_pair]
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Found position to close using direct trading pair lookup")
                    # Try appending position sides (HEDGE mode)
                    else:
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with appended side keys")
                        for side in ["LONG", "SHORT"]:
                            pos_key = f"{trading_pair}{side}"
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Checking for position with key: {pos_key}")
                            if pos_key in positions:
                                position = positions[pos_key]
                                self.logger().info(f"[ASSIGNMENT_ADAPTER] Found position to close using {pos_key}")
                                # Remember this side for later if needed
                                default_position_side = PositionSide.LONG if side == "LONG" else PositionSide.SHORT
                                break
            
            if position and abs(position.amount) > Decimal("0"):
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Closing position with amount: {position.amount}, side: {position.position_side}")
                
                # Make sure we use the correct side for closing
                close_side = TradeType.BUY if position.position_side == PositionSide.SHORT else TradeType.SELL
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Using close side: {close_side} for position side: {position.position_side}")
                
                # Update the amount to close
                amount_to_close = abs(position.amount)
            else:
                self.logger().info(f"[ASSIGNMENT_ADAPTER] No position found in account_positions, will still try to close using assignment data")
                
                # Determine the position side based on the assignment config or the default from lookup
                if default_position_side is None:
                    # If no position found, guess based on the assignment side (opposite of the trade_type)
                    default_position_side = PositionSide.SHORT if self.config.side == TradeType.SELL else PositionSide.LONG
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Using inferred position side: {default_position_side} based on assignment side: {self.config.side}")
                
                # Determine close side from position side
                close_side = TradeType.BUY if default_position_side == PositionSide.SHORT else TradeType.SELL
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Using derived close side: {close_side} for inferred position side: {default_position_side}")
                
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Will attempt to close with original assignment amount: {amount_to_close}")
            
            # Place a direct market order to close the position
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Placing market order to close position: amount={amount_to_close}, side={close_side}")
            try:
                order_id = self.place_order(
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    order_type=OrderType.MARKET,
                    side=close_side,
                    amount=amount_to_close,
                    position_action=PositionAction.CLOSE,
                )
                
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Placed close order with ID: {order_id} for amount {amount_to_close} {self.config.trading_pair}")
                
                # Create a tracked order
                self._close_order = TrackedOrder(order_id=order_id)
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Created tracked order for close order: {order_id}")
                
                # Set the executor status to shutting down
                self.close_type = CloseType.TIME_LIMIT
                self._status = RunnableStatus.SHUTTING_DOWN
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Set status to {self._status} and close_type to {self.close_type}")
                
                # Let the control task handle the rest - similar to PositionExecutor
                return
            except Exception as e:
                error_str = str(e).lower()
                # Check for position already closed error using connector's helper method if available
                if hasattr(connector, "_is_position_already_closed_error") and callable(connector._is_position_already_closed_error):
                    if connector._is_position_already_closed_error(e):
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Connector confirmed position already closed: {e}")
                        self.close_type = CloseType.COMPLETED
                        self.stop()
                        return
                # Fallback error message check if connector helper not available
                elif ("would not reduce position" in error_str or 
                      "position not open" in error_str or 
                      "wouldnotreduceposition" in error_str or
                      "position already closed" in error_str):
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Error indicates position already closed: {e}")
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                    return
                else:
                    # For any other error, log it but don't mark as completed yet - we'll retry
                    self.logger().error(f"[ASSIGNMENT_ADAPTER] Error placing close order: {e}", exc_info=True)
                    self._current_retries += 1
                    return
        except Exception as e:
            error_str = str(e).lower()
            # Check for position already closed error using connector's helper method if available
            if hasattr(connector, "_is_position_already_closed_error") and callable(connector._is_position_already_closed_error):
                if connector._is_position_already_closed_error(e):
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Connector confirmed position already closed: {e}")
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                    return
            # Fallback error message check if connector helper not available
            elif ("would not reduce position" in error_str or 
                  "position not open" in error_str or 
                  "wouldnotreduceposition" in error_str or
                  "position already closed" in error_str):
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Error indicates position already closed: {e}")
                self.close_type = CloseType.COMPLETED
                self.stop()
            else:
                # For other errors, don't mark as completed - increment retries and log the error
                self.logger().error(f"[ASSIGNMENT_ADAPTER] Error in close_position_safely: {e}", exc_info=True)
                self._current_retries += 1
    
    async def validate_sufficient_balance_for_closing(self):
        """
        Validate that we have sufficient balance to close the position.
        Unlike regular positions, for assignments we need to validate balance for closing, not opening.
        """
        close_side = self.close_order_side
        
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Validating balance for CLOSE order with side: {close_side}")
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Current assigned amount: {self._assigned_amount}")
        
        try:
            if self.is_perpetual:
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Creating PerpetualOrderCandidate for balance check")
                order_candidate = PerpetualOrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=False,
                    order_type=OrderType.MARKET,
                    order_side=close_side,
                    amount=self._assigned_amount,
                    price=self.entry_price,
                    leverage=Decimal(self.config.leverage),
                )
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Created PerpetualOrderCandidate: {order_candidate}")
            else:
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Creating regular OrderCandidate for balance check")
                order_candidate = OrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=False,
                    order_type=OrderType.MARKET,
                    order_side=close_side,
                    amount=self._assigned_amount,
                    price=self.entry_price,
                )
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Created OrderCandidate: {order_candidate}")
            
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Adjusting order candidates with connector: {self.config.connector_name}")
            adjusted_order_candidates = self.adjust_order_candidates(
                self.config.connector_name, 
                [order_candidate]
            )
            
            # If the adjusted amount is zero, we might not have enough balance
            adjusted_amount = adjusted_order_candidates[0].amount
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Adjusted amount: {adjusted_amount} (original: {self._assigned_amount})")
            
            if adjusted_amount == Decimal("0"):
                self.logger().error(f"[ASSIGNMENT_ADAPTER] Not enough balance to execute assignment closing. Side: {close_side}")
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Will still attempt to close with original amount: {self._assigned_amount}")
                # But we'll still try to close the position with the original amount
                # For assignments, we should try to close even if balance validation fails
                # because it's more important to attempt to close the position than to leave it open
            else:
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Sufficient balance for closing order with amount {adjusted_amount}")
        except Exception as e:
            self.logger().error(f"[ASSIGNMENT_ADAPTER] Error validating balance: {e}", exc_info=True)
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Will still attempt to close position despite balance validation error")
            # Don't fail here, still attempt to close the position

    async def check_if_position_exists(self):
        """
        Check if the position actually exists on the exchange.
        Returns True if position exists, False if not found.
        
        This is similar to the implementation in PositionExecutor.
        """
        try:
            connector = self.connectors.get(self.config.connector_name)
            if not connector or not hasattr(connector, "account_positions"):
                self.logger().warning(f"[ASSIGNMENT_ADAPTER] Cannot check positions - connector {self.config.connector_name} unavailable or doesn't support position tracking")
                return True  # Default to true if we can't verify

            positions = connector.account_positions
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Checking position existence - all keys: {list(positions.keys())}")
            
            position_exists = False
            trading_pair = self.config.trading_pair
            position_amount = Decimal("0")
            
            # First try using position_key helper if available
            if hasattr(connector, "position_key") and callable(connector.position_key):
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Checking for position using position_key method")
                for side in [PositionSide.LONG, PositionSide.SHORT]:
                    pos_key = connector.position_key(trading_pair, side)
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] Checking position_key: {pos_key}")
                    if pos_key in positions:
                        position = positions[pos_key]
                        if abs(position.amount) > Decimal("0"):
                            position_exists = True
                            position_amount = position.amount
                            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Found position using position_key: {pos_key} with amount {position.amount}")
                            break
            
            # If not found, try direct key lookup
            if not position_exists:
                # Try direct lookup with trading pair (ONEWAY mode)
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Trying direct trading pair lookup: {trading_pair}")
                if trading_pair in positions:
                    position = positions[trading_pair]
                    if abs(position.amount) > Decimal("0"):
                        position_exists = True
                        position_amount = position.amount
                        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Found position using direct trading pair lookup with amount {position.amount}")
                        
                # Try appending position sides (HEDGE mode)
                if not position_exists:
                    for side in ["LONG", "SHORT"]:
                        pos_key = f"{trading_pair}{side}"
                        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Checking with appended side: {pos_key}")
                        if pos_key in positions and abs(positions[pos_key].amount) > Decimal("0"):
                            position_exists = True
                            position_amount = positions[pos_key].amount
                            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Found position using {pos_key} with amount {positions[pos_key].amount}")
                            break
                        
            if position_exists:
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Found active position for {trading_pair} with amount {position_amount}")
                return True
            else:
                self.logger().info(f"[ASSIGNMENT_ADAPTER] No active position found for {trading_pair}")
                return False
            
        except Exception as e:
            self.logger().error(f"[ASSIGNMENT_ADAPTER] Error checking position existence: {e}", exc_info=True)
            return True  # Default to true if error

    async def control_task(self):
        """
        Override the control task from PositionExecutor to be more 
        aggressive about closing positions from assignments.
        """
        try:
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] control_task called for assignment: {self._assignment_id}, status: {self.status}")
            
            # If we've been trying to close a position for too long, check if it still exists
            stalled_close_attempt = (
                self.status == RunnableStatus.SHUTTING_DOWN and
                self._current_retries >= 2  # At least a couple retries
            )
            
            if stalled_close_attempt:
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Potential stalled close process detected after {self._current_retries} retries, checking if position still exists")
                position_exists = await self.check_if_position_exists()
                
                if not position_exists:
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Position no longer exists on exchange, marking as completed")
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                    return
            
            # If the executor has been running for a long time without progress, check if position still exists
            if (hasattr(self, "_last_progress_timestamp") and 
                time.time() - self._last_progress_timestamp > 30 and  # 30 seconds without progress
                self.status == RunnableStatus.RUNNING):
                
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Executor appears stalled for {time.time() - self._last_progress_timestamp:.1f} seconds, checking position status")
                if not await self.check_if_position_exists():
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Position is already closed, stopping stalled executor")
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                    return
            
            # If we're running and time_limit is 0, try to close the position immediately
            if self.status == RunnableStatus.RUNNING and self.config.triple_barrier_config.time_limit == 0:
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Time limit is 0, attempting immediate position closure")
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Calling close_position_safely for immediate execution")
                await self.close_position_safely()
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Returned from close_position_safely call, status: {self.status}")
                return
            
            # Periodically check if position already closed (every 5th call, or around every 5 seconds with default update interval)
            # This helps us detect positions that were closed by other means
            if self.status == RunnableStatus.RUNNING and self._current_retries % 5 == 0 and self._current_retries > 0:
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Performing periodic check for already closed position")
                if not await self.check_if_position_exists():
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Position detected as already closed during periodic check")
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                    return
            
            # Otherwise, fallback to the standard control task behavior
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Using standard control flow based on status: {self.status}")
            
            # For assignments, we're only concerned with barriers and shutdown
            if self.status == RunnableStatus.RUNNING:
                # Skip controlling open orders since we're treating this as an already-open position
                # self.control_open_order() - Skip this for assignments
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Skipping open order control since this is an assignment")
                
                # Control barriers (including time_limit which will trigger closure)
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Calling control_barriers")
                self.control_barriers()
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Returned from control_barriers, status: {self.status}")
                
                # Update progress timestamp
                self._last_progress_timestamp = time.time()
            elif self.status == RunnableStatus.SHUTTING_DOWN:
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] In SHUTTING_DOWN state, calling control_shutdown_process")
                await self.control_shutdown_process()
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Returned from control_shutdown_process, status: {self.status}")
                
                # Update progress timestamp
                self._last_progress_timestamp = time.time()
            
            # Check retries
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Evaluating max retries: current={self._current_retries}, max={self._max_retries}")
            self.evaluate_max_retries()
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] After retry evaluation, status: {self.status}")
            
            # Increment retry counter for use in periodic checks
            # This isn't used like normal retries, just as a counter for periodic operations
            if self.status == RunnableStatus.RUNNING:
                self._current_retries += 1
        except Exception as e:
            self.logger().error(f"[ASSIGNMENT_ADAPTER] Error in assignment adapter control task: {e}", exc_info=True)
            # Increment retries on errors to avoid infinite loops
            self._current_retries += 1
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Incremented retries to {self._current_retries} due to error")

    async def control_shutdown_process(self):
        """
        Override to enhance logging during shutdown process.
        """
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] control_shutdown_process called for assignment: {self._assignment_id}")
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Current status: {self.status}, close_type: {self.close_type}")
        
        # Set close timestamp if not already set
        if not hasattr(self, "close_timestamp") or self.close_timestamp is None:
            self.close_timestamp = self._strategy.current_timestamp
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Setting close timestamp to {self.close_timestamp}")
        
        # Log the state of orders
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Open order state: {self._open_order and self._open_order.is_done}")
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Close order state: {self._close_order and self._close_order.is_done}")
        
        # First check if the position is already closed on the exchange
        position_exists = await self.check_if_position_exists()
        if not position_exists:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Position appears to be already closed during shutdown, stopping executor")
            self.close_type = CloseType.COMPLETED
            self.stop()
            return
        
        # Check if open orders are completed
        open_orders_completed = self.open_orders_completed()
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Open orders completed: {open_orders_completed}")
        
        # Check volume matching
        order_execution_completed = self.open_and_close_volume_match()
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Order execution completed: {order_execution_completed}")
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Open filled amount: {self.open_filled_amount}")
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Close filled amount: {self.close_filled_amount}")
        
        # If everything is complete, stop the executor
        if open_orders_completed and order_execution_completed:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Shutdown conditions met, stopping executor")
            self.stop()
        else:
            # Otherwise, continue with the shutdown process
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Shutdown conditions not met, continuing with shutdown process")
            
            # Control close order if needed
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Calling control_close_order")
            await self.control_close_order()
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Returned from control_close_order")
            
            # If the status changed to TERMINATED during the close order control (e.g., position was found to be already closed),
            # don't continue with the shutdown process
            if self.status == RunnableStatus.TERMINATED:
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Executor was terminated during close order control, exiting shutdown process")
                return
            
            # Cancel any remaining open orders
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Calling cancel_open_orders")
            self.cancel_open_orders()
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Returned from cancel_open_orders")
            
            # Increment retry counter
            self._current_retries += 1
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Incremented retries to {self._current_retries}")
        
        # Sleep a bit to allow for order processing
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Sleeping for 5 seconds during shutdown")
        await self._sleep(5.0)
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Woke up from sleep, control_shutdown_process completed") 

    async def control_close_order(self):
        """
        Override to enhance logging during close order control.
        Also detects and handles already-closed positions.
        """
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] control_close_order called for assignment: {self._assignment_id}")
        
        # First check if the position still exists
        position_exists = await self.check_if_position_exists()
        if not position_exists:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Position appears to be already closed, marking as completed")
            self.close_type = CloseType.COMPLETED
            self.stop()
            return
        
        if self._close_order:
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] Close order exists with ID: {self._close_order.order_id}")
            
            # Get the in-flight order from the connector
            in_flight_order = self.get_in_flight_order(
                self.config.connector_name,
                self._close_order.order_id
            ) if not self._close_order.order else self._close_order.order
            
            if in_flight_order:
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Found in-flight order: {in_flight_order.client_order_id}")
                self._close_order.order = in_flight_order
                
                # Get the connector
                connector = self.connectors[self.config.connector_name]
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Updating order with connector: {self.config.connector_name}")
                
                try:
                    # Request order status update
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] Requesting order status update for: {in_flight_order.client_order_id}")
                    await connector._update_orders_with_error_handler(
                        orders=[in_flight_order],
                        error_handler=connector._handle_update_error_for_lost_order
                    )
                    
                    # Log order state after update
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] Order state after update - is_done: {in_flight_order.is_done}, is_filled: {in_flight_order.is_filled}")
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] Executed amount: {in_flight_order.executed_amount_base}/{in_flight_order.amount}")
                    
                    # Check for error messages that indicate the position was already closed
                    if hasattr(in_flight_order, "last_state") and in_flight_order.last_state:
                        error_msg = str(in_flight_order.last_state).lower()
                        
                        # Use connector helper if available
                        if hasattr(connector, "_is_position_already_closed_error") and callable(connector._is_position_already_closed_error):
                            if connector._is_position_already_closed_error(Exception(error_msg)):
                                self.logger().info(f"[ASSIGNMENT_ADAPTER] Connector confirmed position already closed: {error_msg}")
                                self.close_type = CloseType.COMPLETED
                                self.stop()
                                return
                        # Fallback check if connector helper not available
                        elif ("would not reduce position" in error_msg or 
                              "position not open" in error_msg or 
                              "wouldnotreduceposition" in error_msg or
                              "position already closed" in error_msg):
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Detected position already closed from order error: {error_msg}")
                            self.close_type = CloseType.COMPLETED
                            self.stop()
                            return
                    
                    # Check if close order is done or filled
                    if in_flight_order.is_done or in_flight_order.is_filled:
                        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Close order is done or filled, marking as completed")
                        self.close_type = CloseType.COMPLETED
                        self.stop()
                        return
                    
                    # Double-check if position still exists after processing order
                    if not await self.check_if_position_exists():
                        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Position no longer exists after order update, marking as completed")
                        self.close_type = CloseType.COMPLETED
                        self.stop()
                        return
                    
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] Waiting for close order to be filled")
                except Exception as e:
                    error_str = str(e).lower()
                    
                    # Use connector helper if available
                    if hasattr(connector, "_is_position_already_closed_error") and callable(connector._is_position_already_closed_error):
                        if connector._is_position_already_closed_error(e):
                            self.logger().info(f"[ASSIGNMENT_ADAPTER] Connector confirmed position already closed: {error_str}")
                            self.close_type = CloseType.COMPLETED
                            self.stop()
                            return
                    # Fallback check if connector helper not available
                    elif ("would not reduce position" in error_str or 
                          "position not open" in error_str or 
                          "wouldnotreduceposition" in error_str or
                          "position already closed" in error_str):
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] Error indicates position already closed: {e}")
                        self.close_type = CloseType.COMPLETED
                        self.stop()
                        return
                    else:
                        self.logger().error(f"[ASSIGNMENT_ADAPTER] Error updating order: {e}", exc_info=True)
            else:
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Could not find in-flight order for ID: {self._close_order.order_id}")
                
                # Check if position is already closed before adding to failed orders
                if not await self.check_if_position_exists():
                    self.logger().info(f"[ASSIGNMENT_ADAPTER] Position is already closed, marking executor as completed")
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                    return
                    
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Adding to failed orders and resetting close_order")
                self._failed_orders.append(self._close_order)
                self._close_order = None
        else:
            # Before placing a new close order, check if position still exists
            if not await self.check_if_position_exists():
                self.logger().info(f"[ASSIGNMENT_ADAPTER] Position is already closed, no need to place close order")
                self.close_type = CloseType.COMPLETED
                self.stop()
                return
            
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] No close order exists, placing new close order")
            # Place a new close order
            self.place_close_order_and_cancel_open_orders(close_type=self.close_type)
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] After placing close order - close_order: {self._close_order and self._close_order.order_id}")

    def evaluate_max_retries(self):
        """
        This method is responsible for evaluating the maximum number of retries to place an order and stop the executor
        if the maximum number of retries is reached.

        :return: None
        """
        # Check if we've exceeded the maximum retries
        if self._current_retries > self._max_retries:
            self.logger().warning(f"[ASSIGNMENT_ADAPTER] Max retries ({self._max_retries}) exceeded, stopping executor")
            self.close_type = CloseType.FAILED
            self.stop()
            return
        
        # Check for stalled shutdown process
        if (self.status == RunnableStatus.SHUTTING_DOWN and 
            hasattr(self, "_last_progress_timestamp") and 
            time.time() - self._last_progress_timestamp > 60):  # 60 seconds with no progress during shutdown
            
            self.logger().warning(f"[ASSIGNMENT_ADAPTER] Shutdown process stalled for {time.time() - self._last_progress_timestamp:.1f} seconds, forcing completion")
            
            # Check if position exists one last time
            try:
                connector = self.connectors[self.config.connector_name]
                if hasattr(connector, "account_positions"):
                    positions = connector.account_positions
                    position_exists = False
                    trading_pair = self.config.trading_pair
                    
                    # Quick check across all possible position keys
                    for pos_key, pos in positions.items():
                        if trading_pair in pos_key and abs(pos.amount) > Decimal("0"):
                            position_exists = True
                            self.logger().warning(f"[ASSIGNMENT_ADAPTER] Position still exists during forced completion: {pos_key} = {pos.amount}")
                            break
                    
                    if not position_exists:
                        self.logger().info(f"[ASSIGNMENT_ADAPTER] No position found during forced completion, marking as completed")
                        self.close_type = CloseType.COMPLETED
                    else:
                        self.logger().warning(f"[ASSIGNMENT_ADAPTER] Position still exists during forced completion, marking as failed")
                        self.close_type = CloseType.FAILED
                else:
                    self.logger().warning(f"[ASSIGNMENT_ADAPTER] Cannot check positions during forced completion, marking as failed")
                    self.close_type = CloseType.FAILED
            except Exception as e:
                self.logger().error(f"[ASSIGNMENT_ADAPTER] Error checking position during forced completion: {e}")
                self.close_type = CloseType.FAILED
            
            self.stop() 

    def place_close_order_and_cancel_open_orders(self, close_type: CloseType, price: Decimal = Decimal("NaN")):
        """
        Override to improve error handling for already-closed positions
        """
        self.logger().info(f"[ASSIGNMENT_ADAPTER] Attempting to place close order for {self.config.trading_pair}")
        
        # First cancel any open orders
        self.cancel_open_orders()
        
        # Check if the position is already closed using a safer approach
        position_looks_closed = False
        
        try:
            # Directly check if position exists in account positions
            connector = self.connectors[self.config.connector_name]
            if hasattr(connector, "account_positions"):
                positions = connector.account_positions
                
                # Add debug logging for all position keys
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] All position keys in place_close_order: {list(positions.keys())}")
                
                # Add detailed logging for each position
                for pos_key, pos in positions.items():
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] Position details - Key: {pos_key}, "
                                       f"Trading Pair: {pos.trading_pair if hasattr(pos, 'trading_pair') else 'N/A'}, "
                                       f"Side: {pos.position_side}, Amount: {pos.amount}, "
                                       f"Entry Price: {pos.entry_price if hasattr(pos, 'entry_price') else 'N/A'}, "
                                       f"Attrs: {dir(pos)}")
                
                position_exists = False
                trading_pair = self.config.trading_pair
                
                # Check all position keys for this trading pair
                for pos_key, pos in positions.items():
                    if trading_pair in pos_key and abs(pos.amount) > Decimal("0"):
                        position_exists = True
                        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Found position: {pos_key} with amount {pos.amount}")
                        break
                
                if not position_exists:
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] No position found in account positions, considering it already closed")
                    position_looks_closed = True
        except Exception as e:
            self.logger().error(f"[ASSIGNMENT_ADAPTER] Error checking positions: {e}", exc_info=True)
        
        # If position appears to be closed, mark as completed and exit
        if position_looks_closed:
            self.logger().info(f"[ASSIGNMENT_ADAPTER] Position appears to be already closed, no need to place close order")
            self.close_type = CloseType.COMPLETED
            self.stop()
            return
        
        # Get the amount to close
        amount_to_close = self.amount_to_close
        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Amount to close: {amount_to_close}, min order size: {self.trading_rules.min_order_size if hasattr(self, 'trading_rules') else 'unknown'}")
        
        # Check if there's a minimum amount requirement
        if hasattr(self, 'trading_rules') and amount_to_close < self.trading_rules.min_order_size:
            self.logger().warning(f"[ASSIGNMENT_ADAPTER] Amount to close {amount_to_close} is less than min order size {self.trading_rules.min_order_size}")
            # If the amount is too small, we consider it already fully closed
            self.close_type = CloseType.COMPLETED
            self.stop()
            return
        
        # Only place order if amount is greater than zero
        if amount_to_close > Decimal("0"):
            try:
                # Get the close side
                close_side = self.close_order_side
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Using close side: {close_side}")
                
                # Place the order
                order_id = self.place_order(
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    order_type=OrderType.MARKET,
                    amount=amount_to_close,
                    price=price,
                    side=close_side,
                    position_action=PositionAction.CLOSE,
                )
                
                self.logger().debug(f"[ASSIGNMENT_ADAPTER] Successfully placed close order {order_id} for amount {amount_to_close}")
                self._close_order = TrackedOrder(order_id=order_id)
                
                # Keep the normal flow like PositionExecutor, let control_task and control_close_order handle completion
            except Exception as e:
                # Use connector helper method if available
                connector = self.connectors[self.config.connector_name]
                if hasattr(connector, "_is_position_already_closed_error") and callable(connector._is_position_already_closed_error):
                    if connector._is_position_already_closed_error(e):
                        self.logger().debug(f"[ASSIGNMENT_ADAPTER] Connector confirmed position already closed: {e}")
                        self.close_type = CloseType.COMPLETED
                        self.stop()
                        return
                # Fallback check if connector helper not available
                error_str = str(e).lower()
                if ("would not reduce position" in error_str or 
                    "position not open" in error_str or 
                    "wouldnotreduceposition" in error_str or
                    "position already closed" in error_str):
                    self.logger().debug(f"[ASSIGNMENT_ADAPTER] Error indicates position already closed: {e}")
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                    return
                else:
                    self.logger().error(f"[ASSIGNMENT_ADAPTER] Error placing close order: {e}", exc_info=True)
                    self._current_retries += 1
        else:
            self.logger().debug(f"[ASSIGNMENT_ADAPTER] No amount to close, position appears to be already closed")
            self.close_type = CloseType.COMPLETED
            self.stop()
            return
        
        # Set the close type and timestamp
        self.close_type = close_type
        self.close_timestamp = self._strategy.current_timestamp
        self._status = RunnableStatus.SHUTTING_DOWN
        
        # Update progress timestamp
        if hasattr(self, "_last_progress_timestamp"):
            self._last_progress_timestamp = time.time()
        