import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from pydantic.v1 import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import AssignmentFillEvent, MarketEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase

# from hummingbot.strategy_v2.executors.assignment_executor.data_types import AssignmentExecutorConfig
from hummingbot.strategy_v2.executors.assignment_adapter_executor.data_types import AssignmentAdapterExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import TripleBarrierConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.utils.common import generate_unique_id


class AssignmentManagerControllerConfig(ControllerConfigBase):
    """
    Configuration for the AssignmentManagerController

    This controller monitors an exchange for position assignments and creates executors to close them.
    """
    controller_name = "assignment_manager_v1"

    connector_name: str = Field(
        default="kraken_perpetual",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the name of the perpetual exchange (e.g., kraken_perpetual):"
        ),
    )

    trading_pairs: List[str] = Field(
        default=["BTC-USD", "ETH-USD", "SOL-USD"],
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the trading pairs separated by commas (e.g., BTC-USD,ETH-USD) or leave empty for all pairs:"
        ),
    )

    all_trading_pairs: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Watch all trading pairs? (True/False):"
        ),
    )

    order_type: str = Field(
        default="MARKET",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Order type for closing positions (MARKET/LIMIT):"
        ),
    )

    close_percent: Decimal = Field(
        default=Decimal("100"),
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Percentage of position to close (1-100):"
        ),
    )

    slippage_buffer: Decimal = Field(
        default=Decimal("0.001"),
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Slippage buffer for limit orders (e.g., 0.001 for 0.1%):"
        ),
    )

    max_order_age: int = Field(
        default=60,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Maximum age of orders before resubmission (in seconds):"
        ),
    )

    @validator("trading_pairs", pre=True)
    def validate_trading_pairs(cls, v):
        if isinstance(v, str):
            pairs = v.replace(" ", "").split(",")
            return [p for p in pairs if p]
        return v

    def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        """Update the list of markets the strategy needs to track"""
        if self.connector_name not in markets:
            markets[self.connector_name] = set()

        # Add trading pairs to the market
        if not self.all_trading_pairs:
            for trading_pair in self.trading_pairs:
                markets[self.connector_name].add(trading_pair)

        return markets


class AssignmentManagerController(ControllerBase):
    """
    Controller for managing assignments from exchanges like Kraken Perpetual.

    This controller listens for assignment events and creates executors to close positions.
    """
    _logger = None

    # Define event tags as class constants
    ASSIGNMENT_FILL_EVENT_TAG = MarketEvent.AssignmentFill  # Use the enum directly
    ASSIGNMENT_FILL_STRING_EVENT_TAG = "assignment_fill"

    # Add this as a class variable near the beginning of the class
    _last_cleanup_timestamp = 0
    _CLEANUP_MINIMUM_INTERVAL = 30  # Minimum seconds between cleanup operations

    # Add this as another class variable near the beginning
    _last_processed_data_update = 0
    _PROCESSED_DATA_UPDATE_INTERVAL = 15  # Seconds between full updates

    # Add this class variable
    _last_verbose_check_timestamp = 0
    _VERBOSE_CHECK_INTERVAL = 60  # 60 seconds between full verbose checks
    
    # Set of fill_ids currently being processed (to prevent duplicates)
    _processing_fill_ids = set()

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, config: AssignmentManagerControllerConfig, market_data_provider: MarketDataProvider,
                 actions_queue: asyncio.Queue, update_interval: float = 1.0):
        super().__init__(config=config,
                         market_data_provider=market_data_provider,
                         actions_queue=actions_queue,
                         update_interval=update_interval)
        self.config = config
        self.assignments = {}  # Track assignments by fill_id
        self.assigned_executors = {}  # Track executors created for assignments
        self._last_status_report_timestamp = 0
        
        # Create a lock for assignment processing
        self._assignment_lock = asyncio.Lock()

        # Create event listeners using EventForwarder
        self._assignment_fill_listener = EventForwarder(self._on_assignment_fill)

        # Setup markets and listeners
        self._markets = set()
        self._setup_markets()
        
        # Use a background task to periodically check for executor updates
        self._executor_check_task = safe_ensure_future(self._periodic_executor_check())

    def _setup_markets(self):
        """Set up markets and event listeners"""
        connector = self.market_data_provider.get_connector(self.config.connector_name)
        if not connector:
            self.logger().error(f"Could not find connector {self.config.connector_name}")
            return

        self.logger().debug(f"Setting up markets for {self.config.connector_name}")
        
        # Get trading pairs to track
        if self.config.all_trading_pairs:
            try:
                trading_pairs = set(connector.trading_pair)  # Use getter method
                self.logger().debug(f"Watching all trading pairs: {trading_pairs}")
            except AttributeError:
                self.logger().error("Connector does not expose trading pairs. Please configure specific pairs.")
                return
        else:
            trading_pairs = set(self.config.trading_pairs)
            self.logger().debug(f"Watching configured trading pairs: {trading_pairs}")

        # Add market to tracked markets for event listening
        self._add_markets([connector])
        
        self.logger().debug(f"Successfully set up market tracking for {len(trading_pairs)} pairs")

    def _add_markets(self, markets: List[Any]):
        """Add markets and set up their event listeners"""
        for market in markets:
            if market not in self._markets:
                self.logger().debug(f"Adding market {market.name}")
                # Add standard event listener for the enum event
                market.add_listener(MarketEvent.AssignmentFill, self._assignment_fill_listener)
                # Try to add string event listener
                try:
                    market.add_listener(self.ASSIGNMENT_FILL_STRING_EVENT_TAG, self._assignment_fill_listener)
                except Exception as e:
                    self.logger().debug(f"Market does not support string event names: {str(e)}")
                self._markets.add(market)

    def _remove_markets(self, markets: List[Any]):
        """Remove markets and their event listeners"""
        for market in markets:
            if market in self._markets:
                self.logger().debug(f"Removing market {market.name}")
                market.remove_listener(MarketEvent.AssignmentFill, self._assignment_fill_listener)
                try:
                    market.remove_listener(self.ASSIGNMENT_FILL_STRING_EVENT_TAG, self._assignment_fill_listener)
                except:
                    pass
                self._markets.remove(market)

    def stop(self):
        """Override the stop method to also cancel our periodic task"""
        # Cancel our periodic task
        if hasattr(self, '_executor_check_task') and self._executor_check_task is not None:
            self._executor_check_task.cancel()
            
        # Clean up when the controller is stopped
        self._remove_markets(list(self._markets))
            
        super().stop()

    def _on_assignment_fill(self, event: AssignmentFillEvent):
        """
        Handler for assignment fill events.
        Creates executors to close the assigned positions.
        """
        # Extract key information from the event first before acquiring lock
        fill_id = getattr(event, "fill_id", None)
        if not fill_id:
            self.logger().error("[ASSIGNMENT] Assignment event missing fill_id")
            return

        # Check if already being processed without acquiring the full lock
        if fill_id in self._processing_fill_ids:
            self.logger().debug(f"[ASSIGNMENT] Already processing assignment {fill_id}, skipping duplicate")
            return
            
        # Create a task to process this with the lock
        safe_ensure_future(self._process_assignment_fill_with_lock(event, fill_id))
        
    async def _process_assignment_fill_with_lock(self, event: AssignmentFillEvent, fill_id: str):
        """Process assignment fill with mutex lock protection"""
        # Double-check if already being processed before trying to acquire lock
        if fill_id in self._processing_fill_ids:
            self.logger().debug(f"[ASSIGNMENT] Already processing assignment {fill_id}, skipping duplicate (pre-lock check)")
            return
            
        # Add to processing set to prevent parallel processing
        self._processing_fill_ids.add(fill_id)
        
        try:
            # Acquire the lock for critical section
            async with self._assignment_lock:
                trading_pair = getattr(event, "trading_pair", "")
                if not trading_pair:
                    self.logger().error("[ASSIGNMENT] Assignment event missing trading_pair")
                    return

                # Ensure trading pair is valid
                if not self._is_valid_trading_pair(trading_pair):
                    self.logger().warning(f"[ASSIGNMENT] Trading pair {trading_pair} not known to connector")
                    if not self.config.all_trading_pairs:
                        self.logger().debug(f"[ASSIGNMENT] Ignoring assignment for {trading_pair} as it's not in watched pairs")
                        return

                # Check if this assignment already exists
                if fill_id in self.assignments:
                    self.logger().debug(f"[ASSIGNMENT] Assignment {fill_id} already exists, skipping")
                    return

                # Store assignment info with EXECUTING status directly (no PENDING state)
                self.assignments[fill_id] = {
                    "event": event,
                    "timestamp": getattr(event, "timestamp", int(time.time() * 1000)),
                    "trading_pair": trading_pair,
                    "position_side": getattr(event, "position_side", "UNKNOWN"),
                    "amount": getattr(event, "amount", 0),
                    "price": getattr(event, "price", 0),
                    "order_id": getattr(event, "order_id", ""),
                    "status": "EXECUTING",  # Start as EXECUTING directly
                    "executor_id": None
                }

                self.logger().info(f"[ASSIGNMENT] Received assignment fill: {trading_pair} "
                                  f"{getattr(event, 'position_side', 'UNKNOWN')} "
                                  f"amount={getattr(event, 'amount', 0)} "
                                  f"price={getattr(event, 'price', 0)} "
                                  f"fill_id={fill_id}")

                # Create executor config
                config = self._create_executor_config(fill_id)
                if not config:
                    self.logger().error(f"[IMMEDIATE CREATION] Failed to create config for assignment {fill_id}")
                    return
                
                # Mark executor in assignments with actual ID (not pending)
                self.assignments[fill_id]["executor_id"] = config.id
                self.logger().info(f"[IMMEDIATE CREATION] Created executor with ID {config.id} for assignment {fill_id}")
                
                # Create the action
                action = CreateExecutorAction(
                    controller_id=self.config.id,
                    executor_config=config
                )
                
                # Put the action in a list before putting it in the queue
                # This is because the listener expects a list of actions
                actions = [action]
                
                # Put the actions list directly into the queue
                await self.actions_queue.put(actions)
                self.logger().info(f"[IMMEDIATE CREATION] Queued executor action with ID {config.id} for assignment {fill_id}")
                
                # Also record in our assigned_executors dict for future reference
                self.assigned_executors[config.id] = {
                    "fill_id": fill_id, 
                    "timestamp": time.time(),
                    "status": "ACTIVE",
                    "config": config
                }
                self.logger().debug(f"[EXECUTOR TRACKING] Assignment {fill_id} now linked to executor {config.id}")
                
        except Exception as e:
            self.logger().error(f"[ASSIGNMENT] Error processing assignment fill: {e}", exc_info=True)
        finally:
            # Remove from processing set when done
            if fill_id in self._processing_fill_ids:
                self._processing_fill_ids.remove(fill_id)

    async def _immediate_executor_creation(self, fill_id: str):
        """
        Immediately create an executor for a new assignment without waiting for the next update cycle.
        This method is now deprecated in favor of creating the executor directly in _process_assignment_fill_with_lock.
        
        :param fill_id: The ID of the assignment to create an executor for
        """
        self.logger().warning(f"[IMMEDIATE CREATION] _immediate_executor_creation is deprecated, skipping for {fill_id}")
        return  # Skip this method as it's now handled in the locked processing

    async def can_create_assignment_executor(self, fill_id: str) -> bool:
        """
        Check if an assignment executor can be created for a specific fill_id.
        
        Args:
            fill_id: The unique identifier of the assignment fill event
            
        Returns:
            bool: True if an executor can be created, False otherwise
        """
        # Use lock to prevent races when checking executor status
        async with self._assignment_lock:
            # Check if the assignment exists
            if fill_id not in self.assignments:
                self.logger().warning(f"[EXECUTOR CHECK] Cannot create executor for unknown assignment {fill_id}")
                return False
            
            assignment = self.assignments[fill_id]
            
            # Check if assignment is already closed
            if assignment.get("status") == "CLOSED":
                self.logger().debug(f"[EXECUTOR CHECK] Assignment {fill_id} is already closed")
                return False
            
            # Don't create if we already have an executor ID assigned
            if assignment.get("executor_id") is not None:
                self.logger().debug(f"[EXECUTOR CHECK] Assignment {fill_id} already has executor_id {assignment.get('executor_id')}")
                return False
            
            # Look for existing executors already handling this assignment
            for executor_info in self.executors_info:
                # Check if this executor has the matching assignment_id
                if (hasattr(executor_info.config, 'assignment_id') and 
                    executor_info.config.assignment_id == fill_id):
                    
                    self.logger().debug(f"[EXECUTOR CHECK] Found existing executor {executor_info.id} for assignment {fill_id}")
                    
                    # Update assignment tracking
                    assignment["executor_id"] = executor_info.id
                    assignment["status"] = "EXECUTING" if executor_info.is_active else "CLOSED"
                    
                    # Update executor tracking
                    self.assigned_executors[executor_info.id] = {
                        "fill_id": fill_id,
                        "timestamp": time.time(),
                        "status": "ACTIVE" if executor_info.is_active else "COMPLETED",
                        "config": executor_info.config,
                    }
                    
                    return False  # Don't create a new executor
            
            # If we get here, no executor exists for this assignment
            self.logger().debug(f"[EXECUTOR CHECK] No existing executor found for assignment {fill_id}, can create one")
            return True

    def create_actions_proposal(self) -> List[ExecutorAction]:
        """
        Create proposals for new executors based on assignments without executors.
        """
        create_actions = []
        
        # Using a list to avoid modifying dict during iteration
        assignments_to_check = list(self.assignments.items())
        self.logger().info(f"[EXECUTOR CREATION] Starting executor creation proposal, checking {len(assignments_to_check)} assignments")
        
        # Create executors for assignments without executors
        for fill_id, assignment in assignments_to_check:
            # Skip if this assignment is already closed
            if assignment.get("status") == "CLOSED":
                continue
            
            # Skip if this assignment already has an executor
            if assignment.get("executor_id"):
                self.logger().info(f"[EXECUTOR CREATION] Assignment {fill_id} already has executor {assignment.get('executor_id')}")
                continue
            
            # Check if we can create an executor - without awaiting to avoid deadlocks in this sync method
            # This isn't ideal, but since create_actions_proposal must be synchronous, we can do a basic check here
            if fill_id in self._processing_fill_ids:
                self.logger().debug(f"[EXECUTOR CREATION] Assignment {fill_id} is currently being processed, skipping")
                continue
                
            trading_pair = assignment.get("trading_pair", "UNKNOWN")
            self.logger().info(f"[EXECUTOR CREATION] Assignment {fill_id}: status={assignment.get('status')}, executor_id={assignment.get('executor_id')}, trading_pair={trading_pair}")
            
            # Create executor config
            config = self._create_executor_config(fill_id)
            if not config:
                self.logger().error(f"[EXECUTOR CREATION] Failed to create config for assignment {fill_id}")
                continue
            
            # Create the action
            action = CreateExecutorAction(
                controller_id=self.config.id,
                executor_config=config
            )

            create_actions.append(action)
            self.logger().debug(f"[EXECUTOR CREATION] Created executor action for assignment {fill_id}")
        
        if create_actions:
            self.logger().debug(f"[EXECUTOR CREATION] Created {len(create_actions)} executor actions in total")
                
        return create_actions

    def on_executor_created(self, executor_id: str, config: AssignmentAdapterExecutorConfig):
        """
        Called when an executor is created. Adds it to our tracking.
        """
        self.logger().debug(f"[EXECUTOR NOTIFICATION] Executor created with ID {executor_id}")
        
        # First check if this is an assignment executor
        if not hasattr(config, 'assignment_id'):
            self.logger().debug(f"[EXECUTOR NOTIFICATION] Not an assignment executor (no assignment_id attribute)")
            return
        
        fill_id = config.assignment_id
        self.logger().debug(f"[EXECUTOR NOTIFICATION] Found assignment_id: {fill_id} in executor config")
        
        # Check if this assignment exists in our records
        if fill_id not in self.assignments:
            self.logger().warning(f"[EXECUTOR NOTIFICATION] Assignment {fill_id} not found in our records")
            return
        
        # Get the assignment
        assignment = self.assignments[fill_id]
        
        # Update the assignment to point to the new executor
        assignment["executor_id"] = executor_id
        assignment["status"] = "EXECUTING"
        
        # Add the executor to our tracking
        self.assigned_executors[executor_id] = {
            "fill_id": fill_id,
            "timestamp": time.time(),
            "status": "ACTIVE",
            "config": config
        }
        
        self.logger().debug(f"[EXECUTOR NOTIFICATION] Updated assignment {fill_id} to use executor {executor_id}")
        
        # Trigger an update to ensure everything is in sync
        self.executors_update_event.set()

    def _create_executor_config(self, fill_id: str) -> Optional[AssignmentAdapterExecutorConfig]:
        """
        Create an executor config for the given assignment.
        This method creates an AssignmentAdapterExecutorConfig which adapts the PositionExecutor
        to handle assignments.
        """
        try:
            self.logger().debug(f"[CONFIG CREATION] Creating adapter executor config for assignment {fill_id}")
            
            assignment = self.assignments[fill_id]
            event = assignment["event"]

            # Generate a unique ID for this executor config
            exec_id = generate_unique_id()
            self.logger().debug(f"[CONFIG CREATION] Generated unique ID {exec_id} for assignment {fill_id}")

            # Determine trade side (opposite of position side)
            side = TradeType.SELL if event.position_side == PositionSide.LONG else TradeType.BUY
            self.logger().debug(f"[CONFIG CREATION] Determined side {side} for assignment {fill_id} with position side {event.position_side}")

            # Calculate amount based on close percentage
            amount = Decimal(str(event.amount)) * (self.config.close_percent / Decimal("100"))
            self.logger().debug(f"[CONFIG CREATION] Calculated amount {amount} for assignment {fill_id} (original: {event.amount}, close %: {self.config.close_percent})")
            
            # Create triple barrier config for position management
            # Setting time_limit to 0 will cause immediate closure
            triple_barrier = TripleBarrierConfig(
                stop_loss=None,         # No stop loss by default
                take_profit=None,       # No take profit by default
                time_limit=0,           # Set to 0 to close immediately
                trailing_stop=None,     # No trailing stop by default
                open_order_type=OrderType.MARKET,  # Not used for assignments but required
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,  # Using MARKET order for immediate execution
            )
            
            self.logger().debug(f"[CONFIG CREATION] Setting time_limit=0 for immediate position closure")
            
            # Create the new adapter config
            config = AssignmentAdapterExecutorConfig(
                id=exec_id,
                type="assignment_adapter_executor",  # Ensure type is set explicitly
                timestamp=time.time(),
                controller_id=self.config.id,
                connector_name=self.config.connector_name,
                trading_pair=event.trading_pair,
                side=side,
                amount=amount,
                entry_price=Decimal(str(event.price)),
                position_action=PositionAction.CLOSE,  # Always closing for assignments
                triple_barrier_config=triple_barrier,
                leverage=1,  # Default leverage
                activation_bounds=None,
                level_id=None,
                assignment_id=fill_id  # Store the assignment ID
            )
            
            # Double-check that assignment_id is set
            if not hasattr(config, 'assignment_id') or not config.assignment_id:
                self.logger().warning(f"[CONFIG CREATION] assignment_id not found in created config, manually setting it")
                config.assignment_id = fill_id
            
            # Ensure these specific keys are set for easy debugging
            self.logger().debug(f"[CONFIG CREATION] Successfully created adapter executor config {exec_id} for assignment {fill_id}")
            self.logger().debug(f"[CONFIG CREATION] Config details: id={config.id}, type={config.type}, "
                              f"assignment_id={config.assignment_id}, "
                              f"trading_pair={config.trading_pair}, entry_price={config.entry_price}, "
                              f"time_limit={config.triple_barrier_config.time_limit}")
                              
            return config
            
        except Exception as e:
            self.logger().error(f"[CONFIG CREATION] Error creating executor config for {fill_id}: {e}", exc_info=True)
            return None

    def _is_valid_trading_pair(self, trading_pair: str) -> bool:
        """Check if trading pair is valid and known to the connector"""
        if not trading_pair:
            return False

        try:
            connector = self.market_data_provider.get_connector(self.config.connector_name)
            
            # First check if the pair exists in the connector's trading pairs
            if trading_pair in connector.trading_pairs:
                return True

            # If watching all pairs, check if the pair format is valid
            if self.config.all_trading_pairs:
                try:
                    # Try to convert the trading pair format
                    converted_pair = connector.convert_to_exchange_trading_pair(trading_pair)
                    return converted_pair is not None
                except Exception:
                    return False

            # Only accept pairs in our configured list
            return trading_pair in self.config.trading_pairs

        except Exception as e:
            self.logger().error(f"Error checking trading pair {trading_pair}: {e}")
            return False

    async def on_executor_completed(self, executor_id: str, close_type: str = None, reason: str = None):
        """
        Handle notification when an executor has completed its task.
        
        :param executor_id: The ID of the completed executor
        :param close_type: The type of completion (e.g., "COMPLETED", "FAILED", "CANCELLED")
        :param reason: Additional reason information
        """
        if executor_id not in self.assigned_executors:
            self.logger().warning(f"Received completion notification for unknown executor: {executor_id}")
            return
            
        # Get the assignment ID for this executor
        assignment_info = self.assigned_executors[executor_id]
        fill_id = assignment_info.get("fill_id")
        
        if not fill_id or fill_id not in self.assignments:
            self.logger().warning(f"Cannot find assignment for executor: {executor_id}")
            return
            
        # Update the assignment status based on the close type
        if close_type in ["COMPLETED", "SUCCESS"]:
            self.assignments[fill_id]["status"] = "CLOSED"
            self.logger().info(f"Assignment {fill_id} marked as CLOSED (executor {executor_id} completed successfully)")
        elif close_type in ["FAILED", "ERROR"]:
            self.assignments[fill_id]["status"] = "FAILED"
            self.assignments[fill_id]["error"] = reason or "Executor failed"
            self.logger().error(f"Assignment {fill_id} marked as FAILED: {reason}")
        else:
            # Unknown or null close type - mark as inactive but don't change status
            self.logger().warning(f"Executor {executor_id} completed with unknown close type: {close_type}")
            
        # Update tracking info
        self.assigned_executors[executor_id]["status"] = "COMPLETED"
        self.assigned_executors[executor_id]["close_type"] = close_type
        self.assigned_executors[executor_id]["reason"] = reason
        
        # Trigger any additional post-processing if needed
        await self._process_completed_assignment(fill_id, executor_id, close_type)
        
    async def _process_completed_assignment(self, fill_id: str, executor_id: str, close_type: str):
        """
        Perform any post-processing after an assignment has been completed.
        Override this in subclasses if needed.
        
        :param fill_id: The assignment ID
        :param executor_id: The executor ID
        :param close_type: How the execution was completed
        """
        pass

    def on_executor_failed(self, executor_id: str, exception: Exception) -> None:
        """Handle failed executors"""
        if executor_id in self.assigned_executors:
            executor_info = self.assigned_executors[executor_id]
            fill_id = executor_info["fill_id"]

            # Update assignment status
            if fill_id in self.assignments:
                self.assignments[fill_id]["status"] = "FAILED"
                self.logger().error(f"Assignment {fill_id} closing failed: {exception}")
                
                # Since we're removing the executor, we should also remove the assignment if it's a failure
                # Get the timestamp to determine if we should remove now
                timestamp = self.assignments[fill_id].get("timestamp", 0)
                if timestamp > 1000000000000:  # If in milliseconds
                    timestamp = timestamp / 1000
                    
                # If the failure is recent (within last 10 minutes), keep the assignment for debugging
                # Otherwise, immediately remove both executor and assignment
                current_time = time.time()
                if current_time - timestamp > 600:  # 10 minutes
                    self.logger().debug(f"Immediately removing failed assignment {fill_id} (age: {(current_time - timestamp) / 60:.1f} minutes)")
                    del self.assignments[fill_id]

            # Update executor status
            executor_info["status"] = "FAILED"

            # Immediately remove this failed executor
            self.logger().debug(f"Immediately removing failed executor {executor_id} for assignment {fill_id}")
            del self.assigned_executors[executor_id]
            
            # Still run periodic cleanup for other completed items
            self._clean_up_old_records()
            
        # Also check if there's a pending executor ID that matches this failed executor
        for fill_id, assignment in list(self.assignments.items()):
            if assignment.get("executor_id") == f"pending_{executor_id}":
                # Update the assignment with the actual executor ID and mark as FAILED
                self.logger().info(f"Updating assignment {fill_id} from pending to FAILED (executor {executor_id} failed)")
                assignment["executor_id"] = executor_id
                assignment["status"] = "FAILED"

    def _clean_up_old_records(self, force=False):
        """Clean up old completed records to prevent memory leaks"""
        # Skip cleanup if it was run recently, unless forced
        current_time = time.time()
        if not force and current_time - self._last_cleanup_timestamp < self._CLEANUP_MINIMUM_INTERVAL:
            self.logger().debug(f"[CLEANUP] Skipping cleanup as last run was only {current_time - self._last_cleanup_timestamp:.1f} seconds ago")
            return
            
        # Update the cleanup timestamp
        self._last_cleanup_timestamp = current_time
        
        self.logger().debug("[CLEANUP] Starting cleanup of old records")
        
        cutoff_time = current_time - 3600  # 1 hour for normal cleanup
        orphan_cutoff_time = current_time - 300  # 5 minutes for orphaned executors
        
        # Keep track of what we cleaned up
        assignments_cleaned = 0
        executors_cleaned = 0
        orphaned_executors_cleaned = 0

        # Clean up old assignments
        for fill_id in list(self.assignments.keys()):
            assignment = self.assignments[fill_id]
            if assignment["status"] in ["CLOSED", "FAILED"]:
                # Check if this assignment has already had its executor removed
                executor_id = assignment.get("executor_id")
                executor_already_removed = executor_id is not None and executor_id not in self.assigned_executors
                
                # Convert timestamp to seconds if needed
                timestamp = assignment["timestamp"]
                if timestamp > 1000000000000:  # If in milliseconds
                    timestamp = timestamp / 1000

                # Immediately clean up if executor is already gone, otherwise use normal time cutoff
                if executor_already_removed or timestamp < cutoff_time:
                    cleanup_reason = "executor already removed" if executor_already_removed else f"age: {(current_time - timestamp) / 60:.1f} minutes"
                    self.logger().debug(f"[CLEANUP] Removing assignment {fill_id} (status: {assignment['status']}, {cleanup_reason})")
                    del self.assignments[fill_id]
                    assignments_cleaned += 1

        # Clean up old executors
        for executor_id in list(self.assigned_executors.keys()):
            executor_info = self.assigned_executors[executor_id]
            
            # Check if this is an orphaned executor (has no valid fill_id)
            is_orphaned = False
            fill_id = executor_info.get("fill_id")
            if fill_id is None or fill_id not in self.assignments:
                is_orphaned = True
                cleanup_cutoff = orphan_cutoff_time  # Use shorter timeout for orphans
            else:
                cleanup_cutoff = cutoff_time  # Use normal timeout for valid executors
            
            # Remove either old completed executors or orphaned executors
            if executor_info["status"] in ["COMPLETED", "FAILED"] or is_orphaned:
                # Use the timestamp key which is what we set when creating the entry
                timestamp = executor_info["timestamp"]
                if timestamp < cleanup_cutoff:
                    if is_orphaned:
                        self.logger().debug(f"[CLEANUP] Removing orphaned executor {executor_id} (status: {executor_info['status']}, age: {(current_time - timestamp) / 60:.1f} minutes)")
                        del self.assigned_executors[executor_id]
                        orphaned_executors_cleaned += 1
                    else:
                        self.logger().debug(f"[CLEANUP] Removing old executor {executor_id} for assignment {fill_id} (status: {executor_info['status']}, age: {(current_time - timestamp) / 60:.1f} minutes)")
                        del self.assigned_executors[executor_id]
                        executors_cleaned += 1
                    
        # Report cleanup results
        if assignments_cleaned > 0 or executors_cleaned > 0 or orphaned_executors_cleaned > 0:
            self.logger().debug(f"[CLEANUP] Removed {assignments_cleaned} assignments, {executors_cleaned} executors, and {orphaned_executors_cleaned} orphaned executors")
        else:
            self.logger().debug("[CLEANUP] No old records found to clean up")

    def _dump_executor_details(self, executor_info):
        """Dump all details about an executor for debugging purposes"""
        config = executor_info.config
        
        self.logger().debug(f"[DEBUG] Executor ID: {executor_info.id}")
        self.logger().debug(f"[DEBUG] Executor Type: {executor_info.type}")
        self.logger().debug(f"[DEBUG] Executor Status: {executor_info.status}")
        self.logger().debug(f"[DEBUG] Executor Is Active: {executor_info.is_active}")
        
        # Try to get config type
        config_type = type(config).__name__
        self.logger().debug(f"[DEBUG] Config Type: {config_type}")
        
        # List all config attributes
        try:
            config_dict = config.dict() if hasattr(config, 'dict') else vars(config)
            self.logger().debug(f"[DEBUG] Config attributes: {config_dict}")
        except Exception as e:
            self.logger().debug(f"[DEBUG] Could not get config attributes: {e}")
            
            # Try to get individual attributes
            for attr in ['id', 'type', 'controller_id', 'connector_name', 'trading_pair', 'side', 
                         'amount', 'entry_price', 'assignment_id']:
                if hasattr(config, attr):
                    self.logger().debug(f"[DEBUG] Config.{attr}: {getattr(config, attr)}")

    async def update_processed_data(self):
        """
        Update the processed data used by the controller.
        This optimized approach minimizes API calls while maintaining sync with executor state.
        """
        current_time = time.time()
        
        # First do a lightweight check - if it's been less than our minimum interval since the last full check,
        # and we don't have many executors to track, skip the full update
        if (current_time - self._last_processed_data_update < self._PROCESSED_DATA_UPDATE_INTERVAL and 
                len(self.assigned_executors) < 10):  # Only skip if we're tracking a reasonable number
            self.logger().debug(f"[EXECUTOR SYNC] Skipping full sync as last update was {current_time - self._last_processed_data_update:.1f} seconds ago")
            return
            
        self._last_processed_data_update = current_time
        self.logger().debug(f"[EXECUTOR SYNC] Starting executor state synchronization with {len(self.executors_info)} executors")
        
        # Get the current set of executor IDs from our tracking
        tracked_executor_ids = set(self.assigned_executors.keys())
        active_executor_ids = set()
        orphaned_executors = []
        
        self.logger().debug(f"[EXECUTOR SYNC] Currently tracking {len(tracked_executor_ids)} executors in local dictionary")
        
        # Process all the current executors with more logging
        assignment_executor_count = 0
        for executor_info in self.executors_info:
            # Log each executor
            executor_id = executor_info.id
            executor_type = executor_info.type
            is_active = executor_info.is_active
            
            # Check if this is an assignment executor either by assignment_id or by type
            is_assignment_executor = False
            fill_id = None
            
            # Simplify the process of matching executors to assignments
            # Just check for assignment_id attribute
            if hasattr(executor_info.config, 'assignment_id'):
                fill_id = executor_info.config.assignment_id
                is_assignment_executor = True
            
            # Skip if not an assignment executor
            if not is_assignment_executor:
                continue
            
            # Count assignment executors
            assignment_executor_count += 1
            
            # Add to active IDs
            active_executor_ids.add(executor_id)
            
            # Check if this executor is already tracked
            if executor_id in tracked_executor_ids:
                # Just update the status
                if is_active:
                    self.assigned_executors[executor_id]["status"] = "ACTIVE"
                else:
                    self.assigned_executors[executor_id]["status"] = "COMPLETED"
                
            else:
                # New executor we haven't seen before
                if fill_id and fill_id in self.assignments:
                    # Update the assignment to point to this executor
                    assignment = self.assignments[fill_id]
                    assignment["executor_id"] = executor_id
                    assignment["status"] = "EXECUTING" if is_active else "CLOSED"
                    
                    # Add to our tracking
                    self.assigned_executors[executor_id] = {
                        "fill_id": fill_id,
                        "timestamp": current_time,
                        "status": "ACTIVE" if is_active else "COMPLETED",
                        "config": executor_info.config
                    }
                else:
                    # Orphaned executor - no matching assignment
                    orphaned_executors.append((executor_id, executor_info))
        
        # Look for assignments without executors
        for fill_id, assignment in self.assignments.items():
            executor_id = assignment.get("executor_id")
            
            # If the assignment has an executor ID but it's not in active_executor_ids,
            # the executor doesn't exist anymore (it's done or was never created)
            if executor_id and executor_id not in active_executor_ids:
                # If the assignment is already CLOSED, we don't need to do anything
                if assignment.get("status") == "CLOSED":
                    continue
                
                # Otherwise, reset it so a new executor can be created
                assignment["executor_id"] = None
                assignment["status"] = "EXECUTING"  # Keep as executing rather than pending
                self.logger().debug(f"[EXECUTOR SYNC] Resetting assignment {fill_id} as its executor {executor_id} is no longer active")
                
                # Queue immediate executor creation
                safe_ensure_future(self._immediate_executor_creation(fill_id))
        
        # Clean up old records
        self._clean_up_old_records()

    async def _check_for_unassigned_assignments(self):
        """
        Check for assignments without executors and tries to fix them.
        """
        current_time = time.time()
        
        # Find assignments without executors that should have them
        unassigned_assignments = []
        for fill_id, assignment in self.assignments.items():
            # Only look for assignments that should be running (not closed)
            if assignment.get("status") == "CLOSED":
                continue
            
            if assignment.get("executor_id") is None:
                unassigned_assignments.append(fill_id)
        
        if not unassigned_assignments:
            return
        
        self.logger().debug(f"[ASSIGNMENT CHECK] Found {len(unassigned_assignments)} assignments without executors")
        
        # Try to fix each assignment
        for fill_id in unassigned_assignments:
            self.logger().debug(f"[ASSIGNMENT CHECK] Trying to create executor for assignment {fill_id}")
            
            # Look for any executor that might be handling this assignment
            matching_executor = None
            for executor_info in self.executors_info:
                if (hasattr(executor_info.config, 'assignment_id') and 
                    executor_info.config.assignment_id == fill_id):
                    matching_executor = executor_info
                    self.logger().debug(f"[ASSIGNMENT CHECK] Found matching executor {executor_info.id}")
                    break
            
            # If we found a matching executor, update the assignment
            if matching_executor:
                self.logger().debug(f"[ASSIGNMENT CHECK] Updating assignment {fill_id} to point to executor {matching_executor.id}")
                assignment = self.assignments[fill_id]
                assignment["executor_id"] = matching_executor.id
                assignment["status"] = "EXECUTING" if matching_executor.is_active else "CLOSED"
                
                # Also update our tracking of assigned executors
                self.assigned_executors[matching_executor.id] = {
                    "fill_id": fill_id,
                    "timestamp": current_time,
                    "status": "ACTIVE" if matching_executor.is_active else "COMPLETED",
                    "config": matching_executor.config
                }
            else:
                # No matching executor found, create a new one
                self.logger().debug(f"[ASSIGNMENT CHECK] No matching executor found for {fill_id}, creating a new one")
                safe_ensure_future(self._immediate_executor_creation(fill_id))

    async def _periodic_executor_check(self):
        """
        Periodically check executor status and update assignments accordingly.
        This simpler implementation just checks if executors exist and their status.
        """
        while True:
            try:
                await asyncio.sleep(5.0)  # Check every 5 seconds
                
                # Get current set of active executors
                active_executor_ids = set()
                completed_executor_ids = set()
                
                # Process all executors and categorize them
                for executor_info in self.executors_info:
                    if hasattr(executor_info.config, 'assignment_id'):
                        if executor_info.is_active:
                            active_executor_ids.add(executor_info.id)
                        else:
                            completed_executor_ids.add(executor_info.id)
                
                # Update our tracking for any assignments with executors not found in either set
                for fill_id, assignment in list(self.assignments.items()):
                    if assignment.get("status") == "CLOSED":
                        continue  # Skip already closed assignments
                        
                    executor_id = assignment.get("executor_id")
                    if not executor_id:
                        continue  # Skip assignments without executors
                        
                    # Check if this executor is still valid
                    if executor_id not in active_executor_ids and executor_id not in completed_executor_ids:
                        # Missing executor - likely it completed but we didn't catch the notification
                        # Check if it has been missing for a while (over 30 seconds)
                        executor_missing_time = time.time() - self.assignments[fill_id].get("timestamp", time.time())
                        if executor_missing_time > 30:
                            self.logger().debug(f"[EXECUTOR SYNC] Executor {executor_id} for assignment {fill_id} not found (missing for {executor_missing_time:.1f}s)")
                            
                            # If it's been missing a while, check if we should reset it or mark it complete
                            # For safety, check if the position still exists on exchange before resetting
                            
                            # For now, we'll just reset it to allow retry
                            self.logger().info(f"[EXECUTOR SYNC] Resetting assignment {fill_id} as its executor {executor_id} was not found")
                            assignment["executor_id"] = None
                
                # Periodically run cleanup
                if time.time() - self._last_cleanup_timestamp > self._CLEANUP_MINIMUM_INTERVAL:
                    self._clean_up_old_records()
                
            except asyncio.CancelledError:
                # Task was cancelled, exit gracefully
                self.logger().debug("Periodic executor check task cancelled")
                break
            except Exception as e:
                self.logger().error(f"Error in periodic executor check: {e}", exc_info=True)

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determine actions based on assignments and existing executors.
        """
        actions = []
        actions.extend(self.create_actions_proposal())
        actions.extend(self.stop_actions_proposal())
        return actions

    def stop_actions_proposal(self) -> List[ExecutorAction]:
        """
        Create stop actions for executors that need to be stopped.
        """
        return []  # No stop actions needed for now

    def to_format_status(self) -> List[str]:
        """
        Format status to show assignment stats
        """
        lines = []
        lines.append("\nAssignment Manager Status:")
        lines.append(f"  Total Assignments: {len(self.assignments)}")
        lines.append(f"  Active Executors: {len(self.assigned_executors)}")

        # Show active assignments
        active_assignments = {k: v for k, v in self.assignments.items()
                              if v["status"] not in ["CLOSED", "FAILED"]}
        if active_assignments:
            lines.append("\nActive Assignments:")
            for fill_id, assignment in active_assignments.items():
                lines.append(f"  {fill_id}: {assignment['trading_pair']} "
                             f"{assignment['position_side']} "
                             f"amount={assignment['amount']} "
                             f"status={assignment['status']}")

        return lines 

    async def create_executor(
        self,
        fill_id: str,
        assignment: Dict,
        executor_type: str,
    ) -> str:
        """
        Create a position executor to handle this assignment.
        
        :param fill_id: ID of the position fill event.
        :param assignment: Assignment data.
        :param executor_type: Type of executor to create.
        :return: The ID of the created executor.
        """
        self.logger().info(f"Creating {executor_type} executor for assignment: {fill_id}")
        
        executor_config = {}
        executor_id = None
        try:
            # Extract basic details from the assignment
            exchange = assignment.get("exchange")
            trading_pair = assignment.get("trading_pair")
            side = assignment.get("side")
            
            # Create the executor config based on executor type
            if executor_type == "ClosePositionExecutor":
                position_config = {
                    "exchange": exchange,
                    "trading_pair": trading_pair,
                    "amount": assignment.get("amount", 0),
                    "leverage": assignment.get("leverage", 0),
                    "side": side,
                    "position_mode": PositionMode.HEDGE,
                    "assignment_id": fill_id,  # Store the fill ID in the executor for future reference
                }
                
                order_config = {
                    "order_id": f"close_position_{fill_id}",
                    "exchange": exchange,
                    "trading_pair": trading_pair,
                    "side": PositionSide.LONG.name if side == PositionSide.SHORT.name else PositionSide.SHORT.name,
                    "amount_type": AmountType.POSITION_AMOUNT,
                    "amount": assignment.get("amount", 0),
                    "order_type": "MARKET",
                    "price": 0,  # Market order
                    "position_action": PositionAction.CLOSE,
                    "position_mode": PositionMode.HEDGE
                }
                
                executor_config = {
                    "id": f"close_{fill_id}",
                    "exchange": exchange,
                    "trading_pair": trading_pair,
                    "side": side,
                    "amount": assignment.get("amount", 0),
                    "leverage": assignment.get("leverage", 0),
                    "position_config": position_config,
                    "order_config": order_config,
                    "safety_module_config": {},
                    "status_report": True,
                    "assignment_id": fill_id,  # Store the fill ID for reference
                }
            else:
                raise ValueError(f"Unsupported executor type: {executor_type}")
            
            # Create the executor
            result = await self.create_executor_call(executor_type, executor_config)
            
            # Link to created executor's ID
            executor_id = result.get("id") if isinstance(result, dict) else None
            
            if executor_id:
                # Update assignment status and tracking info
                self.assignments[fill_id]["executor_id"] = executor_id
                self.assignments[fill_id]["status"] = "EXECUTING"
                self.logger().info(f"Executor {executor_id} created for assignment {fill_id}")
                
                # Also record in assigned_executors for future reference
                self.assigned_executors[executor_id] = {
                    "fill_id": fill_id,
                    "timestamp": time.time(),
                    "status": "ACTIVE",
                    "config": executor_config
                }
            else:
                self.logger().error(f"Failed to create executor for assignment {fill_id}: No executor ID returned")
        
        except Exception as e:
            self.logger().error(f"Error creating executor for assignment {fill_id}: {e}", exc_info=True)
            # Update assignment to show failure
            self.assignments[fill_id]["status"] = "FAILED"
            self.assignments[fill_id]["error"] = str(e)
            
        return executor_id 

    async def _handle_assignment(self, fill_id: str) -> bool:
        """
        Handle a pending position assignment by creating an executor for it
        
        :param fill_id: ID of the position fill event
        :return: True if handled, False otherwise
        """
        if fill_id not in self.assignments:
            return False
            
        assignment = self.assignments[fill_id]
        
        # If this assignment already has an executor, check if it's still active
        if assignment.get("executor_id"):
            # Assignment already has an executor assigned, check if it's in our active list
            executor_id = assignment["executor_id"]
            
            # Find if this executor is still active in our pool
            executor_found = False
            executor_inactive = False
            
            for executor_info in self.executors_info:
                if executor_info.id == executor_id:
                    executor_found = True
                    # Check if it's still active
                    if not executor_info.is_active:
                        executor_inactive = True
                    break
            
            if executor_found:
                if executor_inactive:
                    # Executor exists but is no longer active, wait for on_executor_completed to be called
                    # or for the periodic status check to handle it
                    self.logger().debug(f"Assignment {fill_id} has inactive executor {executor_id}, waiting for completion notification")
                    return True
                else:
                    # Executor is still active, nothing to do
                    return True
            else:
                # Executor not found in list, but give it some time (30 seconds) before resetting
                # This allows for any potential race conditions during startup
                executor_missing_time = time.time() - assignment.get("timestamp", time.time())
                if executor_missing_time < 30:
                    self.logger().debug(f"Assignment {fill_id} has executor {executor_id} not yet found in active list (missing for {executor_missing_time:.1f}s)")
                    return True
                    
                # If we've waited long enough, reset the executor ID to allow retry
                self.logger().info(f"Resetting assignment {fill_id} as its executor {executor_id} was not found after {executor_missing_time:.1f}s")
                assignment["executor_id"] = None
        
        # Don't retry failed assignments to avoid loops
        if assignment.get("status") == "FAILED":
            self.logger().debug(f"Skipping failed assignment {fill_id}")
            return False
            
        # Don't re-handle closed assignments
        if assignment.get("status") == "CLOSED":
            self.logger().debug(f"Skipping closed assignment {fill_id}")
            return False
        
        # If we reach here, we need to create a new executor
        try:
            # Set status to PENDING before we create the executor
            assignment["status"] = "PENDING"
            
            # Create the appropriate executor based on assignment details
            executor_id = await self.create_executor(fill_id, assignment, "ClosePositionExecutor")
            
            if not executor_id:
                # If we couldn't create an executor, mark as failed
                self.logger().error(f"Failed to create executor for assignment {fill_id}")
                assignment["status"] = "FAILED"
                assignment["error"] = "Could not create executor"
                return False
                
            # If we got here, we have successfully created an executor
            return True
            
        except Exception as e:
            # If there was an error creating the executor, mark as failed
            self.logger().error(f"Error handling assignment {fill_id}: {e}", exc_info=True)
            assignment["status"] = "FAILED"
            assignment["error"] = str(e)
            return False 