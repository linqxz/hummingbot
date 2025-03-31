import asyncio
import inspect
import logging
import time
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.kraken_perpetual import kraken_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.core.event.event_listener import EventListener
from hummingbot.core.event.events import AssignmentFillEvent, MarketEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class StringEventListener(EventListener):
    """A special event listener for string event tags"""
    def __init__(self, callback: Callable):
        super().__init__()
        self._callback = callback
        
    def __call__(self, event):
        try:
            self._callback(event)
        except Exception as e:
            logging.getLogger(__name__).error(f"Error in StringEventListener: {e}", exc_info=True)


class AssignmentManager(ScriptStrategyBase):
    """
    A script that automatically manages positions received through Kraken's assignment program.
    When an assignment is received, it immediately places an order to close the position.
    """
    
    # Define markets class attribute required by Hummingbot - will be filled dynamically
    markets = {"kraken_perpetual": []}

    def __init__(
        self,
        connectors: Dict[str, ConnectorBase],
        connector_name: str = "kraken_perpetual",
        trading_pairs: List[str] = None,  # Changed default to None
        all_trading_pairs: bool = True,   # New parameter
        order_type: str = "MARKET",
        close_percent: Decimal = Decimal("100"),  # 100% = close entire position
        slippage_buffer: Decimal = Decimal("0.001"),  # 0.1% slippage buffer for limit orders
        max_order_age: int = 60,  # in seconds, resubmit orders after this time
    ):
        # Initialize key instance variables before calling super().__init__()
        self._connector_name = connector_name
        self._all_trading_pairs = all_trading_pairs
        self._trading_pairs = trading_pairs if trading_pairs is not None else []
        self._order_type_str = order_type.upper()
        self._order_type = OrderType.MARKET if self._order_type_str == "MARKET" else OrderType.LIMIT
        self._close_percent = close_percent
        self._slippage_buffer = slippage_buffer
        self._max_order_age = max_order_age

        # Dictionary to track assignment fills
        self._assignments: Dict[str, Dict] = {}
        # Dictionary to track orders placed to close assignments
        self._closing_orders: Dict[str, Dict] = {}

        # Update the class attribute with the provided connector
        if connectors and self._connector_name in connectors:
            trading_pairs_list = connectors[self._connector_name].trading_pairs
            AssignmentManager.markets = {self._connector_name: trading_pairs_list}
        
        # Initialize the strategy
        super().__init__(connectors)

        # Initialize and validate the connectors
        self._initialize_markets()

    def _initialize_markets(self):
        """Initialize markets and validate trading pairs"""
        try:
            self._market = self.connectors[self._connector_name]
            
            # Log connector information
            self.logger().info(f"Initialized connector: {self._connector_name}")
            self.logger().info(f"Connector type: {type(self._market).__name__}")
            
            # Check if trading pairs are available
            if not self._market.trading_pairs:
                self.logger().warning(
                    f"No trading pairs available in connector {self._connector_name}. "
                    f"This is normal during initial setup or in testnet environments."
                )
                self.logger().info(
                    f"Assignment Manager will operate in dynamic trading pair mode, converting formats as needed."
                )
                
                # Set an empty list for trading pairs if using all pairs
                if self._all_trading_pairs:
                    self._trading_pairs = []
                    AssignmentManager.markets[self._connector_name] = []
                    self.logger().info("Watching for assignments on all trading pairs")
            else:
                    # If all_trading_pairs is True, get all available trading pairs
                if self._all_trading_pairs:
                    self._trading_pairs = self._market.trading_pairs
                    # Update the markets class attribute with all trading pairs
                    AssignmentManager.markets[self._connector_name] = self._trading_pairs
                    self.logger().info(f"Watching ALL trading pairs on {self._connector_name} ({len(self._trading_pairs)} pairs)")
                else:
                    # Validate specified trading pairs
                    for trading_pair in self._trading_pairs:
                        if trading_pair not in self._market.trading_pairs:
                            self.logger().error(f"Trading pair {trading_pair} not available on {self._connector_name}")
                            raise ValueError(f"Trading pair {trading_pair} not available")
                    # Update the markets class attribute with specified trading pairs
                    AssignmentManager.markets[self._connector_name] = self._trading_pairs
                    self.logger().info(f"Watching specific trading pairs: {', '.join(self._trading_pairs)}")
            
            # Create direct method patches that don't rely on the event system
            self._setup_direct_monitor()
            
            self.logger().info(f"AssignmentManager initialized. Watching for assignments on {self._connector_name}")
            self.logger().info(f"Order type: {self._order_type_str}")
            self.logger().info(f"Close percent: {self._close_percent}%")
        except Exception as e:
            self.logger().error(f"Error initializing markets: {e}", exc_info=True)
            raise
            
    def _setup_direct_monitor(self):
        """Set up direct monitoring for connector events"""
        if hasattr(self._market, "trigger_event"):
            # Store the original method for later restoration
            if not hasattr(self._market, "_original_trigger_event"):
                self._market._original_trigger_event = self._market.trigger_event

            # Create a patched version that will call our handler
            def patched_trigger_event(event_tag, event):
                """
                A safe wrapper around the original trigger_event method that 
                handles both string and enum event types.
                """
                try:
                    # Log what event is being processed
                    self.logger().debug(f"Patched trigger_event called with event_tag: {event_tag}, event: {event}")
                    
                    # If it's a string event tag, handle directly
                    if isinstance(event_tag, str):
                        if event_tag == "assignment_fill":
                            # Process assignment fill events directly
                            self._on_assignment_fill(event)
                        else:
                            self.logger().debug(f"String event tag: {event_tag}, skipping original trigger_event")
                    else:
                        # For enum event tags, call the original method
                        self._market._original_trigger_event(event_tag, event)
                    
                except Exception as e:
                    self.logger().error(f"Error in patched trigger_event: {e}", exc_info=True)
            
            # Replace the original method with our patched version
            self._market.trigger_event = patched_trigger_event

        # Also patch the _process_assignment_fill method if it exists
        if hasattr(self._market, "_process_assignment_fill"):
            if not hasattr(self._market, "_original_process_assignment"):
                self._market._original_process_assignment = self._market._process_assignment_fill
            
            # Create a patched version
            def patched_process_assignment(fill):
                """
                Process an assignment fill directly from connector. 
                This method modifies the behavior of the original _process_assignment_fill 
                method to use our custom event handling.
                """
                try:
                    # Extract assignment data from the fill object
                    self.logger().info(f"Processing assignment fill directly from connector: {fill}")
                    
                    # Extract timestamp (try different field names)
                    timestamp = fill.get("timestamp", "")
                    if not timestamp:
                        timestamp = fill.get("fill_time", "") or fill.get("time", "") or int(time.time() * 1000)
                    
                    # Extract fill ID
                    fill_id = fill.get("fill_id", "")
                    if not fill_id:
                        fill_id = fill.get("fillId", "") or fill.get("id", "") or f"fill-{int(time.time())}"
                    
                    # Extract order ID
                    order_id = fill.get("order_id", "")
                    if not order_id:
                        order_id = fill.get("orderId", "") or fill.get("orderID", "") or f"order-{fill_id}"
                    
                    # Extract trading pair - IMPORTANT: Kraken uses 'instrument' for the trading pair
                    # The format is typically 'PF_SOLUSD' which needs conversion to 'SOL-USD'
                    exchange_trading_pair = fill.get("instrument", "") or fill.get("symbol", "") or fill.get("trading_pair", "")
                    
                    # Convert from Kraken format to Hummingbot format
                    trading_pair = ""
                    if exchange_trading_pair:
                        try:
                            # Try multiple conversion methods from the connector or utilities
                            # Option 1: Use the connector's method directly
                            if hasattr(self._market, "convert_from_exchange_trading_pair"):
                                trading_pair = self._market.convert_from_exchange_trading_pair(exchange_trading_pair)
                                self.logger().info(f"Converted using connector method: {exchange_trading_pair} → {trading_pair}")
                            
                            # Option 2: Import and use the utils module from the connector
                            elif self._connector_name.startswith("kraken_perpetual"):
                                # Try to import specific utility module for Kraken
                                try:
                                    from hummingbot.connector.derivative.kraken_perpetual import kraken_perpetual_utils
                                    trading_pair = kraken_perpetual_utils.convert_from_exchange_trading_pair(exchange_trading_pair)
                                    self.logger().info(f"Converted using kraken_perpetual_utils: {exchange_trading_pair} → {trading_pair}")
                                except (ImportError, AttributeError) as e:
                                    self.logger().warning(f"Could not import kraken_perpetual_utils: {e}")
                                    # Fallback to simple conversion
                                    if exchange_trading_pair.startswith("PF_"):
                                        trading_pair = exchange_trading_pair[3:].replace("_", "-")
                                    else:
                                        trading_pair = exchange_trading_pair.replace("_", "-")
                                    self.logger().info(f"Fallback conversion: {exchange_trading_pair} → {trading_pair}")
                            
                            # Option 3: Use a more general utility
                            else:
                                # Try to import the web utils where trading pair functions often exist
                                connector_name = self._connector_name
                                module_path = f"hummingbot.connector.derivative.{connector_name}.{connector_name}_web_utils"
                                
                                try:
                                    module = __import__(module_path, fromlist=["convert_from_exchange_trading_pair"])
                                    if hasattr(module, "convert_from_exchange_trading_pair"):
                                        trading_pair = module.convert_from_exchange_trading_pair(exchange_trading_pair)
                                        self.logger().info(f"Converted using web_utils: {exchange_trading_pair} → {trading_pair}")
                                    else:
                                        raise AttributeError("Module does not have convert_from_exchange_trading_pair")
                                except (ImportError, AttributeError) as e:
                                    self.logger().warning(f"Could not import {module_path}: {e}")
                                    # Fallback to simple conversion
                                    if exchange_trading_pair.startswith("PF_"):
                                        trading_pair = exchange_trading_pair[3:].replace("_", "-")
                                    else:
                                        trading_pair = exchange_trading_pair.replace("_", "-")
                                    self.logger().info(f"Fallback conversion: {exchange_trading_pair} → {trading_pair}")
                                
                        except Exception as e:
                            self.logger().error(f"Error converting trading pair {exchange_trading_pair}: {e}")
                            # For Kraken, we can reasonably guess SOL-USD from PF_SOLUSD
                            if "SOL" in exchange_trading_pair and "USD" in exchange_trading_pair:
                                trading_pair = "SOL-USD"
                            else:
                                trading_pair = "BTC-USD"  # Default fallback
                            self.logger().warning(f"Using fallback trading pair: {trading_pair}")
                    else:
                        # If no trading pair found in the fill data
                        self.logger().warning(f"No trading pair found in fill data: {fill}")
                        
                        # Try to derive it from context - check if a market exists for SOL-USD
                        if "SOL-USD" in self._market.trading_pairs:
                            trading_pair = "SOL-USD"
                            self.logger().info(f"Using SOL-USD as default trading pair based on available markets")
                        else:
                            # Use the first available trading pair as a last resort
                            available_pairs = self._market.trading_pairs
                            if available_pairs:
                                trading_pair = available_pairs[0]
                                self.logger().info(f"Using first available trading pair: {trading_pair}")
                            else:
                                trading_pair = "BTC-USD"  # Ultimate fallback
                                self.logger().warning(f"No trading pairs available, using default: {trading_pair}")
                    
                    # Ensure the trading pair is tracked in the connector
                    self._ensure_trading_pair_tracked(trading_pair)
                    
                    # IMPORTANT: Interpreting position side from Kraken's data
                    # Kraken uses 'buy': True/False to indicate position side
                    # 'buy': True = LONG, 'buy': False = SHORT
                    is_buy = fill.get("buy", None)
                    self.logger().info(f"Raw position data from Kraken - buy: {is_buy}")
                    
                    # Derive position side from buy field (True = LONG, False = SHORT)
                    if is_buy is not None:
                        position_side_raw = "LONG" if is_buy else "SHORT"
                    else:
                        # Fall back to other fields if 'buy' is not present
                        position_side_raw = fill.get("positionSide", "") or fill.get("position_side", "") or "LONG"
                    
                    self.logger().info(f"Derived position side: {position_side_raw}")
                    position_side = position_side_raw
                    
                    # Extract amount and price
                    amount = float(fill.get("amount", "0")) or float(fill.get("qty", "0")) or float(fill.get("quantity", "0"))
                    price = float(fill.get("price", "0"))
                    
                    # Log all the extracted data for debugging
                    self.logger().info(f"Extracted assignment data: "
                                      f"exchange_trading_pair={exchange_trading_pair}, "
                                      f"converted_trading_pair={trading_pair}, "
                                      f"position_side={position_side}, "
                                      f"amount={amount}, "
                                      f"price={price}, "
                                      f"timestamp={timestamp}, "
                                      f"fill_id={fill_id}, "
                                      f"order_id={order_id}")
                    
                    # Create the event with the correct parameters
                    event = AssignmentFillEvent(
                        timestamp=timestamp,
                        trading_pair=trading_pair,
                        position_side=position_side,
                        amount=amount,
                        price=price,
                        fill_id=fill_id,
                        order_id=order_id
                    )
                    
                    # Process the event with our custom handler
                    self._on_assignment_fill(event)
                    
                    # Trigger assignment_fill event for other listeners using safe method
                    try:
                        self.logger().info(f"Triggering assignment_fill event")
                        self._safe_trigger_event("assignment_fill", event)
                    except Exception as e:
                        self.logger().error(f"Error triggering assignment_fill event: {e}", exc_info=True)
                    
                    # Call the original method with the original parameter to ensure safe handling
                    try:
                        self.logger().info(f"Calling original _process_assignment_fill method")
                        self._market._original_process_assignment(fill)
                    except Exception as e:
                        self.logger().error(f"Error in original _process_assignment_fill method: {e}", exc_info=True)
                    
                except Exception as e:
                    self.logger().error(f"Error processing assignment fill: {e}", exc_info=True)
            
            # Replace the original method with our patched version
            self._market._process_assignment_fill = patched_process_assignment

    def _safe_trigger_event(self, event_tag, event):
        """
        A safer way to trigger events that handles both string and enum event types.
        This avoids the 'str object has no attribute value' error.
        """
        try:
            # Log what event is being processed
            self.logger().debug(f"Safe trigger event called with: {event_tag}, event: {event}")
            
            # Handle all event types - both enum and string
            # For assignment fills
            if isinstance(event_tag, str) and event_tag == "assignment_fill":
                self.logger().info(f"Directly handling assignment_fill event")
                self._on_assignment_fill(event)
            # For order updates - these will be enum types from MarketEvent
            elif not isinstance(event_tag, str) and event_tag in [MarketEvent.BuyOrderCompleted, 
                                                                 MarketEvent.SellOrderCompleted,
                                                                 MarketEvent.OrderCancelled, 
                                                                 MarketEvent.OrderExpired,
                                                                 MarketEvent.OrderFilled, 
                                                                 MarketEvent.OrderFailure]:
                self.logger().info(f"Processing order update event: {event_tag}")
                self._handle_order_update(event_tag, event)
            
            # Try to call the original trigger_event for all events except string events
            if not isinstance(event_tag, str):
                try:
                    # Call the original method with the enum
                    self._market._original_trigger_event(event_tag, event)
                except Exception as e:
                    self.logger().error(f"Error in original trigger_event with enum: {e}")
            else:
                # For string events, try to map to enum if possible
                try:
                    # Look for a matching enum in MarketEvent
                    enum_event = None
                    for market_event in MarketEvent:
                        if market_event.name.lower() == event_tag.lower():
                            enum_event = market_event
                            break
                    
                    if enum_event is not None:
                        # Found a matching enum, call original with the enum
                        self.logger().debug(f"Mapped string event {event_tag} to enum {enum_event}")
                        self._market._original_trigger_event(enum_event, event)
                    else:
                        # No matching enum found, log this case
                        self.logger().debug(f"No matching enum found for string event: {event_tag}")
                except Exception as e:
                    self.logger().error(f"Error mapping string event to enum: {e}")
                    
        except Exception as e:
            self.logger().error(f"Error in _safe_trigger_event: {e}", exc_info=True)

    def _handle_order_update(self, event_tag, event):
        """Handle order status update events"""
        order_id = None
        
        # Extract order_id from the event
        if hasattr(event, "order_id"):
            order_id = event.order_id
        elif hasattr(event, "client_order_id"):
            order_id = event.client_order_id
        
        self.logger().info(f"Received order update event: {event_tag} for order {order_id}")
        
        # Check all fill IDs to see if this order is tracked
        tracked_fill_id = None
        for fill_id, order_data in self._closing_orders.items():
            if order_data.get("order_id") == order_id:
                tracked_fill_id = fill_id
                break
        
        # If not found by exact match, check for task IDs which are prefixed with "order_task_"
        if tracked_fill_id is None and order_id and isinstance(order_id, str) and "task" not in order_id:
            # Try to find by order ID substring match with our placeholder task IDs
            for fill_id, order_data in self._closing_orders.items():
                closing_order_id = order_data.get("order_id", "")
                if closing_order_id and "task" in closing_order_id and order_id != closing_order_id:
                    self.logger().info(f"Checking if {order_id} is related to task {closing_order_id}")
                    # This is a new order ID that might be from our async task
                    # Update the placeholder with the real order ID
                    self.logger().info(f"Updating order ID from {closing_order_id} to {order_id} for fill {fill_id}")
                    self._closing_orders[fill_id]["order_id"] = order_id
                    tracked_fill_id = fill_id
                    break
        
        if tracked_fill_id is None:
            self.logger().debug(f"Order {order_id} not tracked as a closing order. Ignoring update.")
            return  # Not one of our closing orders
        
        self.logger().info(f"Order {order_id} is for assignment {tracked_fill_id}")
        
        # Update the order status based on the event
        if event_tag in [MarketEvent.BuyOrderCompleted, MarketEvent.SellOrderCompleted, 
                        "order_filled", "order_completed", "buy_order_completed", "sell_order_completed"]:
            self._closing_orders[tracked_fill_id]["is_done"] = True
            self.logger().info(f"Order {order_id} for assignment {tracked_fill_id} has been filled successfully.")
            
            # If we have the assignment still tracked, mark it as complete
            if tracked_fill_id in self._assignments:
                self.logger().info(f"Marking assignment {tracked_fill_id} as complete.")
                del self._assignments[tracked_fill_id]
        
        elif event_tag in [MarketEvent.OrderCancelled, MarketEvent.OrderExpired, 
                         "order_cancelled", "order_expired", "order_failure"]:
            # Order wasn't filled, need to retry
            self.logger().warning(f"Order {order_id} for assignment {tracked_fill_id} was cancelled/expired. Will retry.")
            
            # If we still have the assignment tracked, mark it for retry
            if tracked_fill_id in self._assignments:
                self.logger().info(f"Marking assignment {tracked_fill_id} for retry.")
                self._assignments[tracked_fill_id]["metadata"]["ready_to_close"] = True
            
            # Remove from closing orders since this attempt failed
            del self._closing_orders[tracked_fill_id]
        
        # Log all tracked closing orders for debugging
        self.logger().info(f"Current closing orders: {self._closing_orders}")
        self.logger().info(f"Current assignments: {self._assignments}")
                
    def _on_assignment_fill(self, event: AssignmentFillEvent):
        """
        Handle an assignment fill event. This is called directly when an assignment
        is detected through the monitoring of user data streams.
        """
        # First ensure the trading pair is in our tracked list
        self._ensure_trading_pair_tracked(event.trading_pair)
        
        # Extract data from the event
        fill_id = event.fill_id
        trading_pair = event.trading_pair
        
        # Log detailed information about the assignment
        self.logger().info(f"==========================================")
        self.logger().info(f"ASSIGNMENT FILL RECEIVED: {event}")
        self.logger().info(f"Trading pair: {trading_pair}")
        self.logger().info(f"Position side: {event.position_side}")
        self.logger().info(f"Amount: {event.amount}")
        self.logger().info(f"Price: {event.price}")
        self.logger().info(f"==========================================")
        
        # Skip if this trading pair is not in our watch list and all_trading_pairs is False
        if not self._all_trading_pairs and trading_pair not in self._trading_pairs:
            self.logger().info(f"Received assignment for {trading_pair} but not in watched pairs. Ignoring.")
            return

        # Try to find the proper trading pair format that the exchange recognizes
        recognized_trading_pair = self._find_recognized_trading_pair(trading_pair)
        
        # If we still couldn't find a matching trading pair, log and return
        if not recognized_trading_pair:
            self.logger().warning(f"Available trading pairs: {self._market.trading_pairs}")
            self.logger().warning(f"Received assignment for {trading_pair} but it doesn't exist on the exchange. Ignoring.")
            return

        # Store the original trading pair for reference
        original_trading_pair = trading_pair
        
        # Store additional metadata with the assignment for processing
        assignment_metadata = {
            "recognized_trading_pair": recognized_trading_pair,
            "original_trading_pair": original_trading_pair,
            "ready_to_close": True,  # Mark as ready to close immediately
            "timestamp": self.current_timestamp,
            "fill_timestamp": event.timestamp,
            "attempts": 0
        }
        
        # Store the assignment with metadata
        self._assignments[fill_id] = {
            "event": event, 
            "metadata": assignment_metadata
        }
        
        self.logger().info(f"Assignment stored with ID {fill_id}. Will attempt to close position immediately.")
        
        # Process immediately instead of scheduling
        self._process_pending_assignments()

    def _find_recognized_trading_pair(self, trading_pair: str) -> str:
        """
        Find a recognized trading pair format for the given trading pair.
        Returns the recognized format or None if not found.
        """
        # Handle special case: if trading_pairs list is empty (common in testnet or initial setup)
        if not self._market.trading_pairs:
            self.logger().info(f"Trading pairs list is empty. Attempting direct format conversion.")
            
            # For Kraken Perpetual (testnet or mainnet), try our safe conversion helper
            if self._connector_name.startswith("kraken_perpetual"):
                try:
                    # First ensure the pair is in Hummingbot format
                    hb_format = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=True)
                    
                    # Then convert to exchange format
                    exchange_format = self._safe_convert_trading_pair_format(hb_format, to_hb_format=False)
                    
                    self.logger().info(f"Converted {trading_pair} to exchange format: {exchange_format}")
                    return exchange_format
                except Exception as e:
                    self.logger().error(f"Error in trading pair conversion: {e}")
        
        # First check direct methods for converting trading pairs if available
        if hasattr(self._market, "convert_to_exchange_trading_pair"):
            try:
                # Ensure the pair is in Hummingbot format first
                hb_format = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=True)
                
                # Then convert to exchange format
                exchange_pair = self._market.convert_to_exchange_trading_pair(hb_format)
                if not self._market.trading_pairs or exchange_pair in self._market.trading_pairs:
                    self.logger().info(f"Found trading pair in exchange format: {exchange_pair}")
                    return exchange_pair
            except Exception as e:
                self.logger().error(f"Error using direct conversion method: {e}")
        
        # Try checking module-level utils if available via the web_utils property
        if hasattr(self._market, "web_utils") and hasattr(self._market.web_utils, "convert_to_exchange_trading_pair"):
            try:
                # Ensure the pair is in Hummingbot format first
                hb_format = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=True)
                
                # Then convert using web_utils
                exchange_pair = self._market.web_utils.convert_to_exchange_trading_pair(hb_format)
                if not self._market.trading_pairs or exchange_pair in self._market.trading_pairs:
                    self.logger().info(f"Found trading pair using web_utils: {exchange_pair}")
                    return exchange_pair
            except Exception as e:
                self.logger().error(f"Error using web_utils conversion: {e}")

        # If we have a utils module directly
        try:
            # Handle both main connector and testnet variations
            base_connector_name = self._connector_name.replace("_testnet", "")
            module_name = f"hummingbot.connector.derivative.{base_connector_name}.{base_connector_name}_utils"
            
            self.logger().info(f"Attempting to import utils module: {module_name}")
            utils_module = __import__(module_name, fromlist=["convert_to_exchange_trading_pair"])
            
            if hasattr(utils_module, "convert_to_exchange_trading_pair"):
                # Ensure the pair is in Hummingbot format first
                hb_format = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=True)
                
                # Then convert using the utils module
                exchange_pair = utils_module.convert_to_exchange_trading_pair(hb_format)
                if not self._market.trading_pairs or exchange_pair in self._market.trading_pairs:
                    self.logger().info(f"Found trading pair using utils module: {exchange_pair}")
                    return exchange_pair
        except ImportError:
            self.logger().debug(f"Could not import utils module for {self._connector_name}")
        except Exception as e:
            self.logger().error(f"Error using imported utils module: {e}")

        # If nothing else worked, check if the original format is in the list
        if trading_pair in self._market.trading_pairs:
            self.logger().info(f"Trading pair already in correct format: {trading_pair}")
            return trading_pair
            
        # If all else failed, use our safe conversion helper as a last resort
        try:
            # Try converting to exchange format
            exchange_format = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=False)
            if exchange_format != trading_pair:  # If we got a different result
                self.logger().info(f"Using helper method to convert {trading_pair} to {exchange_format}")
                return exchange_format
        except Exception as e:
            self.logger().error(f"Error in final conversion attempt: {e}")

        # No match found
        return None

    def _safe_convert_trading_pair_format(self, trading_pair: str, to_hb_format: bool = True) -> str:
        """
        Safely convert a trading pair between Hummingbot format and exchange format.
        
        Args:
            trading_pair: The trading pair to convert
            to_hb_format: If True, convert from exchange to Hummingbot format.
                         If False, convert from Hummingbot to exchange format.
        
        Returns:
            The converted trading pair, or the original if conversion fails
        """
        try:
            # Handle null or empty input
            if not trading_pair:
                self.logger().warning("Empty trading pair provided for conversion")
                return trading_pair
                
            # Convert from exchange to Hummingbot format
            if to_hb_format:
                # If it's already in Hummingbot format (contains a hyphen), return as is
                if "-" in trading_pair:
                    return trading_pair
                    
                # Handle Kraken Perpetual format (PF_XBTUSD)
                if trading_pair.startswith("PF_") and len(trading_pair) > 3:
                    # Attempt to identify the boundary between base and quote
                    # First check if we can identify a standard quote currency
                    for quote_len in [3, 4]:  # Try common quote currency lengths
                        if len(trading_pair) >= (3 + quote_len):  # 3 is for "PF_"
                            potential_quote = trading_pair[-quote_len:]
                            potential_base = trading_pair[3:-quote_len]
                            
                            # Special handling for XBT -> BTC
                            if potential_base.upper() == "XBT":
                                potential_base = "BTC"
                                
                            result = f"{potential_base}-{potential_quote}"
                            self.logger().info(f"Converted exchange format {trading_pair} to Hummingbot format: {result}")
                            return result
                
                # If we can't parse it using the above rules, log a warning and return original
                self.logger().warning(f"Could not convert exchange format {trading_pair} to Hummingbot format")
                return trading_pair
            
            # Convert from Hummingbot to exchange format
            else:
                # If it's already in an exchange format (doesn't contain a hyphen), return as is
                if "-" not in trading_pair:
                    return trading_pair
                    
                # Try using the connector's built-in method if available
                if hasattr(self._market, "convert_to_exchange_trading_pair"):
                    result = self._market.convert_to_exchange_trading_pair(trading_pair)
                    if result:
                        self.logger().info(f"Converted Hummingbot format {trading_pair} to exchange format: {result}")
                        return result
                
                # Handle Kraken Perpetual format manually as fallback
                if self._connector_name.startswith("kraken_perpetual"):
                    try:
                        base, quote = trading_pair.split("-")
                        
                        # Convert BTC to XBT for Kraken
                        if base.upper() == "BTC":
                            base = "XBT"
                            
                        result = f"PF_{base.upper()}{quote.upper()}"
                        self.logger().info(f"Manually converted Hummingbot format {trading_pair} to exchange format: {result}")
                        return result
                    except Exception as e:
                        self.logger().error(f"Error in manual Kraken pair conversion: {e}")
                
                # If we can't convert it, log a warning and return original
                self.logger().warning(f"Could not convert Hummingbot format {trading_pair} to exchange format")
                return trading_pair
                
        except Exception as e:
            self.logger().error(f"Error converting trading pair format: {e}", exc_info=True)
            return trading_pair  # Return the original trading pair as fallback

    def on_tick(self):

        try:
            # Create async tasks for async operations
            safe_ensure_future(self._check_and_resubmit_orders())
            
            # Directly call synchronous method
            self._process_pending_assignments()
        except Exception as e:
            self.logger().error(f"Error in on_tick: {e}", exc_info=True)

    def _process_pending_assignments(self):
        """
        Process any pending assignments that need to be closed.
        """
        current_time = self.current_timestamp
        
        if not self._assignments:
            self.logger().debug("No pending assignments to process.")
            return
            
        self.logger().info(f"Processing {len(self._assignments)} pending assignments")
        
        # Check each pending assignment
        for fill_id, assignment_data in list(self._assignments.items()):
            try:
                event = assignment_data["event"]
                metadata = assignment_data["metadata"]
                
                # Check if it's time to process this assignment (if it has a ready_to_close flag)
                if metadata.get("ready_to_close", False):
                    # Log that we're attempting to close this position
                    trading_pair = event.trading_pair
                    position_side = event.position_side
                    amount = event.amount
                    price = event.price
                    
                    recognized_trading_pair = metadata.get("recognized_trading_pair", trading_pair)
                    
                    self.logger().info(f"Attempting to close position for assignment {fill_id}")
                    self.logger().info(f"Trading pair: {trading_pair} (recognized as {recognized_trading_pair})")
                    self.logger().info(f"Position side: {position_side}, Amount: {amount}, Price: {price}")
                    
                    # Increment attempt counter
                    metadata["attempts"] = metadata.get("attempts", 0) + 1
                    
                    # Place the order to close the position
                    order_id = None
                    try:
                        # Convert position_side to string if it's an enum object
                        position_side_str = position_side
                        if hasattr(position_side, "name"):
                            position_side_str = position_side.name
                        elif hasattr(position_side, "value"):
                            position_side_str = position_side.value
                            
                        # For SHORT positions we BUY to close, for LONG positions we SELL to close
                        is_buy = position_side_str.upper() == "SHORT"
                        
                        # Log the closing order direction clearly
                        if is_buy:
                            self.logger().info(f"Placing BUY order to close SHORT position for {recognized_trading_pair}")
                        else:
                            self.logger().info(f"Placing SELL order to close LONG position for {recognized_trading_pair}")
                        
                        # Place the closing order using the synchronous wrapper
                        order_id = self._place_direct_market_order_sync(
                            recognized_trading_pair,
                            is_buy=is_buy,
                            amount=abs(float(amount))
                        )
                        
                        if order_id:
                            self.logger().info(f"Successfully scheduled order {order_id} to close position for assignment {fill_id}")
                            # Store the order ID with the assignment for tracking
                            metadata["order_id"] = order_id
                            metadata["order_placed_time"] = self.current_timestamp
                            metadata["ready_to_close"] = False  # Mark as no longer ready to close since we've initiated the order
                            
                            # Add to closing orders for tracking
                            self._closing_orders[fill_id] = {
                                "order_id": order_id,
                                "is_done": False,
                                "trading_pair": recognized_trading_pair,
                                "is_buy": is_buy,
                                "amount": abs(float(amount)),
                                "timestamp": self.current_timestamp
                            }
                        else:
                            self.logger().warning(f"Failed to place order to close position for assignment {fill_id} - no order ID returned")
                            # Will retry on next tick, no scheduling needed
                    except Exception as e:
                        self.logger().error(f"Error placing order to close position for assignment {fill_id}: {e}", exc_info=True)
                        # Will retry on next tick, no scheduling needed
                        
            except Exception as e:
                self.logger().error(f"Error processing assignment {fill_id}: {e}", exc_info=True)

    async def _check_and_resubmit_orders(self):
        """Check if any orders need to be resubmitted due to age or failure"""
        current_time = self.current_timestamp

        # Get orders that have been open too long
        expired_orders = [
            fill_id for fill_id, order_data in self._closing_orders.items()
            if not order_data["is_done"] and (current_time - order_data["timestamp"] > self._max_order_age)
        ]

        for fill_id in expired_orders:
            self.logger().info(f"Order for assignment {fill_id} has been open too long. Resubmitting.")
            # Cancel the existing order if possible
            if self._closing_orders[fill_id]["order_id"] in self._market.in_flight_orders:
                await self._market.cancel_order(self._closing_orders[fill_id]["order_id"])

            # Place a new order
            await self._place_close_order(self._assignments[fill_id]["event"])

    async def _place_close_order(self, assignment, order_type=OrderType.MARKET) -> bool:
        """
        Places a close order for an assignment.
        """
        if not assignment:
            self.logger().error("Cannot place close order - no assignment provided")
            return False
            
        if not hasattr(assignment, "fill_id") or not assignment.fill_id:
            self.logger().error("Assignment does not have a fill_id")
            return False
            
        fill_id = assignment.fill_id
        trading_pair = assignment.trading_pair
        
        # Check if this assignment has already been processed
        if fill_id in self._closing_orders and self._closing_orders[fill_id].get("is_done", False):
            self.logger().info(f"Assignment {fill_id} has already been processed")
            return False
            
        # Calculate the close amount
        close_amount = assignment.amount
        if not isinstance(close_amount, Decimal):
            try:
                close_amount = Decimal(str(close_amount))
            except Exception as e:
                self.logger().error(f"Error converting amount to Decimal: {e}")
                return False
        
        # Convert the trading pair if necessary (some strategies might use a different format)
        hb_trading_pair = trading_pair
        
        # Determine trade type - if we got a LONG position (BUY), we need to SELL to close it
        # If we got a SHORT position (SELL), we need to BUY to close it
        if hasattr(assignment, "position_side"):
            # Use our safe comparison helper that handles both string and enum types
            trade_type = TradeType.SELL if self._is_equivalent_position_side(assignment.position_side, PositionSide.LONG) else TradeType.BUY
        else:
            # Fallback to using 'side' if 'position_side' is not available
            # Use our safe comparison helper method
            trade_type = TradeType.SELL if self._is_equivalent_position_side(assignment.side, "BUY") else TradeType.BUY
            
        self.logger().info(f"Determined trade type: {trade_type} to close {'LONG' if trade_type == TradeType.SELL else 'SHORT'} position")
        
        # For limit orders, get the price
        price = None
        if order_type == OrderType.LIMIT:
            try:
                # Try to get current price from order book first
                price = self.get_price(trading_pair, is_buy=trade_type == TradeType.BUY)
                self.logger().info(f"Got current price for {trading_pair}: {price}")
            except Exception as e:
                # If that fails, use the assignment price as fallback
                self.logger().warning(f"Error getting current price: {e} - using assignment price")
                price = assignment.price
                
            if not price or price == 0:
                self.logger().warning(f"No valid price found for {trading_pair}, using assignment price")
                price = assignment.price
                
            # Ensure price is a Decimal
            if not isinstance(price, Decimal):
                try:
                    price = Decimal(str(price))
                except Exception as e:
                    self.logger().error(f"Error converting price to Decimal: {e}")
                    return False
        
        # Place the order
        try:
            # Convert the exchange trading pair to Hummingbot format if needed
            if not "-" in hb_trading_pair:
                self.logger().warning(f"Provided trading pair {hb_trading_pair} not in expected Hummingbot format. Attempting to convert.")
                
                # For Kraken PF_XBTUSD -> BTC-USD
                if self._connector_name.startswith("kraken_perpetual") and trading_pair.startswith("PF_XBT"):
                    hb_trading_pair = "BTC-USD"
                    self.logger().info(f"Using hardcoded conversion from {trading_pair} to {hb_trading_pair}")
            
            # Final check to ensure amount is a Decimal object
            final_close_amount = Decimal(str(close_amount))
            
            # Add debugging for the order closure attempt
            self.logger().info(f"Attempting to close position with {order_type} order: {trading_pair} | Amount: {final_close_amount} | Price: {'MARKET PRICE' if order_type == OrderType.MARKET else price}")
            
            # Enhanced logging before order placement
            self.logger().info("=" * 50)
            self.logger().info(f"ORDER PLACEMENT SUMMARY:")
            self.logger().info(f"Assignment ID: {fill_id}")
            self.logger().info(f"Order Type: {order_type}")
            self.logger().info(f"Trade Type: {trade_type}")
            self.logger().info(f"Trading Pair (Exchange format): {trading_pair}")
            self.logger().info(f"Trading Pair (Hummingbot format): {hb_trading_pair}")
            self.logger().info(f"Amount: {final_close_amount}")
            if order_type == OrderType.LIMIT:
                self.logger().info(f"Price: {price}")
            else:
                self.logger().info(f"Price: MARKET (no price parameter)")
            self.logger().info("=" * 50)
            
            # Check if order book exists before attempting to place order
            if order_type == OrderType.MARKET:
                self.logger().info(f"Placing MARKET order (no price needed) with amount: {final_close_amount}")
                
                # Check if the order book is tracked
                if not self._is_order_book_tracked(trading_pair):
                    self.logger().info(f"Order book for {trading_pair} is not available - using direct market order placement")
                    order_id = await self._place_direct_market_order(trading_pair, trade_type == TradeType.BUY, final_close_amount)
                    self.logger().info(f"Direct market order placed successfully with ID: {order_id}")
                else:
                    # Order book is available, use standard methods
                    self.logger().info(f"Order book available - using standard market order placement")
                    if trade_type == TradeType.BUY:
                        order_id = self.buy(
                connector_name=self._connector_name,
                            trading_pair=hb_trading_pair,  # Use Hummingbot format
                            amount=final_close_amount,
                            order_type=order_type,
                            position_action=PositionAction.CLOSE
                        )
                    else:  # SELL
                        order_id = self.sell(
                            connector_name=self._connector_name,
                            trading_pair=hb_trading_pair,  # Use Hummingbot format
                            amount=final_close_amount,
                            order_type=order_type,
                            position_action=PositionAction.CLOSE
                        )
            else:
                # Limit order - need price
                final_price = Decimal(str(price))
                self.logger().info(f"Placing LIMIT order with price: {final_price} and amount: {final_close_amount}")
                
                if trade_type == TradeType.BUY:
                    order_id = self.buy(
                        connector_name=self._connector_name,
                        trading_pair=hb_trading_pair,  # Use Hummingbot format
                        amount=final_close_amount,
                        order_type=order_type,
                        price=final_price,
                        position_action=PositionAction.CLOSE
                    )
                else:  # SELL
                    order_id = self.sell(
                        connector_name=self._connector_name,
                        trading_pair=hb_trading_pair,  # Use Hummingbot format
                        amount=final_close_amount,
                        order_type=order_type,
                        price=final_price,
                position_action=PositionAction.CLOSE
            )
            
            # Store the order details
            self._closing_orders[fill_id] = {
                "order_id": order_id,
                "timestamp": self.current_timestamp,
                "is_done": False
            }
            
            self.logger().info(f"Order placed successfully. Order ID: {order_id}")
            return True
        except Exception as e:
            self.logger().error(f"Error placing order to close assignment: {e}", exc_info=True)
            return False
            
    @property
    def assignment_stats(self):
        """Return statistics about assignment handling"""
        total = len(self._assignments)
        closed = sum(1 for fill_id, data in self._closing_orders.items() if data["is_done"])
        pending = total - closed
        
        return {
            "total_assignments": total,
            "closed_positions": closed,
            "pending_positions": pending,
            "success_rate": (closed / total) * 100 if total > 0 else 0
        }
        

    def did_fill_order(self, event):
        """
        Called when an order is filled. Update the status of closing orders.
        """
        for fill_id, order_data in self._closing_orders.items():
            if order_data["order_id"] == event.order_id:
                self.logger().info(f"Order {event.order_id} filled. Assignment {fill_id} closed.")
                order_data["is_done"] = True
                break

    def did_cancel_order(self, event):
        """
        Called when an order is cancelled.
        """
        for fill_id, order_data in self._closing_orders.items():
            if order_data["order_id"] == event.order_id:
                self.logger().info(f"Order {event.order_id} cancelled for assignment {fill_id}.")
                # We don't mark as done since we'll want to place a new order
                break

    def did_fail_order(self, event):
        """
        Called when an order fails.
        """
        for fill_id, order_data in self._closing_orders.items():
            if order_data["order_id"] == event.order_id:
                self.logger().error(f"Order {event.order_id} failed for assignment {fill_id}.")
                # We don't mark as done since we'll want to retry
                break

    def format_status(self) -> str:
        """Format the status of the strategy for display."""
        if not self._assignments:
            return "No assignments received yet."
        
        lines = []
        
        # Add statistics section
        stats = self.assignment_stats
        lines.append("\n  Statistics:")
        lines.append(f"    Total assignments: {stats['total_assignments']}")
        lines.append(f"    Closed positions: {stats['closed_positions']}")
        lines.append(f"    Pending positions: {stats['pending_positions']}")
        lines.append(f"    Success rate: {stats['success_rate']:.1f}%")
        
        # Existing assignments section
        lines.append("\n  Assignments:")
        for fill_id, assignment_data in self._assignments.items():
            order_status = "Not processed"
            if fill_id in self._closing_orders:
                order_data = self._closing_orders[fill_id]
                order_status = "Closed" if order_data["is_done"] else "Processing"
            
            # Handle both new and old format
            if isinstance(assignment_data, dict) and "event" in assignment_data:
                # New format
                event = assignment_data["event"]
                metadata = assignment_data["metadata"]
                recognized_pair = metadata.get("recognized_trading_pair", "unknown")
                lines.append(f"    {event.trading_pair} (as {recognized_pair}) | {event.position_side.name} | "
                            f"{event.amount} @ {event.price} | Status: {order_status}")
            else:
                # Old format - assignment_data is the AssignmentFillEvent itself
                event = assignment_data
                lines.append(f"    {event.trading_pair} | {event.position_side.name} | "
                            f"{event.amount} @ {event.price} | Status: {order_status}")
        
        return "\n".join(lines)

    def stop(self):
        """Clean up when the script is stopped"""
        # Restore original methods if possible
        if hasattr(self, "_market") and hasattr(self._market, "_original_trigger_event"):
            self._market.trigger_event = self._market._original_trigger_event
        if hasattr(self, "_market") and hasattr(self._market, "_original_process_assignment"):
            self._market._process_assignment_fill = self._market._original_process_assignment
            
        super().stop()

    def _is_order_book_tracked(self, trading_pair: str) -> bool:
        """
        Check if an order book is being tracked for the given trading pair.
        Returns True if the order book is being tracked, False otherwise.
        """
        try:
            # Check if the pair exists in the order books dict directly
            if hasattr(self._market, "order_books") and trading_pair in self._market.order_books:
                self.logger().info(f"Order book for {trading_pair} is already tracked (direct check)")
                return True
                
            # Check via the order book tracker if available
            if hasattr(self._market, "order_book_tracker"):
                if hasattr(self._market.order_book_tracker, "order_books") and trading_pair in self._market.order_book_tracker.order_books:
                    self.logger().info(f"Order book for {trading_pair} is already tracked (via tracker)")
                    return True
                    
                if hasattr(self._market.order_book_tracker, "_order_books") and trading_pair in self._market.order_book_tracker._order_books:
                    self.logger().info(f"Order book for {trading_pair} is already tracked (via tracker private dict)")
                    return True
                    
            # Try the get_order_book method with exception handling
            try:
                order_book = self._market.get_order_book(trading_pair)
                if order_book is not None:
                    self.logger().info(f"Order book for {trading_pair} exists and can be retrieved")
                    return True
            except:
                # This is expected to fail if not tracked
                pass
                
            return False
        except Exception as e:
            self.logger().error(f"Error checking if order book is tracked for {trading_pair}: {e}")
            return False

    async def _place_direct_market_order(self, trading_pair: str, is_buy: bool, amount: Decimal) -> str:
        """
        Place a market order directly through the exchange API, bypassing the order book.
        This is useful for exchanges where the order book is not yet available.
        """
        if not trading_pair:
            self.logger().error("Cannot place order with empty trading pair")
            return ""
            
        # Ensure the trading pair is tracked
        self._ensure_trading_pair_tracked(trading_pair)
            
        # Convert to standardized format first (ensure it has a hyphen)
        trading_pair = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=True)

        connector_name = self._market.name.lower()
        amount_str = str(amount)
        
        # Ensure amount is a Decimal
        if not isinstance(amount, Decimal):
            self.logger().warning(f"Converting amount {amount} to Decimal")
            amount = Decimal(str(amount))
        
        try:
            if "kraken" in connector_name:
                # Kraken specific handling
                self.logger().info(f"Using Kraken direct API for market order: {trading_pair}, {'buy' if is_buy else 'sell'}, {amount}")
                
                # Convert to exchange trading pair format if method exists
                exchange_trading_pair = trading_pair
                if hasattr(self._market, "exchange_symbol_associated_to_pair"):
                    try:
                        # Use our safe conversion helper method to ensure the trading pair is in Hummingbot format first
                        trading_pair_hb_format = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=True)
                        self.logger().info(f"Ensured trading pair is in Hummingbot format: {trading_pair_hb_format}")
                        
                        # Now convert to exchange format using the API
                        exchange_trading_pair = await self._market.exchange_symbol_associated_to_pair(trading_pair_hb_format)
                        self.logger().info(f"Converted to exchange symbol: {exchange_trading_pair}")
                    except Exception as e:
                        self.logger().warning(f"Error converting {trading_pair} to exchange symbol: {e}")
                        # If API conversion fails, try our manual helper method
                        exchange_trading_pair = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=False)
                        self.logger().info(f"Using manual conversion to exchange format: {exchange_trading_pair}")
                
                # Generate a client order ID
                client_order_id = f"hbot-{self.current_timestamp}-{trading_pair}"
                
                # Prepare parameters according to Kraken API docs
                # Based on kraken_perpetual_derivative.py implementation
                data = {
                    "orderType": "mkt",  # Market order
                    "symbol": exchange_trading_pair,
                    "side": "buy" if is_buy else "sell",
                    "size": float(amount),
                    "cliOrdId": client_order_id,
                    "reduceOnly": "true"  # Since we're closing a position
                }
                
                self.logger().info(f"Prepared Kraken order parameters: {data}")
                
                # Check which API methods are available
                if hasattr(self._market, "_api_post"):
                    # Try to get the endpoint path from CONSTANTS
                    try:
                        endpoint = CONSTANTS.PLACE_ACTIVE_ORDER_PATH_URL
                    except (NameError, AttributeError):
                        # Fallback path if CONSTANTS is not available
                        endpoint = "/derivatives/api/v3/sendorder"
                    
                    self.logger().info(f"Using _api_post API with endpoint: {endpoint}")
                    result = await self._market._api_post(
                        path_url=endpoint,
                        data=data,
                        is_auth_required=True,
                        headers={"referer": "HBOT"}
                    )
                    
                    self.logger().info(f"Kraken API response: {result}")
                    
                    # Extract order ID based on Kraken's response format
                    if isinstance(result, dict):
                        if result.get("result") == "success":
                            # Check for sendStatus structure
                            if "sendStatus" in result:
                                send_status = result["sendStatus"]
                                if "order_id" in send_status:
                                    return str(send_status["order_id"])
                                elif "orderEvents" in send_status and send_status["orderEvents"]:
                                    order_events = send_status["orderEvents"]
                                    if order_events and "order" in order_events[0]:
                                        return str(order_events[0]["order"]["orderId"])
                        
                        # Check for various ID fields
                        for key in ["orderId", "order_id", "id", "orderID"]:
                            if key in result:
                                return str(result[key])
                    
                    # Return the result as a string if extraction failed
                    self.logger().warning(f"Could not extract order ID from response, using entire response")
                    return f"response-{self.current_timestamp}-{str(result)[:20]}"
                        
                elif hasattr(self._market, "_api_request"):
                    endpoint = "/derivatives/api/v3/sendorder"
                    method = "POST"
                    self.logger().info(f"Sending direct API request via _api_request to {endpoint} with params: {data}")
                    result = await self._market._api_request(method, endpoint, params=data, is_auth_required=True)
                    
                    self.logger().info(f"Kraken API response: {result}")
                    
                    # Process result with same logic as above
                    if isinstance(result, dict):
                        if result.get("result") == "success":
                            # Check for sendStatus structure
                            if "sendStatus" in result:
                                send_status = result["sendStatus"]
                                if "order_id" in send_status:
                                    return str(send_status["order_id"])
                                elif "orderEvents" in send_status and send_status["orderEvents"]:
                                    order_events = send_status["orderEvents"]
                                    if order_events and "order" in order_events[0]:
                                        return str(order_events[0]["order"]["orderId"])
                        
                        # Check for various ID fields
                        for key in ["orderId", "order_id", "id", "orderID"]:
                            if key in result:
                                return str(result[key])
                    
                    # Return the result as a string if extraction failed
                    self.logger().warning(f"Could not extract order ID from response, using entire response")
                    return f"response-{self.current_timestamp}-{str(result)[:20]}"
            
            # Fallback to the standard method as last resort
            self.logger().info(f"Using standard execution method for market order: {trading_pair}")
            if is_buy:
                result = await self.buy(trading_pair, amount, OrderType.MARKET)
            else:
                result = await self.sell(trading_pair, amount, OrderType.MARKET)
                
            return str(result.order_id) if hasattr(result, "order_id") else str(result)
            
        except Exception as e:
            error_msg = f"Error placing market order: {e}"
            self.logger().error(error_msg, exc_info=True)
            # Return an error order ID to indicate failure
            return f"error-{self.current_timestamp}-{trading_pair}"

    # Add this helper method to safely compare position sides
    def _is_equivalent_position_side(self, side1, side2):
        """
        Safely compare position sides regardless of whether they're string or enum values.
        Returns True if the sides are equivalent, False otherwise.
        """
        # Convert to uppercase strings for comparison
        side1_str = str(side1).upper()
        side2_str = str(side2).upper()
        
        # Normalize variations
        if side1_str in ["BUY", "LONG", "POSITIONSIDE.LONG"]:
            side1_str = "LONG"
        elif side1_str in ["SELL", "SHORT", "POSITIONSIDE.SHORT"]:
            side1_str = "SHORT"
            
        if side2_str in ["BUY", "LONG", "POSITIONSIDE.LONG"]:
            side2_str = "LONG"
        elif side2_str in ["SELL", "SHORT", "POSITIONSIDE.SHORT"]:
            side2_str = "SHORT"
        
        return side1_str == side2_str

    def _ensure_trading_pair_tracked(self, trading_pair: str) -> bool:
        """
        Ensure that a trading pair is in our tracked list, adding it if needed.
        Returns True if the pair was added, False if it was already tracked.
        """
        try:
            # Convert to Hummingbot format for consistent tracking
            hb_trading_pair = self._safe_convert_trading_pair_format(trading_pair, to_hb_format=True)
            if not hb_trading_pair:
                self.logger().error(f"Failed to convert {trading_pair} to Hummingbot format")
                return False
                
            # Check if already tracked
            if hb_trading_pair in self._trading_pairs:
                return False
                
            # Add to tracked pairs
            self._trading_pairs.append(hb_trading_pair)
            # Update class attribute
            if hb_trading_pair not in AssignmentManager.markets[self._connector_name]:
                AssignmentManager.markets[self._connector_name].append(hb_trading_pair)
                
            self.logger().info(f"Added new trading pair to tracked list: {hb_trading_pair}")
            return True
        except Exception as e:
            self.logger().error(f"Error adding trading pair {trading_pair} to tracked list: {e}")
            return False

    # Add a synchronous wrapper method for _place_direct_market_order
    def _place_direct_market_order_sync(self, trading_pair: str, is_buy: bool, amount: Decimal) -> str:
        """
        Synchronous wrapper around the async _place_direct_market_order method.
        This allows calling the async method from synchronous code.
        """
        self.logger().info(f"Placing {'BUY' if is_buy else 'SELL'} order for {amount} {trading_pair}")
        
        # Create and schedule the async task to be run in the event loop
        task = safe_ensure_future(self._place_direct_market_order(
            trading_pair=trading_pair,
            is_buy=is_buy,
            amount=amount
        ))
        
        # Log that we've scheduled the order but don't wait for the result
        self.logger().info(f"Scheduled order placement task: {task}")
        return f"order_task_{int(time.time())}"  # Return a placeholder ID


def setup_kraken_assignment_manager(connectors = None):
    """
    Standard setup function for the Kraken Assignment Manager.
    Returns an instance of the AssignmentManager.
    """
    # All values here can be adjusted according to your preferences
    assignment_manager = AssignmentManager(
        connectors=connectors,                     # Connectors passed from Hummingbot
        connector_name="kraken_perpetual",         # Default to testnet for safer testing
        all_trading_pairs=True,                    # Listen to all trading pairs
        trading_pairs=None,                        # No specific pairs (will be ignored when all_trading_pairs=True)
        order_type="MARKET",                       # Order type to usse when closing positions (MARKET or LIMIT)
        close_percent=Decimal("100"),              # Percentage of position to close (100 = all)
        slippage_buffer=Decimal("0.001"),          # For limit orders: 0.1% slippage buffer
        max_order_age=60                           # Resubmit orders after 60 seconds if not filled
    )
    return assignment_manager