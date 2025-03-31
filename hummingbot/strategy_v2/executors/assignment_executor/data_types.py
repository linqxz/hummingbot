from __future__ import annotations

import inspect
import traceback
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


# This marker class helps distinguish AssignmentExecutorConfig from other executor configs
class AssignmentMarker:
    """
    This is a marker class to make AssignmentExecutorConfig distinct from other executor configs.
    It helps with isinstance() checks in the executor orchestrator without requiring changes to
    other executor types or the orchestrator itself.
    """
    pass


class TrailingStop(BaseModel):
    activation_price: Decimal
    trailing_delta: Decimal


class TripleBarrierConfig(BaseModel):
    stop_loss: Optional[Decimal]
    take_profit: Optional[Decimal]
    time_limit: Optional[int]
    trailing_stop: Optional[TrailingStop]
    open_order_type: OrderType = OrderType.LIMIT
    take_profit_order_type: OrderType = OrderType.MARKET
    stop_loss_order_type: OrderType = OrderType.MARKET
    time_limit_order_type: OrderType = OrderType.MARKET

    def new_instance_with_adjusted_volatility(self, volatility_factor: float) -> TripleBarrierConfig:
        new_trailing_stop = None
        if self.trailing_stop is not None:
            new_trailing_stop = TrailingStop(
                activation_price=self.trailing_stop.activation_price * Decimal(volatility_factor),
                trailing_delta=self.trailing_stop.trailing_delta * Decimal(volatility_factor)
            )

        return TripleBarrierConfig(
            stop_loss=self.stop_loss * Decimal(volatility_factor) if self.stop_loss is not None else None,
            take_profit=self.take_profit * Decimal(volatility_factor) if self.take_profit is not None else None,
            time_limit=self.time_limit,
            trailing_stop=new_trailing_stop,
            open_order_type=self.open_order_type,
            take_profit_order_type=self.take_profit_order_type,
            stop_loss_order_type=self.stop_loss_order_type,
            time_limit_order_type=self.time_limit_order_type
        )


class AssignmentExecutorConfig(AssignmentMarker, ExecutorConfigBase):
    """
    Configuration for the AssignmentExecutor.
    
    This executor is responsible for handling positions assigned through exchange programs
    like Kraken's Market Maker program. It executes orders to close positions that have
    been assigned by the exchange.
    """
    # Important: This must be unique and match what's expected in executor_orchestrator.py
    type = "assignment_executor"  # Class variable like other executors
    
    # Core parameters
    connector_name: str
    trading_pair: str
    side: TradeType  # BUY to close SHORT, SELL to close LONG
    amount: Decimal
    entry_price: Optional[Decimal]  # Reference price from assignment
    order_type: OrderType = OrderType.MARKET
    position_action: PositionAction = PositionAction.OPEN
    
    # Execution parameters
    triple_barrier_config: TripleBarrierConfig = TripleBarrierConfig()
    leverage: int = 50
    slippage_buffer: Decimal = Decimal("0.001")  # 0.1% slippage buffer for limit orders
    max_order_age: int = 60  # seconds before resubmitting orders
    max_retries: int = 3  # maximum number of retries for failed orders
    activation_bounds: Optional[List[Decimal]] = None
    level_id: Optional[str] = None

    # Reference fields
    assignment_id: Optional[str] = None  # Reference to the original assignment

    def __init__(self, **data):
        # Check if this is being incorrectly used for position_executor
        if data.get('type') == 'position_executor':
            print("\n========= INCORRECT USAGE DETECTED =========")
            print("AssignmentExecutorConfig created with type='position_executor'")
            print("This should be using PositionExecutorConfig instead!")
            print("\nCreation stack trace:")
            traceback.print_stack()
            
            # Try to find the culprit - who's calling this constructor?
            frame = inspect.currentframe()
            calling_frames = inspect.getouterframes(frame)
            if len(calling_frames) > 1:
                caller = calling_frames[1]
                print(f"\nDirect caller: {caller.filename}:{caller.lineno} - {caller.function}")
                
                # Show some code context
                if caller.code_context:
                    print("Code context:")
                    for i, line in enumerate(caller.code_context):
                        print(f"  {caller.lineno + i - 1}: {line.rstrip()}")
            
            print("==========================================\n")
        
        # Continue with the original __init__
        super().__init__(**data)

    # class Config:
    #     frozen = True  # Make the config immutable
    #     arbitrary_types_allowed = True
