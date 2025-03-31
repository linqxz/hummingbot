from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop, TripleBarrierConfig


class AssignmentAdapterExecutorConfig(ExecutorConfigBase):
    """
    Configuration for the AssignmentAdapterExecutor.
    
    This executor adapts the PositionExecutor to handle assignments. It treats
    an assignment as a pre-existing position that needs to be managed and closed.
    """
    # Important: This must be unique and match what's expected in executor_orchestrator.py
    type = "assignment_adapter_executor"
    
    # Core parameters - same as PositionExecutor
    trading_pair: str
    connector_name: str
    side: TradeType  # BUY to close SHORT, SELL to close LONG
    entry_price: Decimal  # Required for assignments (not optional)
    amount: Decimal
    triple_barrier_config: TripleBarrierConfig = TripleBarrierConfig()
    leverage: int = 1
    activation_bounds: Optional[List[Decimal]] = None
    level_id: Optional[str] = None
    
    # Assignment-specific parameters
    assignment_id: Optional[str] = None  # Reference to the original assignment
    position_action: PositionAction = PositionAction.CLOSE  # For assignments, we're always closing 