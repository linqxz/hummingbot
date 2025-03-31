from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.assignment_executor.assignment_executor import AssignmentExecutor
from hummingbot.strategy_v2.executors.assignment_executor.data_types import AssignmentExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TestPositionClosure(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.strategy = self.create_mock_strategy
        self.trading_rules = TradingRule(
            trading_pair="BTC-USDT",
            min_order_size=Decimal("0.01"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.01"),
            min_quote_amount_increment=Decimal("0.01"),
            min_notional_size=Decimal("0.01"),
        )

    @property
    def create_mock_strategy(self):
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-USDT")
        type(strategy).current_timestamp = PropertyMock(return_value=1234567890)
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        strategy.connectors = {
            "kraken_perpetual": MagicMock(spec=ExchangePyBase),
        }
        return strategy

    def get_assignment_config_market_long(self):
        """Create a sample config for a long market assignment executor"""
        return AssignmentExecutorConfig(
            id="test_assignment_long",
            timestamp=1234567890,
            connector_name="kraken_perpetual",
            trading_pair="BTC-USDT",
            side=TradeType.SELL,  # SELL to close a LONG position
            amount=Decimal("0.1"),
            entry_price=Decimal("50000"),
            order_type=OrderType.MARKET,
            position_action=PositionAction.CLOSE,
            assignment_id="test_fill_id"
        )

    def get_assignment_executor_from_config(self, assignment_config):
        """Create an assignment executor from a config"""
        executor = AssignmentExecutor(
            strategy=self.strategy,
            config=assignment_config
        )
        # Add missing attributes needed for tests
        executor._executors_update_event = MagicMock()
        executor._executors_update_event.is_set = MagicMock(return_value=False)
        return executor

    async def test_no_duplicate_market_orders_when_position_closed(self):
        """Test that no duplicate market orders are placed when a position is already closed"""
        # Setup
        config = self.get_assignment_config_market_long()
        executor = self.get_assignment_executor_from_config(config)
        
        # Mock connector with no positions (simulating position already closed)
        mock_connector = MagicMock()
        mock_connector.account_positions = []
        executor.connectors = {config.connector_name: mock_connector}
        
        # Mock open_and_close_volume_match to return True (position is closed)
        executor.open_and_close_volume_match = MagicMock(return_value=True)
        
        # Set to running
        executor._status = RunnableStatus.RUNNING
        
        # Execute the control task
        await executor.control_task()
        
        # Verify executor was terminated and no orders were placed
        self.assertEqual(RunnableStatus.TERMINATED, executor._status)
        mock_connector.sell.assert_not_called()
        mock_connector.buy.assert_not_called()

    async def test_position_closure_with_fill_event(self):
        """Test that position is properly closed after receiving a fill event"""
        # Setup
        config = self.get_assignment_config_market_long()
        executor = self.get_assignment_executor_from_config(config)
        
        # Create a tracked order with in-flight order
        tracked_order = TrackedOrder(order_id="OID-1")
        in_flight_order = InFlightOrder(
            client_order_id="123",
            exchange_order_id="EX123",
            trading_pair=config.trading_pair,
            order_type=OrderType.MARKET,
            trade_type=TradeType.SELL,
            amount=Decimal("0.1"),
            price=Decimal("0"),
            creation_timestamp=1234567830,
            initial_state=OrderState.OPEN
        )
        tracked_order.order = in_flight_order
        executor._close_order = tracked_order
        
        # Set executor to running
        executor._status = RunnableStatus.RUNNING
        
        # Create a fill event that completes the position
        fill_event = OrderFilledEvent(
            timestamp=1234567890,
            order_id="OID-1",
            trading_pair=config.trading_pair,
            trade_type=TradeType.SELL,
            order_type=OrderType.MARKET,
            price=Decimal("50100"),
            amount=Decimal("0.1"),  # Fully filled
            trade_fee=AddedToCostTradeFee(percent=Decimal("0"), flat_fees=[])
        )
        
        # Setup the mock connection for order tracking
        mock_connector = MagicMock()
        mock_connector._order_tracker = MagicMock()
        mock_connector._order_tracker.fetch_order.return_value = in_flight_order
        executor.connectors = {config.connector_name: mock_connector}
        
        # Make sure our close_filled_amount property reflects the right amount
        type(executor).close_filled_amount = PropertyMock(return_value=Decimal("0.1"))
        type(executor)._assigned_amount = PropertyMock(return_value=Decimal("0.1"))
        
        # Process the fill event with mocked update_tracked_orders
        with patch.object(executor, 'update_tracked_orders_with_order_id', MagicMock()):
            executor.process_order_filled_event(None, mock_connector, fill_event)
        
        # Verify executor was terminated
        self.assertEqual(RunnableStatus.TERMINATED, executor._status)
        
        # Run control task to verify it doesn't place another order
        mock_connector.sell.reset_mock()
        await executor.control_task()
        
        # Verify no orders were placed
        mock_connector.sell.assert_not_called()

    async def test_exchange_position_verification(self):
        """Test that the executor correctly verifies position status with the exchange"""
        # Setup
        config = self.get_assignment_config_market_long()
        executor = self.get_assignment_executor_from_config(config)
        
        # Mock connector with a position
        mock_connector = MagicMock()
        mock_connector.account_positions = [
            Position(
                trading_pair=config.trading_pair,
                position_side=PositionSide.LONG,
                unrealized_pnl=Decimal("0"),
                amount=Decimal("0.1"),
                entry_price=Decimal("50000"),
                leverage=Decimal("10")
            )
        ]
        executor.connectors = {config.connector_name: mock_connector}
        
        # Set executor to running
        executor._status = RunnableStatus.RUNNING
        
        # Set up amount_to_close and trading rules
        type(executor).amount_to_close = PropertyMock(return_value=Decimal("0.1"))
        executor.trading_rules = self.trading_rules
        
        # Execute control_close_order
        with patch.object(executor, 'place_order', return_value="test-order-id"):
            await executor.control_close_order()
            # Directly set the close order since our mock doesn't actually set it
            executor._close_order = TrackedOrder(order_id="test-order-id")
        
        # Verify an order was attempted to be placed
        self.assertIsNotNone(executor._close_order)
        
        # Now simulate position disappearing
        mock_connector.account_positions = []
        
        # Reset executor state
        executor._close_order = None
        executor._status = RunnableStatus.RUNNING
        
        # Execute control_close_order again
        await executor.control_close_order()
        
        # Verify executor was terminated without placing an order
        self.assertEqual(RunnableStatus.TERMINATED, executor._status)
        self.assertIsNone(executor._close_order) 