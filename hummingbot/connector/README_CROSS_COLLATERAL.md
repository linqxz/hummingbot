# Cross-Collateral Trading for Hummingbot

This document explains how to use the cross-collateral trading functionality in Hummingbot, which allows using any available asset as collateral for margin trading and perpetual futures positions.

## Overview

Cross-collateral trading enables strategies to use the full range of available assets as collateral, rather than just the specific trading pair assets. This increases capital efficiency and allows for more flexible trading strategies.

Key features:
- Use any supported asset as collateral for positions
- Automatic collateral selection based on available balances
- Support for weighted allocation of collateral across multiple assets
- Compatible with both margin trading and perpetual futures

## Core Components

The cross-collateral functionality is provided by these key components:

1. **CrossCollateralTradingBase**: 
   - Base class that implements cross-collateral functionality
   - Tracks available collateral assets and their allocation

2. **CrossCollateralDerivativePyBase**: 
   - Extends PerpetualDerivativePyBase with cross-collateral capabilities
   - Bridges between perpetual trading and cross-collateral functionality

3. **CrossCollateralBudgetChecker**: 
   - Validates if sufficient balance exists for orders across multiple collateral assets
   - Extends the standard BudgetChecker with cross-collateral capabilities

4. **CrossCollateralOrderCandidate** and **CrossCollateralPerpetualOrderCandidate**: 
   - Order candidate classes that support specifying custom collateral tokens
   - Allow for weighted allocation of collateral

## How to Implement Cross-Collateral Trading in a Connector

### 1. Extend the appropriate base class

For a perpetual futures connector that needs cross-collateral functionality:

```python
from hummingbot.connector.cross_collateral_derivative_py_base import CrossCollateralDerivativePyBase

class MyExchangeDerivative(CrossCollateralDerivativePyBase):
    # Your connector implementation
    # ...
```

### 2. Initialize valid collateral assets

In your connector's initialization, set up the valid collateral assets:

```python
def __init__(self, ...):
    super().__init__(...)
    
    # Initialize collateral assets
    self._initialize_collateral_assets()

def _initialize_collateral_assets(self):
    # Add assets that can be used as collateral
    self.add_collateral_asset("USDT")
    self.add_collateral_asset("USDC")
    self.add_collateral_asset("BTC")
    
    # For each trading pair, specify which assets can be used as collateral
    for trading_pair in self._trading_pairs:
        self.set_valid_collateral_tokens(
            trading_pair=trading_pair,
            collateral_tokens=["USDT", "USDC", "BTC"]
        )
```

### 3. Implement collateral token methods

Override the collateral token methods to enable cross-collateral functionality:

```python
def get_buy_collateral_token(self, trading_pair: str) -> str:
    # You can return the default collateral token or implement custom logic
    # This will be used by default, but can be overridden when creating order candidates
    return self._trading_rules[trading_pair].buy_order_collateral_token

def get_sell_collateral_token(self, trading_pair: str) -> str:
    return self._trading_rules[trading_pair].sell_order_collateral_token

def get_total_collateral_value(self, quote_currency: str) -> Decimal:
    # Implement logic to calculate the total collateral value
    # This usually involves summing all available collateral and converting to the quote currency
    # ...
```

### 4. Create cross-collateral orders in your strategy

When creating orders in your strategy, you can specify custom collateral tokens:

```python
from hummingbot.connector.cross_collateral_budget_checker import CrossCollateralOrderCandidate, CrossCollateralPerpetualOrderCandidate

# For a regular order with custom collateral
order_candidate = CrossCollateralOrderCandidate(
    trading_pair="BTC-USDT",
    is_maker=False,
    order_type=OrderType.LIMIT,
    order_side=TradeType.BUY,
    amount=Decimal("0.1"),
    price=Decimal("40000"),
    custom_collateral_tokens=["USDC", "ETH"]  # Specify which tokens to use as collateral
)

# For a weighted allocation of collateral
order_candidate.set_collateral_token_weights({
    "USDC": Decimal("0.7"),  # Use 70% USDC
    "ETH": Decimal("0.3")    # Use 30% ETH
})

# For a perpetual position with custom collateral
perpetual_order = CrossCollateralPerpetualOrderCandidate(
    trading_pair="BTC-USDT-PERP",
    is_maker=False,
    order_type=OrderType.LIMIT,
    order_side=TradeType.BUY,
    amount=Decimal("0.1"),
    price=Decimal("40000"),
    leverage=Decimal("5"),
    position_action=PositionAction.OPEN,
    custom_collateral_tokens=["BTC", "USDT"]
)
```

## Example Implementation

See the `example_cross_collateral_derivative.py` file for a complete example of implementing a connector with cross-collateral functionality.

## Compatibility with Existing Code

The cross-collateral functionality is designed to be compatible with the existing Hummingbot architecture:

- Existing connectors continue to work without modification
- Existing strategies can use cross-collateral connectors without changes
- Cross-collateral capabilities are enabled when using the appropriate order candidate classes

For complete strategies that want to take advantage of cross-collateral trading, consider using the cross-collateral order candidate classes to have more control over collateral allocation. 