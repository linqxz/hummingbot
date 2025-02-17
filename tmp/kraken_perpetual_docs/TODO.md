# Kraken Perpetual Connector TODO List

## Bybit Features Not Available in Kraken

### Position Mode
- Bybit supports both one-way and hedge modes via `POSITION_MODE_MAP`
- Kraken doesn't have equivalent position mode switching
- Need to handle this difference in position management logic

### Rate Limiting
- Bybit uses bucket-based rate limiting with different buckets for linear/non-linear markets
- Kraken uses two separate rate limit pools:
  1. Derivatives Pool:
     - 500 cost units per 10 seconds
     - Different endpoints have different costs
     - Need to track cumulative cost and reset every 10 seconds
  2. History Pool:
     - 100 tokens that replenish at 100 tokens per 10 minutes
     - Need to implement token bucket algorithm with replenishment
     - Account for different costs based on parameters (e.g., account log count ranges)
- Implementation needs:
  - Track separate pools for /derivatives and /history endpoints
  - Implement token bucket with replenishment for history endpoints
  - Calculate costs based on endpoint and parameters (especially for batch orders and account log)
  - Handle rate limit errors and implement backoff strategy

### WebSocket Subscription Model
- Bybit has specific subscription endpoints:
  ```python
  WS_AUTHENTICATE_USER_ENDPOINT_NAME = "auth"
  WS_SUBSCRIPTION_POSITIONS_ENDPOINT_NAME = "position"
  WS_SUBSCRIPTION_ORDERS_ENDPOINT_NAME = "order"
  WS_SUBSCRIPTION_EXECUTIONS_ENDPOINT_NAME = "execution"
  WS_SUBSCRIPTION_WALLET_ENDPOINT_NAME = "wallet"
  ```
- Kraken uses direct feed subscriptions without separate endpoints
- Need to adapt WebSocket subscription logic

### Error Codes
- Bybit has detailed error code mapping:
  ```python
  RET_CODE_MODE_POSITION_NOT_EMPTY = 110024
  RET_CODE_MODE_NOT_MODIFIED = 110025
  RET_CODE_MODE_ORDER_NOT_EMPTY = 110028
  RET_CODE_HEDGE_NOT_SUPPORTED = 110029
  RET_CODE_LEVERAGE_NOT_MODIFIED = 110043
  RET_CODE_ORDER_NOT_EXISTS = 110001
  ```
- Kraken uses string-based error codes
- Need to map Kraken's error responses to Hummingbot's error handling system

### Market Types
- Bybit has LINEAR/NON_LINEAR market distinction
- Kraken uses multi-collateral system with pl_/pi_ prefixes
- Need to adapt market type handling

## Kraken-Specific Features to Implement

### Multi-Collateral Support
- Handle dynamic collateral types from accounts endpoint
- Monitor collateral balances via WebSocket feed
- Implement collateral-specific margin calculations

### Assignment Program
- Implement assignment program functionality
- Handle flex/fixed contract types
- Monitor assignment status

### Additional API Features
- Implement transfer/withdrawal endpoints
- Handle market-specific endpoints (executions, orders, price)
- Support CSV account log retrieval

## Testing Requirements

1. Rate Limiting
   - Verify cost-based rate limiting works correctly
   - Test rate limit handling during high-frequency operations

2. WebSocket Feeds
   - Test all Kraken-specific feeds (account_log, balances)
   - Verify feed reconnection and error handling

3. Multi-Collateral
   - Test trading with different collateral types
   - Verify margin calculations
   - Test collateral balance updates

4. Error Handling
   - Test all Kraken-specific error scenarios
   - Verify proper error mapping to Hummingbot system

## Documentation Needs

1. Update connector documentation with:
   - Kraken-specific features
   - Multi-collateral handling
   - Rate limiting differences
   - WebSocket feed structure

2. Add examples for:
   - Multi-collateral trading
   - Assignment program usage
   - Market-specific operations
