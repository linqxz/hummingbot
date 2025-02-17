from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "kraken_perpetual"
DEFAULT_DOMAIN = "kraken_perpetual_main"
DEFAULT_TIME_IN_FORCE = "GTC"

MAX_ID_LEN = 36  # Maximum length for client order IDs

HBOT_BROKER_ID = "KRKN"

# Base URLs
REST_URLS = {
    "kraken_perpetual_main": "https://futures.kraken.com",
    "kraken_perpetual_testnet": "https://demo-futures.kraken.com"
}
WSS_URLS = {
    "kraken_perpetual_main": "wss://futures.kraken.com/ws/v1",
    "kraken_perpetual_testnet": "wss://demo-futures.kraken.com/ws/v1"
}
WSS_PUBLIC_URLS = WSS_URLS

WSS_PRIVATE_URLS = WSS_URLS

# REST API Public Endpoints
LATEST_SYMBOL_INFORMATION_ENDPOINT = "/derivatives/api/v3/instruments"
QUERY_SYMBOL_ENDPOINT = "/derivatives/api/v3/instruments/"
QUERY_SYMBOL_STATUS_ENDPOINT = "/derivatives/api/v3/instruments/{symbol}/status"
ORDER_BOOK_ENDPOINT = "/derivatives/api/v3/orderbook"
TICKER_PRICE_ENDPOINT = "/derivatives/api/v3/tickers"
TICKER_SYMBOL_ENDPOINT = "/derivatives/api/v3/tickers/{symbol}"
EXCHANGE_INFO_ENDPOINT = "/derivatives/api/v3/instruments"
SERVER_TIME_PATH_URL = "/derivatives/api/v3/instruments/status"
HISTORICAL_FUNDING_RATES_ENDPOINT = "/derivatives/api/v4/historicalfundingrates"
FEE_SCHEDULES_ENDPOINT = "/derivatives/api/v3/feeschedules"
FEE_VOLUMES_ENDPOINT = "/derivatives/api/v3/feeschedules/volumes"
INITIAL_MARGIN_ENDPOINT = "/derivatives/api/v3/initialmargin"
MAX_ORDER_SIZE_ENDPOINT = "/derivatives/api/v3/initialmargin/maxordersize"
UNWIND_QUEUE_ENDPOINT = "/derivatives/api/v3/unwindqueue"

# REST API Private Endpoints
SET_LEVERAGE_PATH_URL = "/derivatives/api/v3/leveragepreferences"
GET_POSITIONS_PATH_URL = "/derivatives/api/v3/openpositions"
PLACE_ACTIVE_ORDER_PATH_URL = "/derivatives/api/v3/sendorder"
CANCEL_ACTIVE_ORDER_PATH_URL = "/derivatives/api/v3/cancelorder"
CANCEL_ALL_ACTIVE_ORDERS_PATH_URL = "/derivatives/api/v3/cancelallorders"
CANCEL_ALL_ORDERS_AFTER_PATH_URL = "/derivatives/api/v3/cancelallordersafter"
QUERY_ACTIVE_ORDER_PATH_URL = "/derivatives/api/v3/orders/status"
USER_TRADE_RECORDS_PATH_URL = "/derivatives/api/v3/fills"
GET_WALLET_BALANCE_PATH_URL = "/derivatives/api/v3/accounts"
BATCH_ORDER_PATH_URL = "/derivatives/api/v3/batchorder"
EDIT_ORDER_PATH_URL = "/derivatives/api/v3/editorder"
OPEN_ORDERS_PATH_URL = "/derivatives/api/v3/openorders"
NOTIFICATIONS_PATH_URL = "/derivatives/api/v3/notifications"
TRANSFER_PATH_URL = "/derivatives/api/v3/transfer"
WITHDRAWAL_PATH_URL = "/derivatives/api/v3/withdrawal"
SUBACCOUNT_TRANSFER_PATH_URL = "/derivatives/api/v3/transfer/subaccount"
SUBACCOUNTS_LIST_PATH_URL = "/derivatives/api/v3/subaccounts"
SUBACCOUNT_TRADING_ENABLED_PATH_URL = "/derivatives/api/v3/subaccount/{subaccountUid}/trading-enabled"
PNL_PREFERENCES_PATH_URL = "/derivatives/api/v3/pnlpreferences"
SELF_TRADE_STRATEGY_PATH_URL = "/self-trade-strategy"
INITIAL_MARGIN_ENDPOINT = "/derivatives/api/v3/initialmargin"
MAX_ORDER_SIZE_ENDPOINT = "/derivatives/api/v3/initialmargin/maxordersize"

# Assignment Program Endpoints
ASSIGNMENT_ADD_PATH_URL = "/derivatives/api/v3/assignmentprogram/add"
ASSIGNMENT_DELETE_PATH_URL = "/derivatives/api/v3/assignmentprogram/delete"
ASSIGNMENT_CURRENT_PATH_URL = "/derivatives/api/v3/assignmentprogram/current"
ASSIGNMENT_HISTORY_PATH_URL = "/derivatives/api/v3/assignmentprogram/history"

# History API Endpoints
HISTORY_BASE_ENDPOINT = "/api/history/v2/history"
HISTORY_EXECUTIONS_ENDPOINT = "/api/history/v2/executions"
HISTORY_ORDERS_ENDPOINT = "/api/history/v2/orders"
HISTORY_ACCOUNT_LOG_ENDPOINT = "/api/history/v2/account-log"
HISTORY_ACCOUNT_LOG_CSV_ENDPOINT = "/api/history/v2/accountlogcsv"
HISTORY_MARKET_EXECUTIONS_ENDPOINT = "/api/history/v2/market/{tradeable}/executions"
HISTORY_MARKET_ORDERS_ENDPOINT = "/api/history/v2/market/{tradeable}/orders"
HISTORY_MARKET_PRICE_ENDPOINT = "/api/history/v2/market/{tradeable}/price"
HISTORY_TRIGGERS_ENDPOINT = "/api/history/v2/triggers"

# Charts API Endpoints
CHARTS_BASE_ENDPOINT = "/api/charts/v1"
CHARTS_LIQUIDITY_POOL_ENDPOINT = "/api/charts/v1/analytics/liquidity-pool"
CHARTS_ANALYTICS_ENDPOINT = "/api/charts/v1/analytics/{symbol}/{analytics_type}"
CHARTS_TICK_ENDPOINT = "/api/charts/v1/{tick_type}"
CHARTS_TICK_SYMBOL_ENDPOINT = "/api/charts/v1/{tick_type}/{symbol}"
CHARTS_TICK_SYMBOL_RESOLUTION_ENDPOINT = "/api/charts/v1/{tick_type}/{symbol}/{resolution}"

# WebSocket Channels/Feeds
WS_TRADES_TOPIC = "trade"
WS_ORDER_BOOK_EVENTS_TOPIC = "book"
WS_INSTRUMENTS_INFO_TOPIC = "ticker"
WS_INSTRUMENTS_INFO_LITE_TOPIC = "ticker_lite"
WS_FILLS_TOPIC = "fills"
WS_OPEN_POSITIONS_TOPIC = "open_positions"
WS_OPEN_ORDERS_TOPIC = "open_orders"
WS_OPEN_ORDERS_VERBOSE_TOPIC = "open_orders_verbose"
WS_BALANCES_TOPIC = "balances"
WS_NOTIFICATIONS_AUTH_TOPIC = "notifications_auth"
WS_ACCOUNT_LOG_TOPIC = "account_log"
WS_HEARTBEAT_TOPIC = "heartbeat"
WS_CHALLENGE_TOPIC = "challenge"

# WebSocket Heartbeat Interval (in seconds)
WS_HEARTBEAT_TIME_INTERVAL = 30

# API Versions (for reference)

# Order Type Mapping
ORDER_TYPE_MAP = {
    OrderType.LIMIT: "lmt",            # A limit order
    OrderType.MARKET: "mkt",           # An immediate-or-cancel order with 1% price protection
    OrderType.LIMIT_MAKER: "post",     # A post-only limit order
    # OrderType.STOP_LOSS: "stp",                      # A stop order
    # OrderType.TAKE_PROFIT: "take_profit",     # A take profit order
    # OrderType.IOC: "ioc",     # A take profit order                    # An immediate-or-cancel order
    # OrderType.TRAILING_STOP:"trailing_stop"   # A trailing stop order
}

# Order State Mapping
ORDER_STATE = {
    "untouched": OrderState.OPEN,
    "placed": OrderState.OPEN,
    "CANCELLED": OrderState.CANCELED,
    "pending": OrderState.PENDING_CREATE,
    "open": OrderState.OPEN,
    "closed": OrderState.FILLED,
    "edited": OrderState.PENDING_CANCEL,
    "rejected": OrderState.FAILED,
    "triggered": OrderState.OPEN,
    "untriggered": OrderState.PENDING_CREATE,
    "expired": OrderState.CANCELED,
    "ENTERED_BOOK": OrderState.OPEN,
    "FULLY_EXECUTED": OrderState.FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "FILLED": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "CANCELED": OrderState.CANCELED,
    "FAILED": OrderState.FAILED,
    "REJECTED": OrderState.FAILED,
    "INSUFFICIENT_MARGIN": OrderState.FAILED,
    "UNKNOWN": OrderState.FAILED,
    "new_placed_order_by_user": OrderState.OPEN,
    "liquidation": OrderState.CANCELED,
    "stop_order_triggered": OrderState.CANCELED,
    "limit_order_from_stop": OrderState.OPEN,
    "partial_fill": OrderState.PARTIALLY_FILLED,
    "full_fill": OrderState.FILLED,
    "cancelled_by_user": OrderState.CANCELED,
    "contract_expired": OrderState.CANCELED,
    "not_enough_margin": OrderState.FAILED,
    "market_inactive": OrderState.CANCELED,
    "cancelled_by_admin": OrderState.CANCELED,
    "dead_man_switch": OrderState.CANCELED,
    "ioc_order_failed_because_it_would_not_be_executed": OrderState.FAILED,
    "post_order_failed_because_it_would_filled": OrderState.FAILED,
    "would_execute_self": OrderState.FAILED,
    "would_not_reduce_position": OrderState.FAILED,
    "order_for_edit_not_found": OrderState.FAILED
}

WS_ORDER_STATE = {
    "new_placed_order_by_user": OrderState.OPEN,
    "liquidation": OrderState.CANCELED,
    "stop_order_triggered": OrderState.CANCELED,
    "limit_order_from_stop": OrderState.OPEN,
    "partial_fill": OrderState.PARTIALLY_FILLED,
    "full_fill": OrderState.FILLED,
    "cancelled_by_user": OrderState.CANCELED,
    "contract_expired": OrderState.CANCELED,
    "not_enough_margin": OrderState.FAILED,
    "market_inactive": OrderState.CANCELED,
    "cancelled_by_admin": OrderState.CANCELED,
    "dead_man_switch": OrderState.CANCELED,
    "ioc_order_failed_because_it_would_not_be_executed": OrderState.FAILED,
    "post_order_failed_because_it_would_filled": OrderState.FAILED,
    "would_execute_self": OrderState.FAILED,
    "would_not_reduce_position": OrderState.FAILED,

    "order_for_edit_not_found": OrderState.FAILED,
}

# Default fees
DEFAULT_FEES = {
    "maker": 0.0002,  # 0.02%
    "taker": 0.0005,  # 0.05%
}

# Trading pair prefixes
PERPETUAL_PREFIXES = {
    "FUTURES": "PF_",  # Perpetual Futures prefix
#     "LINEAR": "PL_",  # Linear perpetuals
#     "INVERSE": "PI_" # Inverse perpetualLi
}

SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE = 30

# Rate Limits (in seconds)
DERIVATIVES_RATE_LIMIT_TIME = 10
HISTORY_RATE_LIMIT_TIME = 600  # 10 minutes

# Rate Limit Costs
DERIVATIVES_RATE_LIMIT_COST = 500  # per 10 seconds
HISTORY_RATE_LIMIT_COST = 100     # per 10 minutes

# Order Operation Costs
ORDER_SEND_COST = 10
ORDER_EDIT_COST = 10
ORDER_CANCEL_COST = 10
BATCH_ORDER_BASE_COST = 9  # Additional cost based on batch size
CANCEL_ALL_ORDERS_COST = 25

# Account Operation Costs
ACCOUNT_INFO_COST = 2
POSITIONS_INFO_COST = 2
OPEN_ORDERS_COST = 2
ORDER_STATUS_COST = 1

# Fill Operation Costs
FILLS_COST = 2  # Without lastFillTime
FILLS_WITH_TIME_COST = 25  # With lastFillTime

# High Cost Operations
WITHDRAWAL_COST = 100
UNWIND_QUEUE_COST = 200

# Preferences Operation Costs
LEVERAGE_GET_COST = 2
LEVERAGE_PUT_COST = 10
PNL_GET_COST = 2
PNL_PUT_COST = 10

# Assignment Program Costs
ASSIGNMENT_READ_COST = 10
ASSIGNMENT_CURRENT_COST = 50
ASSIGNMENT_DELETE_COST = 50
ASSIGNMENT_HISTORY_COST = 50
ASSIGNMENT_ADD_COST = 100

# Transfer Operation Costs
TRANSFER_COST = 10
SUBACCOUNT_TRANSFER_COST = 10

# Other Operation Costs
TRADING_ENABLED_COST = 2
SELF_TRADE_COST = 2

# History Operation Costs
HISTORY_ORDER_COST = 1
HISTORY_TRIGGER_COST = 1
HISTORY_EXECUTION_COST = 1
HISTORY_ACCOUNT_LOG_CSV_COST = 6

# Special case: Account Log costs vary by count
ACCOUNT_LOG_COSTS = {
    "default": 1,    # count: 1-25
    "medium": 2,     # count: 26-50
    "large": 3,      # count: 51-1000
    "xlarge": 6,     # count: 1001-5000
    "xxlarge": 10    # count: 5001-100000
}

POSITION_IDX_ONEWAY = 0
