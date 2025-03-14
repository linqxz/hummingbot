from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

from hummingbot.connector.derivative.kraken_perpetual import kraken_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class KrakenPerpetualRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        request.headers = request.headers or {}

        # Convert dict to url-encoded format
        if isinstance(request.data, dict):
            from urllib.parse import urlencode
            request.data = urlencode(sorted(request.data.items()))  # Sort items for consistent ordering

        return request


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """Create Web Assistant Factory with necessary pre-processors."""
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(throttler=throttler))
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
            KrakenPerpetualRESTPreProcessor(),
        ],
    )
    return api_factory


def create_throttler() -> AsyncThrottler:
    """Create throttler with rate limits."""
    throttler = AsyncThrottler(build_rate_limits())
    return throttler


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN
) -> float:
    """Get current server time from Kraken Perpetual using instruments/status endpoint."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()

    url = get_rest_url_for_endpoint(
        endpoint=CONSTANTS.SERVER_TIME_PATH_URL,
        domain=domain
    )

    response = await rest_assistant.execute_request(
        url=url,
        method=RESTMethod.GET,
        throttler_limit_id=PUBLIC_LIMIT_ID,
    )

    if response.get("serverTime") is not None:
        server_time = response["serverTime"]
        dt = datetime.strptime(server_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.timestamp() * 1000
    else:
        raise ValueError("Failed to get server time from Kraken")


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    """Create Web Assistant Factory without time synchronizer."""
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[KrakenPerpetualRESTPreProcessor()],
    )
    return api_factory


def get_rest_url_for_endpoint(
    endpoint: Union[str, Dict[str, Any]],
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    api_version: Optional[str] = None,  # Kept for backward compatibility
    trading_pair: Optional[str] = None,
    symbol: Optional[str] = None,
    subaccount_uid: Optional[str] = None
) -> str:
    """
    Get the REST URL for a specific endpoint.
    :param endpoint: The endpoint to get the URL for (can be string or nested dict)
    :param domain: The domain to use (main or testnet)
    :param api_version: Deprecated - API version is now embedded in endpoint paths
    :param trading_pair: Optional trading pair to include in the URL
    :param symbol: Optional symbol to include in the URL
    :param subaccount_uid: Optional subaccount UID to include in the URL
    :return: The REST URL
    """
    url = CONSTANTS.REST_URLS[domain]

    # Handle nested endpoint dictionaries
    if isinstance(endpoint, dict):
        endpoint = next(iter(endpoint.values()))  # Use first value as default

    # Remove leading slash if present to avoid double slashes
    if isinstance(endpoint, str) and endpoint.startswith("/"):
        endpoint = endpoint[1:]

    # Replace placeholders in URL
    if isinstance(endpoint, str):
        if trading_pair is not None:
            endpoint = endpoint.replace("{trading_pair}", trading_pair)
        if symbol is not None:
            endpoint = endpoint.replace("{symbol}", symbol)
        if subaccount_uid is not None:
            endpoint = endpoint.replace("{subaccountUid}", subaccount_uid)

    url = f"{url}/{endpoint}"
    return url


def get_ws_url(domain: str = CONSTANTS.DEFAULT_DOMAIN, is_auth: bool = False) -> str:
    """
    Get the WebSocket URL for a specific domain.
    :param domain: The domain to use (main or testnet)
    :param is_auth: Whether this is for an authenticated connection
    :return: The WebSocket URL
    """
    base_url = CONSTANTS.WSS_URLS[domain]
    return base_url  # Kraken uses same URL for both public and private


# Global rate limit IDs
PUBLIC_LIMIT_ID = "PublicLimit"  # For public endpoints with no rate limit
DERIVATIVES_LIMIT_ID = "DerivativesLimit"  # For /derivatives endpoints
HISTORY_LIMIT_ID = "HistoryLimit"  # For /history endpoints
WS_REQUEST_LIMIT_ID = "WSRequestLimit"
WS_CONNECTION_LIMIT_ID = "WSConnectionLimit"

# Endpoint costs
SEND_ORDER_COST = 10
EDIT_ORDER_COST = 10
CANCEL_ORDER_COST = 10
BATCH_ORDER_BASE_COST = 9
CANCEL_ALL_ORDERS_COST = 25
ACCOUNT_INFO_COST = 2
POSITIONS_INFO_COST = 2
FILLS_COST = 2
FILLS_WITH_TIME_COST = 25
ORDER_STATUS_COST = 1
OPEN_ORDERS_COST = 2
LEVERAGE_GET_COST = 2
LEVERAGE_PUT_COST = 10

def get_rest_api_limit_id_for_endpoint(endpoint: Union[str, Dict[str, str]], method: RESTMethod = RESTMethod.GET) -> str:
    """
    Get the rate limit ID for the specified endpoint.
    :param endpoint: The endpoint path or dictionary of paths
    :param method: The HTTP method for the request
    :return: The rate limit ID for the endpoint
    """
    # Convert endpoint to string if it's a dictionary
    if isinstance(endpoint, dict):
        endpoint = next(iter(endpoint.values()))

    # Public endpoints with no rate limits
    if any(endpoint.startswith(prefix) for prefix in [
        "/derivatives/api/v3/instruments",
        "/derivatives/api/v3/tickers",
        "/derivatives/api/v3/orderbook",
        "/derivatives/api/v3/ticker",
        "/derivatives/api/v3/time",
    ]):
        return PUBLIC_LIMIT_ID
    
    # History endpoints use the history pool
    if "/history" in endpoint:
        return HISTORY_LIMIT_ID
    
    # All other endpoints use the derivatives pool
    return DERIVATIVES_LIMIT_ID

def build_rate_limits() -> List[RateLimit]:
    """
    Build rate limits for Kraken Perpetual.
    :return: List of rate limits
    """
    rate_limits = [
        # Public endpoints pool - no rate limit
        RateLimit(
            limit_id=PUBLIC_LIMIT_ID,
            limit=10000000,  # High number
            time_interval=1,
            weight=0,  # No weight cost
        ),

        # Main derivatives endpoints pool - 500 cost per 10 seconds
        RateLimit(
            limit_id=DERIVATIVES_LIMIT_ID,
            limit=500,
            time_interval=10,
            linked_limits=[
                LinkedLimitWeightPair("/derivatives/api/v3/sendorder", SEND_ORDER_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/editorder", EDIT_ORDER_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/cancelorder", CANCEL_ORDER_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/cancelallorders", CANCEL_ALL_ORDERS_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/accounts", ACCOUNT_INFO_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/openpositions", POSITIONS_INFO_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/fills", FILLS_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/orders/status", ORDER_STATUS_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/openorders", OPEN_ORDERS_COST),
                LinkedLimitWeightPair("/derivatives/api/v3/leveragepreferences", LEVERAGE_GET_COST),
            ]
        ),

        # History endpoints pool - 100 cost per 10 minutes
        RateLimit(
            limit_id=HISTORY_LIMIT_ID,
            limit=100,
            time_interval=600,  # 10 minutes
            linked_limits=[
                LinkedLimitWeightPair("/derivatives/api/v3/historicalorders", 1),
                LinkedLimitWeightPair("/derivatives/api/v3/historicaltriggers", 1),
                LinkedLimitWeightPair("/derivatives/api/v3/historicalexecutions", 1),
                LinkedLimitWeightPair("/derivatives/api/v3/accountlog", 1),  # Default cost, varies by count parameter
            ]
        ),

        # WebSocket limits
        RateLimit(limit_id=WS_REQUEST_LIMIT_ID, limit=100, time_interval=1),    # 100 requests per second
        RateLimit(limit_id=WS_CONNECTION_LIMIT_ID, limit=100, time_interval=0),  # 100 concurrent connections
    ]

    return rate_limits


def build_ws_rate_limit() -> RateLimit:
    """
    Build WebSocket specific rate limit.
    :return: RateLimit configuration for WebSocket
    """
    return RateLimit(
        limit_id=WS_REQUEST_LIMIT_ID,
        limit=100,  # 100 requests per second
        time_interval=1,
    )


def build_ws_connection_limit() -> RateLimit:
    """
    Build WebSocket connection rate limit.
    :return: RateLimit configuration for WebSocket connections
    """
    return RateLimit(
        limit_id=WS_CONNECTION_LIMIT_ID,
        limit=100,  # 100 concurrent connections
        time_interval=0,
    )


def endpoint_from_message(message: Dict[str, Any]) -> Optional[str]:
    """Extract endpoint/feed from WebSocket message."""
    if not isinstance(message, dict):
        return None

    if "event" in message:
        return message["event"]

    if "feed" in message:
        return message["feed"]

    return None


def payload_from_message(message: Dict[str, Any]) -> Any:
    """Extract payload from WebSocket message."""
    if not isinstance(message, dict):
        return message

    if "event" in message:
        return message

    if "feed" in message:
        payload = message.copy()
        payload.pop("feed")
        return payload

    return message


def get_ws_message_payload(channel: str, is_auth: bool = False, **kwargs) -> Dict[str, Any]:
    """
    Build WebSocket subscription message payload.
    :param channel: The channel to subscribe to
    :param is_auth: Whether this is an authenticated subscription
    :param kwargs: Additional subscription parameters
    :return: Subscription message payload
    """
    payload = {
        "event": "subscribe",
        "feed": channel
    }
    if is_auth:
        # Add auth-specific fields if needed
        pass
    payload.update(kwargs)
    return payload


def public_rest_url(endpoint: str, domain: str = CONSTANTS.DEFAULT_DOMAIN, api_version: Optional[str] = None) -> str:
    """Get public REST URL for endpoint."""
    return get_rest_url_for_endpoint(endpoint=endpoint, domain=domain, api_version=api_version)


def private_rest_url(endpoint: str, domain: str = CONSTANTS.DEFAULT_DOMAIN, api_version: Optional[str] = None) -> str:
    """Get private REST URL for endpoint."""
    return get_rest_url_for_endpoint(endpoint=endpoint, domain=domain, api_version=api_version)


def wss_public_url(domain: Optional[str] = None) -> str:
    """Get public WebSocket URL."""
    if domain is None:
        domain = CONSTANTS.DEFAULT_DOMAIN

    # Map domain to the expected format
    mapped_domain = "kraken_perpetual_main" if domain == CONSTANTS.DEFAULT_DOMAIN else domain
    return CONSTANTS.WSS_PUBLIC_URLS[mapped_domain]


def wss_private_url(domain: Optional[str] = None) -> str:
    """Get private WebSocket URL."""
    if domain is None:
        domain = CONSTANTS.DEFAULT_DOMAIN

    # Map domain to the expected format
    mapped_domain = "kraken_perpetual_main" if domain == CONSTANTS.DEFAULT_DOMAIN else domain
    return CONSTANTS.WSS_PRIVATE_URLS[mapped_domain]


# Initialize rate limits
RATE_LIMITS = build_rate_limits()
