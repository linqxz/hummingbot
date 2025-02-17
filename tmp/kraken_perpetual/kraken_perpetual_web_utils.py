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
        
        # For POST requests with data, use application/x-www-form-urlencoded
        if request.method == RESTMethod.POST or RESTMethod.PUT and request.data is not None:
            request.headers["Content-Type"] = "application/x-www-form-urlencoded"
            # Convert JSON string back to dict if it was converted earlier
            if isinstance(request.data, str):
                try:
                    import json
                    request.data = json.loads(request.data)
                except json.JSONDecodeError:
                    pass  # Keep original string if it's not JSON
            # Convert dict to url-encoded format
            if isinstance(request.data, dict):
                from urllib.parse import urlencode
                request.data = urlencode(request.data)
        else:
            # For other requests, use application/json
            request.headers["Content-Type"] = "application/json"

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
        throttler_limit_id=GET_LIMIT_ID,
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
GET_LIMIT_ID = "GETLimit"
POST_LIMIT_ID = "POSTLimit"
WS_REQUEST_LIMIT_ID = "WSRequestLimit"
WS_CONNECTION_LIMIT_ID = "WSConnectionLimit"

# Endpoint-specific rate limit IDs
ORDER_LIMIT_ID = "OrderLimit"
POSITION_LIMIT_ID = "PositionLimit"
BALANCE_LIMIT_ID = "BalanceLimit"
FILLS_LIMIT_ID = "FillsLimit"

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

    # Order-related endpoints
    if any(x in endpoint for x in ["/sendorder", "/cancelorder", "/orders/status", "/openorders"]):
        return POST_LIMIT_ID if method == RESTMethod.POST else ORDER_LIMIT_ID
    # Position-related endpoints
    elif "/openpositions" in endpoint:
        return POSITION_LIMIT_ID
    # Balance-related endpoints
    elif "/accounts" in endpoint:
        return BALANCE_LIMIT_ID
    # Fills-related endpoints
    elif "/fills" in endpoint:
        return FILLS_LIMIT_ID
    # Default to HTTP method limit
    else:
        return POST_LIMIT_ID if method == RESTMethod.POST else GET_LIMIT_ID

def build_rate_limits() -> List[RateLimit]:
    """
    Build rate limits for Kraken Perpetual.
    :return: List of rate limits
    """
    rate_limits = [
        # Global rate limits
        RateLimit(limit_id=GET_LIMIT_ID, limit=100, time_interval=10),   # 100 GET requests per 10 seconds
        RateLimit(limit_id=POST_LIMIT_ID, limit=50, time_interval=10),   # 50 POST requests per 10 seconds

        # Endpoint-specific rate limits
        RateLimit(limit_id=ORDER_LIMIT_ID, limit=60, time_interval=10),  # 60 order requests per 10 seconds
        RateLimit(limit_id=POSITION_LIMIT_ID, limit=10, time_interval=10),  # 10 position requests per 10 seconds
        RateLimit(limit_id=BALANCE_LIMIT_ID, limit=10, time_interval=10),  # 10 balance requests per 10 seconds
        RateLimit(limit_id=FILLS_LIMIT_ID, limit=10, time_interval=10),  # 10 fills requests per 10 seconds

        # WebSocket limits
        RateLimit(limit_id=WS_REQUEST_LIMIT_ID, limit=100, time_interval=1),    # 100 requests per second
        RateLimit(limit_id=WS_CONNECTION_LIMIT_ID, limit=50, time_interval=60),  # 50 connections per minute
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
        limit=50,  # 50 connections per minute
        time_interval=60,
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
