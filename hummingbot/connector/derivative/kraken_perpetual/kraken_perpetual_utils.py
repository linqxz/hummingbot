import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict

try:
    from pydantic.v1 import Field, SecretStr
except ImportError:
    from pydantic.v1 import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.connector.derivative.kraken_perpetual import kraken_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USD"  # Bitcoin-USD perpetual contract

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),  # 0.02%
    taker_percent_fee_decimal=Decimal("0.0005"),  # 0.05%
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    symbol = exchange_info.get("symbol", "")
    tradeable = exchange_info.get("tradeable", False)
    
    # Check if it's a perpetual futures pair (starts with PF_) and is tradeable
    valid = (symbol.startswith("PF_") and 
            symbol.endswith("USD") and 
            tradeable is True)
    
    return valid


KRAKEN_TO_HB_ASSETS = {
    "XBT": "BTC",
}

HB_TO_KRAKEN_ASSETS = {
    "BTC": "XBT",
}


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    """
    Converts a trading pair from Kraken Perpetual format (PF_XBTUSD) to Hummingbot format (BTC-USD)
    Non-perpetual pairs are returned unchanged.
    """
    logger = logging.getLogger(__name__)
    # logger.info(f"\n=== Converting from exchange trading pair: {exchange_trading_pair} ===")
    
    if not exchange_trading_pair:
        logger.warning("Empty exchange_trading_pair received")
        return exchange_trading_pair

    # Check if it's a perpetual pair
    is_perpetual = exchange_trading_pair.startswith(CONSTANTS.PERPETUAL_PREFIXES["FUTURES"])  # This is "PF_"
    # logger.info(f"Is perpetual pair: {is_perpetual}")
    if not is_perpetual:
        logger.info("Not a perpetual pair, returning unchanged")
        return exchange_trading_pair

    # Remove prefix for perpetual pairs
    trading_pair = exchange_trading_pair[3:]  # Remove PF_
    # logger.info(f"After removing prefix: {trading_pair}")

    # For Kraken Perpetual, all pairs end in USD
    if not trading_pair.endswith("USD"):
        logger.warning(f"Trading pair does not end in USD: {trading_pair}")
        return trading_pair

    # Extract base by removing USD
    base = trading_pair[:-3]
    # logger.info(f"Extracted base: {base}")

    # Convert XBT to BTC if needed
    if base == "XBT":
        # logger.info("Converting XBT to BTC")
        base = "BTC"

    result = f"{base}-USD"
    # logger.info(f"Final converted pair: {result}")
    return result


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    """
    Converts a trading pair from Hummingbot format (BTC-USD) to Kraken Perpetual format (PF_XBTUSD)
    Uses PF_ prefix by default for linear perpetual contracts
    """
    logger = logging.getLogger(__name__)
    # logger.info(f"\n=== Converting to exchange trading pair: {hb_trading_pair} ===")
    
    if not hb_trading_pair:
        logger.warning("Empty hb_trading_pair received")
        return hb_trading_pair

    try:
        base, quote = hb_trading_pair.split("-")
        # logger.info(f"Split into base: {base}, quote: {quote}")
        
        # Convert BTC to XBT for Kraken
        if base.upper() == "BTC":
            # logger.info("Converting BTC to XBT")
            base = "XBT"
            
        result = f"PF_{base.upper()}{quote.upper()}"
        # logger.info(f"Final exchange pair: {result}")
        return result
    except Exception as e:
        logger.error(f"Error converting trading pair: {str(e)}", exc_info=True)
        return None


# Alias for backward compatibility
exchange_symbol_associated_to_pair = convert_from_exchange_trading_pair
get_exchange_trading_pair = convert_to_exchange_trading_pair


def get_next_funding_timestamp(current_timestamp: float) -> float:
    """
    Get the next funding timestamp.
    Kraken funding occurs every 8 hours at 00:00 UTC, 08:00 UTC, and 16:00 UTC.

    :param current_timestamp: Current timestamp in seconds
    :return: Next funding timestamp in seconds
    """
    current_time = datetime.fromtimestamp(current_timestamp, tz=timezone.utc)
    next_date = current_time.date()

    funding_hours = [0, 8, 16]
    current_hour = current_time.hour

    next_funding_hour = next(
        (hour for hour in funding_hours if hour > current_hour),
        funding_hours[0]  # If no hours remaining today, use first hour tomorrow
    )

    if next_funding_hour <= current_hour:
        # Move to next day
        next_date = (current_time + timedelta(days=1)).date()

    next_funding_time = datetime.combine(
        next_date,
        datetime.min.time().replace(hour=next_funding_hour),
        timezone.utc
    )

    return next_funding_time.timestamp()


def get_client_order_id(order_side: str, trading_pair: str) -> str:
    """
    Generate a client order ID based on trade characteristics.

    :param order_side: The side of the order (buy/sell)
    :param trading_pair: The trading pair being traded
    :return: A unique client order ID
    """
    timestamp = int(time.time() * 1000)
    order_id = f"{order_side}_{trading_pair}_{timestamp}"
    return order_id[:CONSTANTS.MAX_ID_LEN]


class KrakenPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="kraken_perpetual", client_data=None)
    kraken_perpetual_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Kraken Perpetual API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    kraken_perpetual_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Kraken Perpetual secret key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "kraken_perpetual"


KEYS = KrakenPerpetualConfigMap.construct()

OTHER_DOMAINS = ["kraken_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"kraken_perpetual_testnet": "kraken_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"kraken_perpetual_testnet": "XBT-USD"}
OTHER_DOMAINS_DEFAULT_FEES = {
    "kraken_perpetual_testnet": TradeFeeSchema(
        maker_percent_fee_decimal=Decimal("0.0002"),  # 0.02%
        taker_percent_fee_decimal=Decimal("0.0005"),  # 0.05%
    )
}


class KrakenPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="kraken_perpetual_testnet", client_data=None)
    kraken_perpetual_testnet_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Kraken Perpetual Testnet API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    kraken_perpetual_testnet_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Kraken Perpetual Testnet secret key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "kraken_perpetual_testnet"


OTHER_DOMAINS_KEYS = {
    "kraken_perpetual_testnet": KrakenPerpetualTestnetConfigMap.construct()
}
