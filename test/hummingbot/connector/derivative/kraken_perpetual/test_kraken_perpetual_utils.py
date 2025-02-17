import time
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from hummingbot.connector.derivative.kraken_perpetual import (
    kraken_perpetual_constants as CONSTANTS,
    kraken_perpetual_utils as utils,
)


class KrakenPerpetualUtilsTests(TestCase):
    def test_is_exchange_information_valid(self):
        """Test validation of exchange information for trading pairs."""
        # Valid perpetual instrument
        valid_info = {
            "symbol": "PF_XBTUSD",
            "type": "flexible_futures",
            "suspended": False,
        }
        self.assertTrue(utils.is_exchange_information_valid(valid_info))

        # Invalid - suspended
        suspended_info = {
            "symbol": "PI_XBTUSD",
            "type": "flexible_futures",
            "suspended": True,
        }
        self.assertFalse(utils.is_exchange_information_valid(suspended_info))

        # Invalid - not perpetual
        non_perpetual_info = {
            "symbol": "FI_XBTUSD",
            "tag": "month",
            "suspended": False,
        }
        self.assertFalse(utils.is_exchange_information_valid(non_perpetual_info))

        # Invalid - missing symbol
        missing_symbol_info = {
            "type": "flexible_futures",
            "suspended": False,
        }
        self.assertFalse(utils.is_exchange_information_valid(missing_symbol_info))

        # Invalid - missing required fields
        invalid_info = {}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info))

    def test_get_next_funding_timestamp(self):
        # 2024-08-30-01:00:00 UTC
        timestamp = 1724979600
        # 2024-08-30-08:00:00 UTC
        expected_ts = 1725004800
        self.assertEqual(expected_ts, utils.get_next_funding_timestamp(timestamp))

        # 2024-08-30-09:00:00 UTC
        timestamp = 1725008400
        # 2024-08-30-16:00:00 UTC
        expected_ts = 1725033600
        self.assertEqual(expected_ts, utils.get_next_funding_timestamp(timestamp))

        # 2024-08-30-17:00:00 UTC
        timestamp = 1725037200
        # 2024-08-31-00:00:00 UTC
        expected_ts = 1725062400
        self.assertEqual(expected_ts, utils.get_next_funding_timestamp(timestamp))

    def test_convert_from_exchange_trading_pair(self):
        """Test conversion from exchange trading pair format to hummingbot format."""
        # Test inverse perpetual
        self.assertEqual("BTC-USD", utils.convert_from_exchange_trading_pair("PI_XBTUSD"))
        # Test linear perpetual
        self.assertEqual("BTC-USD", utils.convert_from_exchange_trading_pair("PF_XBTUSD"))
        # Test quanto perpetual
        self.assertEqual("BTC-USD", utils.convert_from_exchange_trading_pair("PL_XBTUSD"))
        # Test non-BTC pair
        self.assertEqual("ETH-USD", utils.convert_from_exchange_trading_pair("PI_ETHUSD"))
        # Test non-perpetual pair (should return unchanged)
        self.assertEqual("ETHUSD", utils.convert_from_exchange_trading_pair("ETHUSD"))

    def test_convert_to_exchange_trading_pair(self):
        """Test conversion from hummingbot trading pair format to exchange format."""
        # Test BTC-USD conversion
        self.assertEqual("PF_XBTUSD", utils.convert_to_exchange_trading_pair("BTC-USD"))
        # Test ETH-USD conversion
        self.assertEqual("PF_ETHUSD", utils.convert_to_exchange_trading_pair("ETH-USD"))
        # Test DOGE-USD conversion
        self.assertEqual("PF_DOGEUSD", utils.convert_to_exchange_trading_pair("DOGE-USD"))

    def test_default_fees(self):
        """Test default fee configuration."""
        self.assertEqual(Decimal("0.0002"), utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.0005"), utils.DEFAULT_FEES.taker_percent_fee_decimal)

        # Test testnet fees
        testnet_fees = utils.OTHER_DOMAINS_DEFAULT_FEES["kraken_perpetual_testnet"]
        self.assertEqual(Decimal("0.0002"), testnet_fees.maker_percent_fee_decimal)
        self.assertEqual(Decimal("0.0005"), testnet_fees.taker_percent_fee_decimal)

    def test_asset_conversion(self):
        """Test asset conversion maps."""
        # Test Kraken to Hummingbot conversion
        self.assertEqual("BTC", utils.KRAKEN_TO_HB_ASSETS["XBT"])

        # Test Hummingbot to Kraken conversion
        self.assertEqual("XBT", utils.HB_TO_KRAKEN_ASSETS["BTC"])
