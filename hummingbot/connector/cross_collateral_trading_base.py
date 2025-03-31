from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.data_type.common import PositionMode

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class CrossCollateralTradingBase(ExchangeBase):
    """
    CrossCollateralTradingBase provides functionality for trading with cross-collateral capabilities.
    
    This base class can be used by both margin trading and perpetual futures connectors
    that support using any available asset as collateral for positions.
    """

    def __init__(self, client_config_map: "ClientConfigAdapter"):
        super().__init__(client_config_map)
        # Dictionary mapping from trading pair to leverage
        self._leverage = {}
        # Set of assets that can be used as collateral
        self._collateral_assets = set()
        # Dictionary mapping from trading pair to position mode (HEDGE or ONEWAY)
        self._position_mode = None
        # Dictionary to track allocation of collateral to positions
        self._allocated_collateral = {}
        # Map of trading pair to list of valid collateral tokens for that pair
        self._valid_collateral_tokens = {}

    def supported_position_modes(self) -> List[PositionMode]:
        """
        Returns a list containing the position modes supported by the exchange
        (ONEWAY and/or HEDGE modes)
        """
        return [PositionMode.ONEWAY]
    
    def set_position_mode(self, position_mode: PositionMode):
        """
        Sets the position mode for the exchange. This should be overridden by the specific
        connector if the exchange requires interaction to set the mode.
        
        :param position_mode: ONEWAY or HEDGE position mode
        """
        self._position_mode = position_mode
        return

    def set_leverage(self, trading_pair: str, leverage: int = 1):
        """
        Sets the leverage for a specific trading pair. This should be overridden by the specific
        connector if the exchange requires interaction to set leverage.
        
        :param trading_pair: The trading pair to set leverage for
        :param leverage: The leverage value to set (default 1)
        """
        self._leverage[trading_pair] = leverage
        return
    
    def get_leverage(self, trading_pair: str) -> int:
        """
        Gets the current leverage setting for a trading pair
        
        :param trading_pair: The trading pair to get leverage for
        :return: The current leverage value
        """
        return self._leverage.get(trading_pair, 1)

    def add_collateral_asset(self, asset: str):
        """
        Adds an asset to the set of valid collateral assets
        
        :param asset: The asset to add as valid collateral
        """
        self._collateral_assets.add(asset)
    
    def remove_collateral_asset(self, asset: str):
        """
        Removes an asset from the set of valid collateral assets
        
        :param asset: The asset to remove from valid collateral
        """
        if asset in self._collateral_assets:
            self._collateral_assets.remove(asset)
    
    def get_collateral_assets(self) -> Set[str]:
        """
        Returns the set of all assets that can be used as collateral
        
        :return: Set of collateral asset symbols
        """
        return self._collateral_assets
    
    def set_valid_collateral_tokens(self, trading_pair: str, collateral_tokens: List[str]):
        """
        Sets the list of valid collateral tokens for a specific trading pair
        
        :param trading_pair: The trading pair to set collateral tokens for
        :param collateral_tokens: List of token symbols that can be used as collateral
        """
        self._valid_collateral_tokens[trading_pair] = collateral_tokens
    
    def get_valid_collateral_tokens(self, trading_pair: str) -> List[str]:
        """
        Gets the list of valid collateral tokens for a specific trading pair
        
        :param trading_pair: The trading pair to get collateral tokens for
        :return: List of valid collateral token symbols
        """
        return self._valid_collateral_tokens.get(trading_pair, [])
    
    def allocate_collateral(self, trading_pair: str, asset: str, amount: Decimal):
        """
        Allocates a specific amount of a collateral asset to a trading pair
        
        :param trading_pair: The trading pair to allocate collateral for
        :param asset: The collateral asset to allocate
        :param amount: The amount to allocate
        """
        if trading_pair not in self._allocated_collateral:
            self._allocated_collateral[trading_pair] = {}
        
        if asset not in self._allocated_collateral[trading_pair]:
            self._allocated_collateral[trading_pair][asset] = Decimal("0")
            
        self._allocated_collateral[trading_pair][asset] += amount
    
    def free_collateral(self, trading_pair: str, asset: str, amount: Decimal):
        """
        Frees a specific amount of collateral that was previously allocated
        
        :param trading_pair: The trading pair to free collateral from
        :param asset: The collateral asset to free
        :param amount: The amount to free
        """
        if (trading_pair in self._allocated_collateral and 
            asset in self._allocated_collateral[trading_pair]):
            self._allocated_collateral[trading_pair][asset] -= amount
            
            # Remove if zero or negative
            if self._allocated_collateral[trading_pair][asset] <= Decimal("0"):
                del self._allocated_collateral[trading_pair][asset]
            
            # Remove empty dictionaries
            if not self._allocated_collateral[trading_pair]:
                del self._allocated_collateral[trading_pair]
    
    def get_allocated_collateral(self, trading_pair: Optional[str] = None) -> Dict:
        """
        Gets the currently allocated collateral
        
        :param trading_pair: Optional trading pair to filter by
        :return: Dictionary of allocated collateral
        """
        if trading_pair is None:
            return self._allocated_collateral
        return self._allocated_collateral.get(trading_pair, {})
    
    def get_available_collateral(self, asset: str) -> Decimal:
        """
        Calculates the available amount of a collateral asset that is not currently allocated
        
        :param asset: The collateral asset to check
        :return: Available amount of the asset for use as collateral
        """
        total_balance = self.get_balance(asset)
        
        # Sum all allocated amounts of this asset across all trading pairs
        allocated = Decimal("0")
        for tp_allocations in self._allocated_collateral.values():
            if asset in tp_allocations:
                allocated += tp_allocations[asset]
                
        return max(total_balance - allocated, Decimal("0"))
    
    def get_all_available_collateral_balances(self) -> Dict[str, Decimal]:
        """
        Returns a dictionary mapping each collateral asset to its available balance
        
        :return: Dictionary of available collateral balances
        """
        result = {}
        for asset in self._collateral_assets:
            result[asset] = self.get_available_collateral(asset)
        return result
    
    def get_total_collateral_value(self, quote_currency: str) -> Decimal:
        """
        Calculates the total value of all available collateral in terms of the specified quote currency
        
        :param quote_currency: The currency to express the total value in
        :return: Total collateral value in the specified currency
        """
        # This method should be implemented by the concrete connector class
        # as it requires exchange-specific knowledge about how to convert between assets
        raise NotImplementedError 