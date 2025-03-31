import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from hummingbot.connector.cross_collateral_budget_checker import CrossCollateralBudgetChecker
from hummingbot.connector.cross_collateral_trading_base import CrossCollateralTradingBase
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.core.data_type.common import PositionMode, TradeType

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class CrossCollateralDerivativePyBase(PerpetualDerivativePyBase, CrossCollateralTradingBase):
    """
    Cross-collateral derivative base class that combines the capabilities of PerpetualDerivativePyBase
    with the cross-collateral functionality provided by CrossCollateralTradingBase.
    
    This class enables perpetual derivatives to use any available asset as collateral for positions.
    """
    
    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        trading_pairs: List[str],
        trading_required: bool = True,
    ):
        """
        Initialize the cross-collateral derivative base.
        
        :param client_config_map: The client configuration map
        :param trading_pairs: List of trading pairs to trade
        :param trading_required: Whether trading is required for this connector
        """
        # Initialize the CrossCollateralTradingBase first to ensure 
        # it has proper collateral asset tracking initialized
        CrossCollateralTradingBase.__init__(self, client_config_map)
        
        # Initialize the PerpetualDerivativePyBase
        PerpetualDerivativePyBase.__init__(
            self,
            client_config_map=client_config_map,
            trading_pairs=trading_pairs, 
            trading_required=trading_required
        )
        
        # Replace the budget checker with our cross-collateral budget checker
        self._budget_checker = CrossCollateralBudgetChecker(self)
    
    def start_network(self):
        """
        Starts the network for both the perpetual derivative and cross-collateral functionality.
        """
        return PerpetualDerivativePyBase.start_network(self)
    
    def _stop_network(self):
        """
        Stops the network for both the perpetual derivative and cross-collateral functionality.
        """
        PerpetualDerivativePyBase._stop_network(self)
    
    def supported_position_modes(self) -> List[PositionMode]:
        """
        Returns the supported position modes.
        Delegates to the PerpetualDerivativePyBase implementation.
        """
        return PerpetualDerivativePyBase.supported_position_modes(self)
    
    def get_leverage(self, trading_pair: str) -> int:
        """
        Gets the leverage for a specific trading pair.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param trading_pair: The trading pair to get leverage for
        :return: The current leverage value
        """
        return CrossCollateralTradingBase.get_leverage(self, trading_pair)
    
    def set_leverage(self, trading_pair: str, leverage: int = 1):
        """
        Sets the leverage for a specific trading pair.
        Delegates to the PerpetualDerivativePyBase implementation which 
        will call _execute_set_leverage with proper exchange interaction.
        
        :param trading_pair: The trading pair to set leverage for
        :param leverage: The leverage value to set
        """
        PerpetualDerivativePyBase.set_leverage(self, trading_pair, leverage)
    
    async def _execute_set_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Execute the setting of leverage on the exchange.
        Delegates to the PerpetualDerivativePyBase implementation but also
        updates our CrossCollateralTradingBase leverage tracking.
        
        :param trading_pair: The trading pair to set leverage for
        :param leverage: The leverage value to set
        :return: A tuple of success flag and error message if any
        """
        success, msg = await super()._execute_set_leverage(trading_pair, leverage)
        if success:
            # Update our cross-collateral leverage tracking
            CrossCollateralTradingBase.set_leverage(self, trading_pair, leverage)
        return success, msg
    
    def set_position_mode(self, position_mode: PositionMode):
        """
        Sets the position mode for the exchange.
        Delegates to the PerpetualDerivativePyBase implementation.
        
        :param position_mode: The position mode to set (HEDGE or ONEWAY)
        """
        PerpetualDerivativePyBase.set_position_mode(self, position_mode)
    
    async def _execute_set_position_mode(self, mode: PositionMode) -> Tuple[bool, str]:
        """
        Execute the setting of position mode on the exchange.
        Delegates to the PerpetualDerivativePyBase implementation but also
        updates our CrossCollateralTradingBase position mode tracking.
        
        :param mode: The position mode to set
        :return: A tuple of success flag and error message if any
        """
        success, msg = await PerpetualDerivativePyBase._execute_set_position_mode(self, mode)
        if success:
            # Update our cross-collateral position mode tracking
            CrossCollateralTradingBase.set_position_mode(self, mode)
        return success, msg
    
    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """
        Gets the token used for collateral when buying.
        Delegates to the PerpetualDerivativePyBase implementation.
        
        Override this in the specific connector implementation to enable
        cross-collateral functionality for buy orders.
        
        :param trading_pair: The trading pair
        :return: The collateral token symbol
        """
        return PerpetualDerivativePyBase.get_buy_collateral_token(self, trading_pair)
    
    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """
        Gets the token used for collateral when selling.
        Delegates to the PerpetualDerivativePyBase implementation.
        
        Override this in the specific connector implementation to enable
        cross-collateral functionality for sell orders.
        
        :param trading_pair: The trading pair
        :return: The collateral token symbol
        """
        return PerpetualDerivativePyBase.get_sell_collateral_token(self, trading_pair)
    
    def get_valid_collateral_tokens(self, trading_pair: str) -> List[str]:
        """
        Gets the list of valid collateral tokens for a specific trading pair.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param trading_pair: The trading pair to get collateral tokens for
        :return: List of valid collateral token symbols
        """
        return CrossCollateralTradingBase.get_valid_collateral_tokens(self, trading_pair)
    
    def set_valid_collateral_tokens(self, trading_pair: str, collateral_tokens: List[str]):
        """
        Sets the list of valid collateral tokens for a specific trading pair.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param trading_pair: The trading pair to set collateral tokens for
        :param collateral_tokens: List of token symbols that can be used as collateral
        """
        CrossCollateralTradingBase.set_valid_collateral_tokens(self, trading_pair, collateral_tokens)
    
    def add_collateral_asset(self, asset: str):
        """
        Adds an asset to the set of valid collateral assets.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param asset: The asset to add as valid collateral
        """
        CrossCollateralTradingBase.add_collateral_asset(self, asset)
    
    def remove_collateral_asset(self, asset: str):
        """
        Removes an asset from the set of valid collateral assets.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param asset: The asset to remove from valid collateral
        """
        CrossCollateralTradingBase.remove_collateral_asset(self, asset)
    
    def get_collateral_assets(self) -> Set[str]:
        """
        Returns the set of all assets that can be used as collateral.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :return: Set of collateral asset symbols
        """
        return CrossCollateralTradingBase.get_collateral_assets(self)
    
    def get_available_collateral(self, asset: str) -> Decimal:
        """
        Calculates the available amount of a collateral asset that is not currently allocated.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param asset: The collateral asset to check
        :return: Available amount of the asset for use as collateral
        """
        return CrossCollateralTradingBase.get_available_collateral(self, asset)
    
    def get_all_available_collateral_balances(self) -> Dict[str, Decimal]:
        """
        Returns a dictionary mapping each collateral asset to its available balance.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :return: Dictionary of available collateral balances
        """
        return CrossCollateralTradingBase.get_all_available_collateral_balances(self)
    
    def allocate_collateral(self, trading_pair: str, asset: str, amount: Decimal):
        """
        Allocates a specific amount of a collateral asset to a trading pair.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param trading_pair: The trading pair to allocate collateral for
        :param asset: The collateral asset to allocate
        :param amount: The amount to allocate
        """
        CrossCollateralTradingBase.allocate_collateral(self, trading_pair, asset, amount)
    
    def free_collateral(self, trading_pair: str, asset: str, amount: Decimal):
        """
        Frees a specific amount of collateral that was previously allocated.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param trading_pair: The trading pair to free collateral from
        :param asset: The collateral asset to free
        :param amount: The amount to free
        """
        CrossCollateralTradingBase.free_collateral(self, trading_pair, asset, amount)
    
    def get_allocated_collateral(self, trading_pair: Optional[str] = None) -> Dict:
        """
        Gets the currently allocated collateral.
        Delegates to the CrossCollateralTradingBase implementation.
        
        :param trading_pair: Optional trading pair to filter by
        :return: Dictionary of allocated collateral
        """
        return CrossCollateralTradingBase.get_allocated_collateral(self, trading_pair)
    
    def get_total_collateral_value(self, quote_currency: str) -> Decimal:
        """
        Calculates the total value of all available collateral in terms of the specified quote currency.
        This must be implemented by the concrete connector class.
        
        :param quote_currency: The currency to express the total value in
        :return: Total collateral value in the specified currency
        """
        raise NotImplementedError("get_total_collateral_value must be implemented by the concrete connector class.") 