import typing
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.connector.budget_checker import BudgetChecker
from hummingbot.connector.cross_collateral_trading_base import CrossCollateralTradingBase
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate

if typing.TYPE_CHECKING:  # avoid circular import problems
    from hummingbot.connector.exchange_base import ExchangeBase


class CrossCollateralOrderCandidate(OrderCandidate):
    """
    Extends OrderCandidate with cross-collateral capabilities.
    """
    def __init__(self, *args, **kwargs):
        # Extract cross-collateral specific parameters
        self.custom_collateral_tokens = kwargs.pop("custom_collateral_tokens", None)
        super().__init__(*args, **kwargs)
        self.collateral_token_weights = {}  # Dictionary to track which tokens to use as collateral and in what proportion
    
    def set_collateral_token_weights(self, weights: Dict[str, Decimal]):
        """
        Sets the weights for each collateral token to be used
        Sum of weights should equal 1.0
        
        :param weights: Dictionary mapping token symbols to weights (0.0 to 1.0)
        """
        # Validate weights sum to 1.0
        total_weight = sum(weights.values())
        if not Decimal("0.99") <= total_weight <= Decimal("1.01"):
            raise ValueError(f"Collateral token weights must sum to 1.0, got {total_weight}")
        
        self.collateral_token_weights = weights


class CrossCollateralPerpetualOrderCandidate(CrossCollateralOrderCandidate, PerpetualOrderCandidate):
    """
    Order candidate that combines cross-collateral capabilities with perpetual trading
    """
    pass


class CrossCollateralBudgetChecker(BudgetChecker):
    """
    Budget checker for cross-collateral trading.
    Extends the standard BudgetChecker to consider multiple collateral assets.
    """
    def __init__(self, exchange: "ExchangeBase"):
        """
        Initialize a budget checker for cross-collateral trading.
        
        :param exchange: The exchange connector (must implement CrossCollateralTradingBase)
        """
        super().__init__(exchange)
        self._validate_cross_collateral_connector()
        # Keep track of locked collateral per asset
        self._locked_collateral_by_asset = {}
    
    def _validate_cross_collateral_connector(self):
        """
        Validates that the exchange connector supports cross-collateral trading
        """
        if not isinstance(self._exchange, CrossCollateralTradingBase):
            raise TypeError(
                f"{self.__class__} must be passed an exchange implementing the CrossCollateralTradingBase interface."
            )
    
    def reset_locked_collateral(self):
        """
        Resets all locked collateral.
        """
        super().reset_locked_collateral()
        self._locked_collateral_by_asset = {}
    
    def populate_collateral_entries(self, order_candidate: OrderCandidate) -> OrderCandidate:
        """
        Populates the collateral entries based on the available collateral assets.
        
        For cross-collateral trading, this method considers all available collateral assets
        instead of just the base and quote assets of the trading pair.
        
        :param order_candidate: The order candidate to populate
        :return: The populated order candidate
        """
        # If it's a cross-collateral order candidate, use custom logic
        if isinstance(order_candidate, CrossCollateralOrderCandidate) and order_candidate.custom_collateral_tokens:
            # Create a copy to avoid modifying the original
            order_candidate = self._populate_cross_collateral_entries(order_candidate)
        else:
            # Use standard population logic for regular order candidates
            order_candidate = super().populate_collateral_entries(order_candidate)
        
        return order_candidate
    
    def _populate_cross_collateral_entries(self, order_candidate: CrossCollateralOrderCandidate) -> CrossCollateralOrderCandidate:
        """
        Populates collateral entries for cross-collateral order candidates.
        
        :param order_candidate: The cross-collateral order candidate
        :return: The populated order candidate
        """
        exchange = self._exchange
        if not isinstance(exchange, CrossCollateralTradingBase):
            # If not a cross-collateral exchange, fall back to standard logic
            return super().populate_collateral_entries(order_candidate)
        
        # Make a copy to avoid modifying the original
        order_candidate = order_candidate.copy()
        
        # Get the valid collateral tokens for this trading pair
        trading_pair = order_candidate.trading_pair
        
        # Use custom collateral tokens if specified, otherwise use valid collateral tokens from exchange
        collateral_tokens = (order_candidate.custom_collateral_tokens or 
                             exchange.get_valid_collateral_tokens(trading_pair))
        
        if not collateral_tokens:
            # If no valid collateral tokens, fall back to standard logic
            return super().populate_collateral_entries(order_candidate)
        
        # If we have collateral token weights, use them to allocate collateral
        # Otherwise, use the first available collateral token
        collateral_amount = order_candidate.amount
        if hasattr(order_candidate, 'collateral_token_weights') and order_candidate.collateral_token_weights:
            self._populate_weighted_collateral(order_candidate, collateral_tokens, collateral_amount)
        else:
            # Find first available collateral token with sufficient balance
            for token in collateral_tokens:
                available_balance = exchange.get_available_collateral(token)
                if available_balance >= collateral_amount:
                    order_candidate.order_collateral = (token, collateral_amount)
                    break
            
            # If no single token has sufficient balance, use a combination
            if order_candidate.order_collateral is None:
                self._populate_multi_collateral(order_candidate, collateral_tokens, collateral_amount)
        
        return order_candidate
    
    def _populate_weighted_collateral(self, 
                                     order_candidate: CrossCollateralOrderCandidate, 
                                     collateral_tokens: List[str], 
                                     collateral_amount: Decimal):
        """
        Populates collateral entries based on specified weights
        
        :param order_candidate: The order candidate
        :param collateral_tokens: List of valid collateral tokens
        :param collateral_amount: The total collateral amount needed
        """
        # Create a list to track individual collateral entries
        collateral_entries = []
        
        # Filter out tokens that aren't in the valid collateral list
        weighted_tokens = {token: weight for token, weight in 
                          order_candidate.collateral_token_weights.items() 
                          if token in collateral_tokens}
        
        # If no valid weighted tokens, fall back to multi-collateral
        if not weighted_tokens:
            self._populate_multi_collateral(order_candidate, collateral_tokens, collateral_amount)
            return
        
        # Normalize weights to sum to 1.0
        total_weight = sum(weighted_tokens.values())
        normalized_weights = {token: weight / total_weight 
                             for token, weight in weighted_tokens.items()}
        
        # Allocate collateral based on weights
        for token, weight in normalized_weights.items():
            token_amount = collateral_amount * weight
            collateral_entries.append((token, token_amount))
        
        # Set the collateral entries
        if collateral_entries:
            # Use the first entry as the main order collateral
            order_candidate.order_collateral = collateral_entries[0]
            # Use the rest as additional collateral
            order_candidate.additional_collaterals = collateral_entries[1:]
    
    def _populate_multi_collateral(self, 
                                  order_candidate: CrossCollateralOrderCandidate, 
                                  collateral_tokens: List[str], 
                                  collateral_amount: Decimal):
        """
        Populates collateral entries using multiple tokens when no single token has sufficient balance
        
        :param order_candidate: The order candidate
        :param collateral_tokens: List of valid collateral tokens
        :param collateral_amount: The total collateral amount needed
        """
        exchange = self._exchange
        collateral_entries = []
        remaining_amount = collateral_amount
        
        # Sort tokens by available balance (descending)
        sorted_tokens = sorted(
            collateral_tokens,
            key=lambda token: exchange.get_available_collateral(token),
            reverse=True
        )
        
        # Allocate collateral from each token until we've covered the total amount
        for token in sorted_tokens:
            if remaining_amount <= Decimal("0"):
                break
                
            available_balance = exchange.get_available_collateral(token)
            allocation = min(available_balance, remaining_amount)
            
            if allocation > Decimal("0"):
                collateral_entries.append((token, allocation))
                remaining_amount -= allocation
        
        # Set the collateral entries
        if collateral_entries:
            # Use the first entry as the main order collateral
            order_candidate.order_collateral = collateral_entries[0]
            # Use the rest as additional collateral
            order_candidate.additional_collaterals = collateral_entries[1:]
    
    def _get_available_balances(self, order_candidate: OrderCandidate) -> Dict[str, Decimal]:
        """
        Get available balances for collateral checking, including cross-collateral assets.
        
        :param order_candidate: The order candidate
        :return: Dictionary mapping token symbols to available balances
        """
        available_balances = {}
        balance_fn = (
            self._exchange.get_available_balance
            if not order_candidate.from_total_balances
            else self._exchange.get_balance
        )

        # Get all possible collateral tokens if it's a cross-collateral order
        collateral_tokens = []
        if isinstance(order_candidate, CrossCollateralOrderCandidate) and hasattr(self._exchange, "get_collateral_assets"):
            exchange = self._exchange
            if isinstance(exchange, CrossCollateralTradingBase):
                collateral_tokens = list(exchange.get_collateral_assets())
        
        # Add the standard collateral tokens
        if order_candidate.order_collateral is not None:
            token, _ = order_candidate.order_collateral
            collateral_tokens.append(token)
        
        if order_candidate.percent_fee_collateral is not None:
            token, _ = order_candidate.percent_fee_collateral
            collateral_tokens.append(token)
            
        for entry in order_candidate.fixed_fee_collaterals:
            token, _ = entry
            collateral_tokens.append(token)
            
        # Add any additional collaterals if they exist
        if hasattr(order_candidate, "additional_collaterals"):
            for token, _ in getattr(order_candidate, "additional_collaterals", []):
                collateral_tokens.append(token)
        
        # Remove duplicates
        collateral_tokens = list(set(collateral_tokens))
        
        # Get the available balance for each token
        for token in collateral_tokens:
            locked_collateral = self._locked_collateral_by_asset.get(token, Decimal("0"))
            available_balances[token] = balance_fn(token) - locked_collateral
        
        return available_balances
    
    def _lock_available_collateral(self, order_candidate: OrderCandidate):
        """
        Locks collateral for the order candidate.
        
        :param order_candidate: The order candidate
        """
        # For perpetual order candidates that are closing positions, don't lock collateral
        if isinstance(order_candidate, PerpetualOrderCandidate) and order_candidate.position_close:
            return
            
        # Lock the main order collateral
        if order_candidate.order_collateral is not None:
            token, amount = order_candidate.order_collateral
            self._lock_collateral_for_token(token, amount)
            
        # Lock percent fee collateral
        if order_candidate.percent_fee_collateral is not None:
            token, amount = order_candidate.percent_fee_collateral
            self._lock_collateral_for_token(token, amount)
            
        # Lock fixed fee collaterals
        for token, amount in order_candidate.fixed_fee_collaterals:
            self._lock_collateral_for_token(token, amount)
            
        # Lock additional collaterals if they exist
        if hasattr(order_candidate, "additional_collaterals"):
            for token, amount in getattr(order_candidate, "additional_collaterals", []):
                self._lock_collateral_for_token(token, amount)
    
    def _lock_collateral_for_token(self, token: str, amount: Decimal):
        """
        Locks a specific amount of collateral for a token.
        
        :param token: The token to lock collateral for
        :param amount: The amount to lock
        """
        # Update the standard locked collateral
        self._locked_collateral[token] = self._locked_collateral.get(token, Decimal("0")) + amount
        
        # Update the per-asset locked collateral
        self._locked_collateral_by_asset[token] = self._locked_collateral_by_asset.get(token, Decimal("0")) + amount 