import pandas as pd
from typing import Dict
from hummingbot.core.utils.trading_pair_fetcher import TradingPairFetcher
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
import asyncio


class IdentifyOpportunity(ScriptStrategyBase):
    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.connectors = connectors
        self.trading_fetcher = TradingPairFetcher()
        self.trading_pairs = None

    @property
    def tradable_assets(self):
        if self.trading_pairs is None:
            loop = asyncio.get_event_loop()
            self.trading_pairs = loop.run_until_complete(
                self.trading_fetcher.fetch_trading_pairs())
        return self.trading_pairs

    def collect_tradable_assets(self):
        kraken_connector = self.connectors.get('kraken')
        columns = ['symbol', 'baseAsset', 'quoteAsset',
                   'permissions', 'ocoAllowed', 'isMarginTradingAllowed',
                   'isSpotTradingAllowed', 'quoteMaxPrice', 'quoteMinPrice', 'baseMaxPrice', 'baseMinPrice']

        tradable_assets = []
        if kraken_connector:
            if kraken_connector.ready:
                tradable_assets = self.trading_pairs.get('kraken')

        for i in tradable_assets:
            if i['status'] == 'TRADING':
                tradable_assets.append({col: i.get(col) for col in columns})
        tradable_assets_df = pd.DataFrame(
            tradable_assets, columns=columns)
        return tradable_assets_df[tradable_assets_df['quoteAsset'] == 'USD']

    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. This function is called
        when status command is issued. Override this function to create custom status display output.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        # balance_df = self.get_balance_df()
        # lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])
        market_status_df = self.get_market_status_df_with_depth()

        market_status_df_volume_sort = market_status_df.sort_values('Total Volume', ascending=False).head(
            self.top_nrows)

        lines.extend(["", "  Top tickers with high Volume:"] + ["    " + line for line in
                                                                market_status_df_volume_sort.to_string(
                                                                    index=False).split("\n")])

        market_status_df_ask_bid_sort = market_status_df.sort_values('ask_bid_pc_basis', ascending=False).head(
            self.top_nrows)

        lines.extend(["", "  Top tickers with high ask-bid price spread percentage basis:"] + ["    " + line for line in
                                                                                               market_status_df_ask_bid_sort.to_string(
                                                                                                   index=False).split(
                                                                                                   "\n")])

        market_status_df_mid_last_sort = market_status_df.sort_values('mid_last_pc_basis', ascending=False).head(
            self.top_nrows)

        lines.extend(
            ["", "  Top tickers with high mid-last price spread percentage basis:"] + ["    " + line for line in
                                                                                       market_status_df_mid_last_sort.to_string(
                                                                                           index=False).split("\n")])

        market_status_df_ask_bid_volume_sort_desc = market_status_df.sort_values('ask_bid_volume_pc_basis',
                                                                                 ascending=False).head(self.top_nrows)

        lines.extend(
            ["", "  Top tickers with high ask-bid volume spread percentage basis:"] + ["    " + line for line in
                                                                                       market_status_df_ask_bid_volume_sort_desc.to_string(
                                                                                           index=False).split("\n")])

        market_status_df_ask_bid_volume_sort_asc = market_status_df.sort_values('ask_bid_volume_pc_basis',
                                                                                ascending=True).head(self.top_nrows)

        lines.extend(["", "  Top tickers with low ask-bid volume spread percentage basis:"] + ["    " + line for line in
                                                                                               market_status_df_ask_bid_volume_sort_asc.to_string(
                                                                                                   index=False).split(
                                                                                                   "\n")])

        # warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)

    def get_market_status_df_with_depth(self):
        market_status_df = self.market_status_data_frame(self.get_market_trading_pair_tuples())
        market_status_df["Exchange"] = market_status_df.apply(
            lambda x: x["Exchange"].strip("PaperTrade") + "paper_trade", axis=1)
        market_status_df["Volume (+1%)"] = market_status_df.apply(
            lambda x: self.get_volume_for_percentage_from_mid_price(x, 0.01), axis=1)
        market_status_df["Volume (-1%)"] = market_status_df.apply(
            lambda x: self.get_volume_for_percentage_from_mid_price(x, -0.01), axis=1)
        market_status_df["Total Volume"] = market_status_df["Volume (+1%)"] + market_status_df["Volume (-1%)"]
        market_status_df["ask_bid_pc_basis"] = (market_status_df['Best Ask Price'] - market_status_df[
            'Best Bid Price']) * 10000 / market_status_df['Best Bid Price']
        market_status_df["Last Trade Price"] = market_status_df.apply(lambda x: float(self.get_lasttrade_price(x)),
                                                                      axis=1)
        market_status_df["mid_last_pc_basis"] = (round(market_status_df["Mid Price"], 3) - round(
            market_status_df["Last Trade Price"], 3)) * 10000 / round(market_status_df["Last Trade Price"], 3)
        market_status_df["ask_bid_volume_pc_basis"] = (market_status_df["Volume (+1%)"] - market_status_df[
            "Volume (-1%)"]) * 10000 / market_status_df["Volume (-1%)"]
        return market_status_df

    def get_volume_for_percentage_from_mid_price(self, row, percentage):
        price = row["Mid Price"] * (1 + percentage)
        is_buy = percentage > 0
        result = self.connectors[row["Exchange"]].get_quote_volume_for_base_amount(row["Market"], is_buy, price)
        return round(result.result_volume)

    def get_lasttrade_price(self, row):

        return round(self.connectors[row["Exchange"]].get_price_by_type(row["Market"], PriceType.LastTrade), 3)