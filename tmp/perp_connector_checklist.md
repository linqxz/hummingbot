# Perpetual Connector v2.1 Checklist

## Prerequisites
- Hummingbot source version installed
- Understanding of exchange API documentation
- Development environment set up

## Build Process

### 1. Constants (`constants.py`)
- [x] Add constants:
  - DEFAULT_DOMAIN (check if multiple domains needed)
    - Used for generating path URL in web_utils
  - REST_URLS 
    - Can be dictionary (like ByBit) or f-string (like Binance)
    - Uses DEFAULT_DOMAIN for URL selection
  - SERVER_TIME_PATH_URL (if timestamp sync needed)

### 2. Web Utils (`connector_name_web_utils.py`)
#### Setup
- [x] Configure linear perpetual or remove logic
- [x] Code the function to create the rest url, check if you need public and private url or just one of them.
- [x] If you need Time Sync:
  - Check that you have the function `build_api_factory_without_time_synchronizer_pre_processor`.
  - Check if you need changes in the function `get_current_server_time` (maybe you need to find the server time in the response).
  - Code the `build_api_factory` with the time synchronizer as a rest_pre_processor.
- [x] If you don’t need Time Sync:
  - Delete the files related with the Time Sync.
  - Code the `build_api_factory` function without that.


- [x] build_api_factory()
- [x] build_api_factory_without_time_synchronizer_pre_processor()
- [x] create_throttler()

- [x] get_current_server_time()

#### WS Implementation
- [x] Extract WebSocket topics and payload handlers
    - [x] wss_public_url()
    - [x] wss_private_url()
#### REST Implementation
- [x] Create REST URL function
    - [x] get_rest_url_for_endpoint()
  - [x] Implement rate limits handling for each type of request
    - [x] build_rate_limits()
    - [x] get_rest_api_limit_id_for_endpoint()
    - [x] get_pair_specific_limit_id()
  - [x] Implement associated request with endpoints.
  - [x] rest_authenticate


### 3. Utils (`connector_name_utils.py`)

- [x] Configure DEFAULT_FEES
  - Mainnet: 0.02% maker, 0.05% taker
  - Testnet: Same as mainnet
  - Properly tested
- [x] Set up domain handling
  - Mainnet and testnet domains configured
  - Domain-specific config maps
  - Domain-specific example pairs
  - Domain-specific fees
  - All properly tested
- [x] Configure linear trading
  - Not needed for Kraken Perpetual (uses inverse perpetuals)
  - Trading pair conversion handles all formats (PI_, PF_, PL_ prefixes)
  - Properly tested
- [x] Other necessary utility functions
  - [x] is_exchange_information_valid: Validates exchange responses
  - [x] get_next_funding_timestamp: Calculates 8-hour funding intervals
  - [x] get_client_order_id: Generates unique order IDs
  - [x] Trading pair conversion:
    - convert_from_exchange_trading_pair
    - convert_to_exchange_trading_pair
  - [x] Asset conversion maps:
    - KRAKEN_TO_HB_ASSETS
    - HB_TO_KRAKEN_ASSETS
  - [x] Config handling:
    - Secure API key storage
    - Both mainnet and testnet configs
    - All properly tested

### 4. Order Book Data Source

- [x] Replace HEARTBEAT_TIME_INTERVAL with appropriate value
- [x] Implement test_connector_name_api_order_book_data_source.py

#### Methods development

##### REST

###### orderbook
- [x] Implement test_get_new_order_book_successful
- [x] Implement _order_book_snapshot
- [x] Implement _request_order_book_snapshot
- [x] Implement test_get_new_order_book_raises_exception

###### funding_info
- [x] Implement test_get_funding_info
- [x] Implement get_funding_info

##### WEBSOCKET

###### listen_for_subscriptions
- [x] Implement test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs_and_funding_info
- [x] Implement _connected_websocket_assistant
- [x] Implement _subscribe_channels
- [x] Implement _process_websocket_messages
- [x] Implement _channel_originating_message
- [x] Implement test_listen_for_subscriptions_raises_cancel_exception
- [x] Implement test_listen_for_subscriptions_logs_exception_details
- [x] Implement test_subscribe_channels_raises_cancel_exception
- [x] Implement test_subscribe_channels_raises_exception_and_logs_error

###### listen_for_trades
- [x] Implement test_listen_for_trades_successful
- [x] Implement _parse_trade_message
- [x] Implement test_listen_for_trades_cancelled_when_listening
- [x] Implement test_listen_for_trades_logs_exception

###### listen_for_order_book_diffs
- [x] Implement test_listen_for_order_book_diffs_successful
- [x] Implement listen_for_order_book_diffs
- [x] Implement _parse_order_book_diff_message
- [x] Implement test_listen_for_order_book_diffs_cancelled
- [x] Implement test_listen_for_order_book_diffs_logs_exception

###### listen_for_order_book_snapshots
- [x] Implement test_listen_for_order_book_snapshots_cancelled_when_fetching_snapshot
- [x] Implement listen_for_order_book_snapshots
- [x] Implement _parse_order_book_snapshot_message
- [x] Implement test_listen_for_order_book_snapshots_log_exception
- [x] Implement test_listen_for_order_book_snapshots_successful

###### listen_for_funding_info
- [x] Implement test_listen_for_funding_info_cancelled_when_listening
- [x] Implement _parse_funding_info_message
- [x] Implement test_listen_for_funding_info_logs_exception
- [x] Implement test_listen_for_funding_info_successful



### 5. Authentication

#### REST Implementation
- [x] test_rest_authenticate
- [x] rest_authenticate
- [x] add_auth_to_params
- [x] header_for_authentication
- [x] _generate_signature

#### WebSocket Implementation
- [x] ws_authenticate

### 6. User Stream Data Source

#### Listen Key Management (if needed)
- [x] test_get_listen_key_log_exception
- [x] _get_listen_key
- [x] test_get_listen_key_successful
- [x] test_ping_listen_key_log_exception
- [x] _ping_listen_key
- [x] test_ping_listen_key_successful
- [x] test_manage_listen_key_task_loop_keep_alive_failed
- [x] _manage_listen_key_task_loop
- [x] test_manage_listen_key_task_loop_keep_alive_successful

#### WebSocket Implementation
- [x] test_listen_for_user_stream_get_listen_key_successful_with_user_update_event
- [x] _connected_websocket_assistant
- [x] _subscribe_channels
- [x] _get_ws_assistant
- [x] _on_user_stream_interruption
- [x] Error handling:
- [x] test_listen_for_user_stream_does_not_queue_empty_payload
- [x] test_listen_for_user_stream_connection_failed
- [x] test_listen_for_user_stream_iter_message_throws_exception

### 7. Exchange Implementation

#### Required Properties
- [ ] authenticator
- [ ] name
- [ ] rate_limits_rules
- [ ] domain 
- [ ] client_order_id_max_length
- [ ] client_order_id_prefix
- [ ] trading_rules_request_path
- [ ] trading_pairs_request_path
- [ ] check_network_request_path
- [ ] trading_pairs
- [ ] supported_order_types
- [ ] supported_position_modes

#### Core Methods
- [ ] _create_web_assistants_factory
- [ ] _create_order_book_data_source
- [ ] _create_user_stream_data_source
- [ ] _get_fee
- [ ] _place_order
- [ ] _place_cancel
- [ ] _format_trading_rules
- [ ] _update_trading_fees
- [ ] _update_balances
- [ ] _initialize_trading_pair_symbols
- [ ] _get_last_traded_price

#### Perpetual-Specific Methods
- [ ] funding_fee_poll_interval
- [ ] get_buy_collateral_token
- [ ] get_sell_collateral_token
- [ ] _update_positions
- [ ] _set_trading_pair_leverage
- [ ] _fetch_last_fee_payment

#### Time Synchronization
- [ ] test_update_time_synchronizer_successfully
- [ ] _update_time_synchronizer
- [ ] test_update_time_synchronizer_failure_is_logged
- [ ] test_update_time_synchronizer_raises_cancelled_error
- [ ] test_time_synchronizer_related_request_error_detection

#### Order Management
- [ ] test_update_order_fills_from_trades_triggers_filled_event
- [ ] _update_order_fills_from_trades
- [ ] test_update_order_fills_request_parameters
- [ ] test_update_order_fills_from_trades_with_repeated_fill_triggers_only_one_event
- [ ] test_update_order_status_when_failed
- [ ] _update_order_status

#### Position Management
- [ ] test_set_position_mode_failure
- [ ] test_set_position_mode_success

#### Funding Info
- [ ] test_listen_for_funding_info_update_initializes_funding_info
- [ ] test_listen_for_funding_info_update_updates_funding_info
- [ ] test_init_funding_info
- [ ] test_update_funding_info_polling_loop_success
- [ ] test_update_funding_info_polling_loop_raise_exception

### Generic Test Class Methods
#### Mock Responses
- [ ] all_symbols_request_mock_response
- [ ] latest_prices_request_mock_response
- [ ] network_status_request_mock_response
- [ ] trading_rules_request_mock_response
- [ ] order_creation_request_mock_response
- [ ] balance_request_mock_response
- [ ] position_event_websocket_update
- [ ] funding_info_event_websocket_update

#### Validation Methods
- [ ] validate_auth_credentials_present
- [ ] validate_order_creation_request
- [ ] validate_order_cancelation_request
- [ ] validate_order_status_request
- [ ] validate_trades_request

#### Configuration Methods
- [ ] configure_successful_cancelation_response
- [ ] configure_erroneous_cancelation_response
- [ ] configure_successful_set_leverage
- [ ] configure_failed_set_leverage
- [ ] configure_successful_set_position_mode
- [ ] configure_failed_set_position_mode

## Final Steps
- [ ] Add to conf_global_TEMPLATE.yml:
  - connector_name_api_key
  - connector_name_api_secret
- [ ] Complete all unit tests
- [ ] Verify against exchange documentation
- [ ] Test with real API credentials