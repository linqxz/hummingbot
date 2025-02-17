# Kraken Perpetual WebSocket Feeds

# Websocket Feed: account_log

Sample Sent Message

#### Event Types
subscribe / unsubscribe

#### Required Permissions
Any

---
# Websocket Feed: balances

Public API for Mars including primary trade endpoints.

### Event Types
subscribe / unsubscribe

#### Required Permissions
Any

#### Sample Subscription

{
  "event": "subscribe",
  "feed": "balances",
  "api_key": "drUfSSmBbDpcIpwpqK0OBTcGLdAYZJU+NlPIsHaKspu/8feT2YSKl+Jw",
  "original_challenge": "c094497e-9b5f-40da-a122-3751c39b107f",
  "signed_challenge": "Ds0wtsHaXlAby/Vnoil59Q+yJIrJwZGUlgECD3+qEvFcTFfacJi2LrSRzAoqwBAeZk4pGXSmyyIW0uDymZ3olw=="
}


#### Sample Message

{
  "feed": "balances_snapshot",
  "account": "4a012c31-df95-484a-9473-d51e4a0c4ae7",
  "holding": {
    "USDT": 4997.5012493753,
    "XBT": 0.1285407184,
    "ETH": 1.8714395862,
    "LTC": 47.6462740614,
    "GBP": 3733.488646461,
    "USDC": 5001.00020004,
    "USD": 5000.0,
    "BCH": 16.8924625832,
    "EUR": 4459.070194683,
    "XRP": 7065.5399485629
  },
  "futures": {
    "F-ETH:EUR": {
      "name": "F-ETH:EUR",
      "pair": "ETH/EUR",
      "unit": "EUR",
      "portfolio_value": 0.0,
      "balance": 0.0,
      "maintenance_margin": 0.0,
      "initial_margin": 0.0,
      "available": 0.0,
      "unrealized_funding": 0.0,
      "pnl": 0.0
    },
    "F-XBT:USD": {
      "name": "F-XBT:USD",
      "pair": "XBT/USD",
      "unit": "XBT",
      "portfolio_value": 0.0,
      "balance": 0.0,
      "maintenance_margin": 0.0,
      "initial_margin": 0.0,
      "available": 0.0,
      "unrealized_funding": 0.0,
      "pnl": 0.0
    },
  },
  "flex_futures": {
    "currencies": {
      "USDT": { "quantity": 0.0, "value": 0.0, "collateral_value": 0.0, "available": 0.0, "haircut": 0.0, "conversion_spread": 0.0 },
      "GBP": { "quantity": 0.0, "value": 0.0, "collateral_value": 0.0, "available": 0.0, "haircut": 0.0, "conversion_spread": 0.0  },
      "USDC": { "quantity": 0.0, "value": 0.0, "collateral_value": 0.0, "available": 0.0, "haircut": 0.0, "conversion_spread": 0.0  },
      "XBT": { "quantity": 0.0, "value": 0.0, "collateral_value": 0.0, "available": 0.0, "haircut": 0.0, "conversion_spread": 0.0  },
      "USD": { "quantity": 0.0, "value": 0.0, "collateral_value": 0.0, "available": 0.0, "haircut": 0.0, "conversion_spread": 0.0  },
      "EUR": { "quantity": 0.0, "value": 0.0, "collateral_value": 0.0, "available": 0.0, "haircut": 0.0, "conversion_spread": 0.0  },
      "ETH": { "quantity": 0.0, "value": 0.0, "collateral_value": 0.0, "available": 0.0, "haircut": 0.0, "conversion_spread": 0.0  }
    },
    "balance_value":0.0,
    "portfolio_value":0.0,
    "collateral_value":0.0,
    "initial_margin":0.0,
    "initial_margin_without_orders":0.0,
    "maintenance_margin":0.0,
    "pnl":0.0,
    "unrealized_funding":0.0,
    "total_unrealized":0.0,
    "total_unrealized_as_margin":0.0,
    "margin_equity":0.0,
    "available_margin":0.0,
    "isolated": {
      "PF_ETHUSD": {
        "initial_margin": 0.0,
        "initial_margin_without_orders": 0.0,
        "maintenance_margin": 0.0,
        "pnl": 0.0,
        "unrealized_funding": 0.0,
        "total_unrealized": 0.0,
        "total_unrealized_as_margin": 0.0
      }
    },
    "cross": {
      "balance_value":9963.66,
      "portfolio_value":9963.66,
      "collateral_value":9963.66,
      "initial_margin":0.0,
      "initial_margin_without_orders":0.0,
      "maintenance_margin":0.0,
      "pnl":0.0,
      "unrealized_funding":0.0,
      "total_unrealized":0.0,
      "total_unrealized_as_margin":0.0,
      "margin_equity":9963.66,
      "available_margin":9963.66,
      "effective_leverage":0.0
    }
  },
  "timestamp":1640995200000,
  "seq":0
}


---

# Websocket Feed: book

### Event Types
subscribe / unsubscribe

### Sample Sent Message

{
  "event": "subscribe",
  "feed": "book",
  "product_ids": [
    "PI_XBTUSD",
  ]
}

### Sample Subscription Snapshot 

{
  "feed": "book_snapshot",
  "product_id": "PI_XBTUSD",
  "timestamp": 1612269825817,
  "seq": 326072249,
  "tickSize": null,
  "bids": [
    {
      "price": 34892.5,
      "qty": 6385
    },
    {
      "price": 34892,
      "qty": 10924
    },
  ],
  "asks": [
    {
      "price": 34911.5,
      "qty": 20598
    },
    {
      "price": 34912,
      "qty": 2300
    },
  ]
}

### Sample Subscription Delta 

{
  "feed": "book",
  "product_id": "PI_XBTUSD",
  "side": "sell",
  "seq": 326094134,
  "price": 34981,
  "qty": 0,
  "timestamp": 1612269953629
}


---

# Websocket Feed: fills

### Event Types
subscribe / unsubscribe

### Sample Sent Message 
{
  "event": "subscribe",
  "feed": "fills",
  "api_key": "CMl2SeSn09Tz+2tWuzPiPUjaXEQRGq6qv5UaexXuQ3SnahDQU/gO3aT+",
  "original_challenge": "226aee50-88fc-4618-a42a-34f7709570b2",
  "signed_challenge":"RE0DVOc7vS6pzcEjGWd/WJRRBWb54RkyvV+AZQSRl4+rap8Rlk64diR+
Z9DQILm7qxncswMmJyvP/2vgzqqh+g=="
}

### Sample Subscription Snapshot 

{
  "feed": "fills_snapshot",
  "account": "DemoUser",
  "fills": [
    {
      "instrument": "FI_XBTUSD_200925",
      "time": 1600256910739,
      "price": 10937.5,
      "seq": 36,
      "buy": true,
      "qty": 5000.0,
      "remaining_order_qty":0.0,
      "order_id": "9e30258b-5a98-4002-968a-5b0e149bcfbf",
      "fill_id": "cad76f07-814e-4dc6-8478-7867407b6bff",
      "fill_type": "maker",
      "fee_paid": -0.00009142857,
      "fee_currency": "BTC",
      "taker_order_type": "ioc",
      "order_type": "limit"
    },
    {
      "instrument": "PI_ETHUSD",
      "time": 1600256945531,
      "price": 364.65,
      "seq": 39,
      "buy": true,
      "qty": 5000.0,
      "remaining_order_qty":0.0,
      "order_id": "7e60b6e8-e4c2-4ce8-bbd0-ef81e18b65bb",
      "fill_id": "b1aa44b2-4f2a-4031-999c-ae1175c91580",
      "fill_type": "taker",
      "fee_paid": 0.00685588921,
      "fee_currency": "ETH",
      "taker_order_type": "market",
      "order_type": "limit"
    }

---

# Websocket Feed: heartbeat

### Event Types
subscribe / unsubscribe

### Sample Subscription

{
  "event": "subscribe",
  "feed": "heartbeat"
}

### Sample Subscription Data
{
  "feed": "heartbeat",
  "time": 1534262350627
}

---

# Websocket Feed: notifications_auth

### Event Types
subscribe / unsubscribe

---

# Websocket Feed: open_orders

### Event Types
subscribe / unsubscribe

### Sample Subscription Snapshot

{
  "feed": "open_orders_snapshot",
  "account": "e258dba9-4dd4-4da5-bfef-75beb91c098e",
  "orders": [
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275024153,
      "last_update_time": 1612275024153,
      "qty": 1000,
      "filled": 0,
      "limit_price": 34900,
      "stop_price": 13789,
      "type": "stop",
      "order_id": "723ba95f-13b7-418b-8fcf-ab7ba6620555",
      "direction": 1,
      "reduce_only": false,
      "triggerSignal": "last"
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275024153,
      "last_update_time": 1612275178153,
      "qty": 12,
      "filled": 0,
      "stop_price": 3200.1,
      "type": "trailing_stop",
      "order_id": "59302619-41d2-4f0b-941f-7e7914760ad3",
      "direction": 1,
      "reduce_only": false,
      "triggerSignal": "mark",
      "trailing_stop_options": {
        "max_deviation": 20.0,
        "unit": "percent"
      },
      "limit_price_offset": {
        "price_offset": -10.0,
        "unit": "percent"
      }
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275209430,
      "last_update_time": 1612275209430,
      "qty": 1000,
      "filled": 0,
      "limit_price": 35058,
      "stop_price": 0,
      "type": "limit",
      "order_id": "7a2f793e-26f3-4987-a938-56d296a11560",
      "direction": 1,
      "reduce_only": false
    }
  ]
}


---

# Websocket Feed: open_orders

Object fields:

#### Event Types
subscribe / unsubscribe

#### Required Permissions
Any

#### Sample Message

{
  "event": "subscribe",
  "feed": "open_orders",
  "api_key": "CMl2SeSn09Tz+2tWuzPiPUjaXEQRGq6qv5UaexXuQ3SnahDQU/gO3aT+",
  "original_challenge": "226aee50-88fc-4618-a42a-34f7709570b2",
  "signed_challenge":"RE0DVOc7vS6pzcEjGWd/WJRRBWb54RkyvV+AZQSRl4+rap8Rlk64diR+
Z9DQILm7qxncswMmJyvP/2vgzqqh+g=="
}
---

# Websocket Feed: open_orders_snapshot

Sample Subscription Snapshot Data

#### Sample Subscription

{
  "feed": "open_orders_snapshot",
  "account": "e258dba9-4dd4-4da5-bfef-75beb91c098e",
  "orders": [
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275024153,
      "last_update_time": 1612275024153,
      "qty": 1000,
      "filled": 0,
      "limit_price": 34900,
      "stop_price": 13789,
      "type": "stop",
      "order_id": "723ba95f-13b7-418b-8fcf-ab7ba6620555",
      "direction": 1,
      "reduce_only": false,
      "triggerSignal": "last"
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275024153,
      "last_update_time": 1612275178153,
      "qty": 12,
      "filled": 0,
      "stop_price": 3200.1,
      "type": "trailing_stop",
      "order_id": "59302619-41d2-4f0b-941f-7e7914760ad3",
      "direction": 1,
      "reduce_only": false,
      "triggerSignal": "mark",
      "trailing_stop_options": {
        "max_deviation": 20.0,
        "unit": "percent"
      },
      "limit_price_offset": {
        "price_offset": -10.0,
        "unit": "percent"
      }
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275209430,
      "last_update_time": 1612275209430,
      "qty": 1000,
      "filled": 0,
      "limit_price": 35058,
      "stop_price": 0,
      "type": "limit",
      "order_id": "7a2f793e-26f3-4987-a938-56d296a11560",
      "direction": 1,
      "reduce_only": false
    }
  ]
}


#### Sample Message

{
  "feed": "open_orders_snapshot",
  "account": "e258dba9-4dd4-4da5-bfef-75beb91c098e",
  "orders": [
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275024153,
      "last_update_time": 1612275024153,
      "qty": 1000,
      "filled": 0,
      "limit_price": 34900,
      "stop_price": 13789,
      "type": "stop",
      "order_id": "723ba95f-13b7-418b-8fcf-ab7ba6620555",
      "direction": 1,
      "reduce_only": false,
      "triggerSignal": "last"
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275024153,
      "last_update_time": 1612275178153,
      "qty": 12,
      "filled": 0,
      "stop_price": 3200.1,
      "type": "trailing_stop",
      "order_id": "59302619-41d2-4f0b-941f-7e7914760ad3",
      "direction": 1,
      "reduce_only": false,
      "triggerSignal": "mark",
      "trailing_stop_options": {
        "max_deviation": 20.0,
        "unit": "percent"
      },
      "limit_price_offset": {
        "price_offset": -10.0,
        "unit": "percent"
      }
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1612275209430,
      "last_update_time": 1612275209430,
      "qty": 1000,
      "filled": 0,
      "limit_price": 35058,
      "stop_price": 0,
      "type": "limit",
      "order_id": "7a2f793e-26f3-4987-a938-56d296a11560",
      "direction": 1,
      "reduce_only": false
    }
  ]
}

---

# Websocket Feed: open_orders_verbose

Sample Sent Message

#### Event Types
subscribe / unsubscribe

#### Required Permissions
Any

#### Sample Message

{
  "event": "subscribe",
  "feed": "open_orders_verbose",
  "api_key": "CMl2SeSn09Tz+2tWuzPiPUjaXEQRGq6qv5UaexXuQ3SnahDQU/gO3aT+",
  "original_challenge": "226aee50-88fc-4618-a42a-34f7709570b2",
  "signed_challenge":"RE0DVOc7vS6pzcEjGWd/WJRRBWb54RkyvV+AZQSRl4+rap8Rlk64diR+
Z9DQILm7qxncswMmJyvP/2vgzqqh+g=="
}

---

# Websocket Feed: open_orders_verbose_snapshot

Sample Subscription Snapshot Data

#### Sample Subscription Snapshot

{
  "feed": "open_orders_verbose_snapshot",
  "account": "0f9c23b8-63e2-40e4-9592-6d5aa57c12ba",
  "orders": [
    {
      "instrument": "PI_XBTUSD",
      "time": 1567428848005,
      "last_update_time": 1567428848005,
      "qty": 100.0,
      "filled": 0.0,
      "limit_price": 8500.0,
      "stop_price": 0.0,
      "type": "limit",
      "order_id": "566942c8-a3b5-4184-a451-622b09493129",
      "direction": 0,
      "reduce_only": false
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1567428874347,
      "last_update_time": 1567428874347,
      "qty": 1501.0,
      "filled": 0.0,
      "limit_price": 7200.0,
      "stop_price": 0.0,
      "type": "limit",
      "order_id": "fcbb1459-6ed2-4b3c-a58c-67c4df7412cf",
      "direction": 0,
      "reduce_only": false
    },
    {
      "instrument": "PI_XBTUSD",
      "time": 1567515137945,
      "last_update_time": 1567515137945,
      "qty": 102.0,
      "filled": 0.0,
      "limit_price": 8500.0,
      "stop_price": 0.0,
      "type": "limit",
      "order_id": "3deea5c8-0274-4d33-988c-9e5a3895ccf8",
      "direction": 0,
      "reduce_only": false
    }
  ]
}


#### Sample Delta Message

{
  "feed": "open_orders_verbose",
  "order": {
    "instrument": "PI_XBTUSD",
    "time": 1567597581495,
    "last_update_time": 1567597581495,
    "qty": 102.0,
    "filled": 0.0,
    "limit_price": 10601.0,
    "stop_price": 0.0,
    "type": "limit",
    "order_id": "fa9806c9-cba9-4661-9f31-8c5fd045a95d",
    "direction": 0,
    "reduce_only": false
  },
  "is_cancel": true,
  "reason": "post_order_failed_because_it_would_be_filled"
}

{
  "feed": "open_orders_verbose",
  "order_id": "660c6b23-8007-48c1-a7c9-4893f4572e8c",
  "is_cancel": true,
  "reason": "cancelled_by_user"
}

---

# Websocket Feed: open_positions

{
  "feed": "open_positions",
  "account": "DemoUser",
  "positions": [
    {
      "instrument":"PI_XRPUSD"  
      "balance":500.0,
      "pnl":-239.6506683474764,
      "entry_price":0.3985,
      "mark_price":0.4925844,
      "index_price":0.49756,
      "liquidation_threshold":0.0,
      "effective_leverage":0.17404676894304316,
      "return_on_equity":-2.3609636135508127,
      "initial_margin":101.5054475943615,
      "initial_margin_with_orders":101.5054475943615,
      "maintenance_margin":50.75272379718075
    },
    {
      "instrument":"PF_XBTUSD",
      "balance":0.04,
      "pnl":119.56244985549435,
      "entry_price":26911.75,
      "mark_price":29900.81124638736,
      "index_price":29900.47,
      "liquidation_threshold":9572.804662403718,
      "effective_leverage":0.31865408963748215,
      "return_on_equity":5.553450159107747,
      "unrealized_funding":0.0004114160669590132,
      "initial_margin":21.529400000000003,
      "initial_margin_with_orders":21.529400000000003,
      "maintenance_margin":10.764700000000001,
      "pnl_currency":"USD"
    } …,
  ],
  "seq":4,
  "timestamp":1687383625330

#### Event Types
subscribe / unsubscribe

#### Required Permissions
Any

---

# Websocket Feed: ticker

The API requires dates and time arguments in the ISO8601 datetime format and returns all dates and times in the same format.
The syntax of this format is <yyyy>-<mm>-<dd>T<HH>:<MM>:<SS>.<sss>Z where <yyyy> is the year, <mm> is the month, <dd> is the day, <HH> is the hour, <MM> is the  minute, <SS> is the second and <sss> is the millisecond.
When provided as an argument, <sss> is optional.
Z denotes that the datetime is in UTC.

#### Event Types
subscribe / unsubscribe

#### Sample Subscription

{
  "event": "subscribe",
  "feed": "ticker",
  "product_ids": [
    "PI_XBTUSD",
    "FI_ETHUSD_210625"
  ]
}


#### Sample Message

{
  "event": "subscribe",
  "feed": "ticker",
  "product_ids": [
    "PI_XBTUSD"
  ]
}


---

# Websocket Feed: ticker_lite

Sample Sent Message

#### Event Types
subscribe / unsubscribe

#### Sample Subscription

{
  "event": "subscribe",
  "feed": "ticker_lite",
  "product_ids": [
    "PI_XBTUSD",
    "FI_ETHUSD_210625"
  ]
}


#### Sample Message

{
  "event": "subscribe",
  "feed": "ticker_lite",
  "product_ids": [
    "PI_XBTUSD",
    "FI_ETHUSD_210625"
  ]
}


---

# Websocket Feed: trade

The HTTP API provides secure access to your Kraken Futures account to perform actions such as:

#### Event Types
subscribe / unsubscribe

#### Sample Subscription

{
  "event": "subscribed ",
  "feed": "trade",
  "product_ids": [
    "PI_XBTUSD"
  ]
}


#### Sample Message

{
  "event": "subscribed ",
  "feed": "trade",
  "product_ids": [
    "PI_XBTUSD"
  ]
}


---

# Websocket Feed: trade_snapshot

Sample Subscription Snapshot Data

#### Sample Subscription

{
  "feed": "trade_snapshot",
  "product_id": "PI_XBTUSD",
  "trades": [
    {
      "feed": "trade",
      "product_id": "PI_XBTUSD",
      "uid": "caa9c653-420b-4c24-a9f1-462a054d86f1",
      "side": "sell",
      "type": "fill",
      "seq": 655508,
      "time": 1612269657781,
      "qty": 440,
      "price": 34893
    },
    {
      "feed": "trade",
      "product_id": "PI_XBTUSD",
      "uid": "45ee9737-1877-4682-bc68-e4ef818ef88a",
      "side": "sell",
      "type": "fill",
      "seq": 655507,
      "time": 1612269656839,
      "qty": 9643,
      "price": 34891
    }
  ]
}


#### Sample Message

{
  "feed": "trade_snapshot",
  "product_id": "PI_XBTUSD",
  "trades": [
    {
      "feed": "trade",
      "product_id": "PI_XBTUSD",
      "uid": "caa9c653-420b-4c24-a9f1-462a054d86f1",
      "side": "sell",
      "type": "fill",
      "seq": 655508,
      "time": 1612269657781,
      "qty": 440,
      "price": 34893
    },
    {
      "feed": "trade",
      "product_id": "PI_XBTUSD",
      "uid": "45ee9737-1877-4682-bc68-e4ef818ef88a",
      "side": "sell",
      "type": "fill",
      "seq": 655507,
      "time": 1612269656839,
      "qty": 9643,
      "price": 34891
    }
  ]
}

#### Sample Delta Message
{
  "feed": "fills",
  "username": "DemoUser",
  "fills": [
    {
      "instrument": "PI_XBTUSD",
      "time": 1600256966528,
      "price": 364.65,
      "seq": 100,
      "buy": true,
      "qty": 5000.0,
      "remaining_order_qty":0.0,
      "order_id": "3696d19b-3226-46bd-993d-a9a7aacc8fbc",
      "cli_ord_id": "8b58d9da-fcaf-4f60-91bc-9973a3eba48d",
      "fill_id": "c14ee7cb-ae25-4601-853a-d0205e576099",
      "fill_type": "taker",
      "fee_paid": 0.00685588921,
      "fee_currency": "ETH",
      "taker_order_type": "liquidation",
      "order_type": "limit"
    } …,
  ]
}
---



