## Kraken Perpetual API Private

## GET /accounts

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **accounts** (``)
  - Account

#### Sample Response

{
  "accounts": {
    "cash": {
      "balances": {
        "xbt": 141.31756797,
        "xrp": 52465.1254
      },
      "type": "cashAccount"
    },
    "fi_xbtusd": {
      "auxiliary": {
        "af": 100.73891563,
        "pnl": 12.42134766,
        "pv": 153.73891563
      },
      "balances": {
        "FI_XBTUSD_171215": 50000,
        "FI_XBTUSD_180615": -15000,
        "xbt": 141.31756797,
        "xrp": 0
      },
      "currency": "xbt",
      "marginRequirements": {
        "im": 52.8,
        "lt": 39.6,
        "mm": 23.76,
        "tt": 15.84
      },
      "triggerEstimates": {
        "im": 3110,
        "lt": 2890,
        "mm": 3000,
        "tt": 2830
      },
      "type": "marginAccount"
    },
    "flex": {
      "availableMargin": 34122.66,
      "balanceValue": 34995.52,
      "collateralValue": 34122.66,
      "currencies": {
        "EUR": {
          "available": 4540.5837374453,
          "collateral": 4886.906656949836,
          "quantity": 4540.5837374453,
          "value": 4999.137289089901
        },
        "USD": {
          "available": 5000,
          "collateral": 5000,
          "quantity": 5000,
          "value": 5000
        },
        "XBT": {
          "available": 0.1185308247,
          "collateral": 4886.49976674881,
          "quantity": 0.1185308247,
          "value": 4998.721054420551
        }
      },
      "initialMargin": 0,
      "initialMarginWithOrders": 0,
      "maintenanceMargin": 0,
      "marginEquity": 34122.66,
      "pnl": 0,
      "portfolioValue": 34995.52,
      "totalUnrealized": 0,
      "totalUnrealizedAsMargin": 0,
      "type": "multiCollateralMarginAccount",
      "unrealizedFunding": 0
    }
  },
  "result": "success",
  "serverTime": "2016-02-25T09:45:53.818Z"
}

---

## GET /assignmentprogram/current

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **participants** (``)
  - Arrayarray items: [AssignmentParticipantDetails]

#### Sample Response

{
  "participants": [
    {
      "id": 0.0,
      "participant": {
        "acceptLong": true,
        "acceptShort": true,
        "contract": "PF_BTCUSD",
        "contractType": "flex",
        "enabled": true,
        "maxPosition": 10,
        "maxSize": 10,
        "timeFrame": "weekdays"
      }
    }
  ]
}

---

## GET /assignmentprogram/history

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **participants** (``)
  - Arrayarray items: [AssignmentParticipantHistory]

#### Sample Response

{
  "participants": [
    {
      "deleted": false,
      "participant": {
        "acceptLong": true,
        "acceptShort": true,
        "contract": "PF_BTCUSD",
        "contractType": "flex",
        "enabled": true,
        "maxPosition": 10,
        "maxSize": 10,
        "timeFrame": "weekdays"
      }
    }
  ]
}

---

## GET /feeschedules

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **feeSchedules** (``)
  - Arrayarray items: [FeeSchedule]

#### Sample Response

{
  "feeSchedules": [
    {
      "name": "PGTMainFees",
      "tiers": [
        {
          "makerFee": 0.02,
          "takerFee": 0.05,
          "usdVolume": 0
        },
        {
          "makerFee": 0.015,
          "takerFee": 0.04,
          "usdVolume": 100000
        },
        {
          "makerFee": 0.0125,
          "takerFee": 0.03,
          "usdVolume": 1000000
        },
        {
          "makerFee": 0.01,
          "takerFee": 0.025,
          "usdVolume": 5000000
        },
        {
          "makerFee": 0.0075,
          "takerFee": 0.02,
          "usdVolume": 10000000
        },
        {
          "makerFee": 0.005,
          "takerFee": 0.015,
          "usdVolume": 20000000
        },
        {
          "makerFee": 0.0025,
          "takerFee": 0.0125,
          "usdVolume": 50000000
        },
        {
          "makerFee": 0,
          "takerFee": 0.01,
          "usdVolume": 100000000
        }
      ],
      "uid": "7fc4d7c0-464f-4029-a9bb-55856d0c5247"
    },
    {
      "name": "mainfees",
      "tiers": [
        {
          "makerFee": 0.02,
          "takerFee": 0.05,
          "usdVolume": 0
        },
        {
          "makerFee": 0.015,
          "takerFee": 0.04,
          "usdVolume": 100000
        },
        {
          "makerFee": 0.0125,
          "takerFee": 0.03,
          "usdVolume": 1000000
        },
        {
          "makerFee": 0.01,
          "takerFee": 0.025,
          "usdVolume": 5000000
        },
        {
          "makerFee": 0.0075,
          "takerFee": 0.02,
          "usdVolume": 10000000
        },
        {
          "makerFee": 0.005,
          "takerFee": 0.015,
          "usdVolume": 20000000
        },
        {
          "makerFee": 0.0025,
          "takerFee": 0.0125,
          "usdVolume": 50000000
        },
        {
          "makerFee": 0,
          "takerFee": 0.01,
          "usdVolume": 100000000
        }
      ],
      "uid": "d46c2190-81e3-4370-a333-424f24387829"
    }
  ],
  "result": "success",
  "serverTime": "2022-03-31T20:38:53.677Z"
}

---

## GET /feeschedules/volumes

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **volumesByFeeSchedule** (``)
  - FeeScheduleVolumes

#### Sample Response

{
  "result": "success",
  "serverTime": "2016-02-25T09:45:53.818Z",
  "volumesByFeeSchedule": {
    "eef90775-995b-4596-9257-0917f6134766": 53823
  }
}

---

## GET /fills

#### Parameters

###### Optional
- **lastFillTime** (`String`)
  - If not provided, returns the last 100 fills in any futures contract. If provided, returns the 100 entries before lastFillTime.

#### Sample Response

{
  "fills": [
    {
      "fillTime": "2020-07-22T13:37:27.077Z",
      "fillType": "maker",
      "fill_id": "3d57ed09-fbd6-44f1-8e8b-b10e551c5e73",
      "order_id": "693af756-055e-47ef-99d5-bcf4c456ebc5",
      "price": 9400,
      "side": "buy",
      "size": 5490,
      "symbol": "PI_XBTUSD"
    },
    {
      "fillTime": "2020-07-21T12:41:52.790Z",
      "fillType": "taker",
      "fill_id": "56b86ada-73b0-454d-a95a-e29e3e85b349",
      "order_id": "3f513c4c-683d-44ab-a73b-d296abbea201",
      "price": 9456,
      "side": "buy",
      "size": 5000,
      "symbol": "PI_XBTUSD"
    }
  ],
  "result": "success",
  "serverTime": "2020-07-22T13:44:24.311Z"
}

---

## GET /history

#### Parameters

###### Optional
- **lastTime** (`String`)
  - Returns the last 100 trades from the specified lastTime value.

- **symbol** (`String`)
  - The symbol of the Futures.

#### Sample Response

{
  "history": [
    {
      "execution_venue": "string",
      "instrument_identification_type": "string",
      "isin": "string",
      "notional_amount": 0.0,
      "notional_currency": "string",
      "price": 0.0,
      "price_currency": "string",
      "price_notation": "string",
      "publication_time": "string",
      "publication_venue": "string",
      "side": "string",
      "size": "string",
      "time": "string",
      "to_be_cleared": false,
      "trade_id": 0,
      "transaction_identification_code": "string",
      "type": "fill",
      "uid": "string"
    }
  ]
}

---

## GET /https://futures.kraken.com/api/charts/v1/

#### Parameters

###### Required
- **tick_type** (`Stringenum: "spot", "mark", "trade"`)
  - Tick Types
---

## GET /https://futures.kraken.com/api/charts/v1/analytics/liquidity-pool

#### Parameters

###### Required
- **interval** (`Integerenum: 60, 300, 900, 1800, 3600, 14400, 43200, 86400, 604800`)
  - Resolution in seconds

- **since** (`Integerformat: int64`)
  - epoch time in seconds

###### Optional
- **to** (`Integer`)
  - epoch time in seconds, default now

#### Sample Response

{
  "errors": [
    {
      "error_class": "string",
      "field": "string",
      "msg": "string",
      "severity": "string",
      "type": "string",
      "value": "string"
    }
  ],
  "result": {
    "data": [],
    "more": false,
    "timestamp": [
      0
    ]
  }
}

---

## GET /https://futures.kraken.com/api/charts/v1/analytics/{symbol}/{analytics_type}

#### Parameters

###### Required
- **analytics_type** (`Stringenum: "open-interest", "aggressor-differential", "trade-volume", "trade-count", "liquidation-volume", "rolling-volatility", "long-short-ratio", "long-short-info", "cvd", "top-traders", "orderbook", "spreads", "liquidity", "slippage", "future-basis"`)
  - Type of analytics

- **symbol** (`String`)
  - Market symbol

- **interval** (`Integerenum: 60, 300, 900, 1800, 3600, 14400, 43200, 86400, 604800`)
  - Resolution in seconds

- **since** (`Integerformat: int64`)
  - epoch time in seconds

###### Optional
- **to** (`Integer`)
  - epoch time in seconds, default now

#### Sample Response

{
  "errors": [
    {
      "error_class": "string",
      "field": "string",
      "msg": "string",
      "severity": "string",
      "type": "string",
      "value": "string"
    }
  ],
  "result": {
    "data": [],
    "more": false,
    "timestamp": [
      0
    ]
  }
}

---

## GET /https://futures.kraken.com/api/history/v3/account-log

#### Parameters

###### Optional
- **before** (`Integerformat: timestamp-milliseconds`)
  - Unix timestamp in milliseconds.

- **count** (`Integer`)
  - Amount of entries to be returned.

- **from** (`Integer`)
  - ID of the first entry (inclusive). IDs start at 1.

- **info** (`Stringenum: "futures trade", "futures liquidation", "futures assignor", "futures assignee", "futures unwind counterparty", "futures unwind bankrupt", "covered liquidation", "funding rate change", "conversion", "interest payment", "transfer", "cross-exchange transfer", "kfee applied", "subaccount transfer", "settlement", "admin transfer"`)
  - Type of entry to filter by. Only this type will be returned.

- **since** (`Integerformat: timestamp-milliseconds`)
  - Unix timestamp in milliseconds.

- **sort** (`Stringenum: "asc", "desc"`)
  - Order of events in response. asc = chronological, desc = reverse-chronological.

- **to** (`Integer`)
  - ID of the last entry (inclusive).

#### Sample Response

{
  "accountUid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45",
  "logs": [
    {
      "asset": "string",
      "booking_uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45",
      "collateral": "string",
      "contract": "string",
      "conversion_spread_percentage": 0.0,
      "date": "2019-08-24T14:15:22Z",
      "execution": "string",
      "fee": 0.0,
      "funding_rate": 0.0,
      "id": 1,
      "info": "futures trade",
      "liquidation_fee": 0.0,
      "margin_account": "string",
      "mark_price": 0.0,
      "new_average_entry_price": 0.0,
      "new_balance": 0.0,
      "old_average_entry_price": 0.0,
      "old_balance": 0.0,
      "realized_funding": 0.0,
      "realized_pnl": 0.0,
      "trade_price": 0.0
    }
  ]
}

---

## GET /https://futures.kraken.com/api/history/v3/accountlogcsv
---

## GET /https://futures.kraken.com/api/history/v3/executions

#### Parameters

###### Optional
- **before** (`Integerformat: timestamp-milliseconds`)
  - Timestamp in milliseconds.

- **continuation_token** (`Stringformat: base64`)
  - Opaque token from the Next-Continuation-Token header used to continue listing events. The sort parameter must be the same as in the previous request to continue listing in the same direction.

- **count** (`Integerformat: int64**min: **1`)
  - The maximum number of results to return. The upper bound is determined by a global limit.

- **since** (`Integerformat: timestamp-milliseconds`)
  - Timestamp in milliseconds.

- **sort** (`Stringenum: "asc", "desc"`)
  - Determines the order of events in response(s).- asc = chronological - desc = reverse-chronological

- **tradeable** (`String`)
  - If present events of other tradeables are filtered out.

#### Sample Response

{
  "accountUid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45",
  "continuationToken": "c3RyaW5n",
  "elements": [
    {
      "event": {
        "Execution": {
          "execution": {
            "limitFilled": false,
            "makerOrder": {
              "accountUid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45",
              "clientId": "string",
              "direction": "Buy",
              "filled": "1234.56789",
              "lastUpdateTimestamp": 1604937694000,
              "limitPrice": "1234.56789",
              "orderType": "Limit",
              "quantity": "1234.56789",
              "reduceOnly": false,
              "spotData": "string",
              "timestamp": 1604937694000,
              "tradeable": "string",
              "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45"
            },
            "makerOrderData": {
              "fee": "1234.56789",
              "positionSize": "1234.56789"
            },
            "markPrice": "1234.56789",
            "oldTakerOrder": {
              "accountUid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45",
              "clientId": "string",
              "direction": "Buy",
              "filled": "1234.56789",
              "lastUpdateTimestamp": 1604937694000,
              "limitPrice": "1234.56789",
              "orderType": "Limit",
              "quantity": "1234.56789",
              "reduceOnly": false,
              "spotData": "string",
              "timestamp": 1604937694000,
              "tradeable": "string",
              "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45"
            },
            "price": "1234.56789",
            "quantity": "1234.56789",
            "takerOrder": {
              "accountUid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45",
              "clientId": "string",
              "direction": "Buy",
              "filled": "1234.56789",
              "lastUpdateTimestamp": 1604937694000,
              "limitPrice": "1234.56789",
              "orderType": "Limit",
              "quantity": "1234.56789",
              "reduceOnly": false,
              "spotData": "string",
              "timestamp": 1604937694000,
              "tradeable": "string",
              "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45"
            },
            "takerOrderData": {
              "fee": "1234.56789",
              "positionSize": "1234.56789"
            },
            "timestamp": 1604937694000,
            "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45",
            "usdValue": "1234.56789"
          },
          "takerReducedQuantity": "string"
        }
      },
      "timestamp": 1604937694000,
      "uid": "string"
    }
  ],
  "len": 0,
  "serverTime": "2022-03-31T20:38:53.677Z"
}

---

## GET /initialmargin

#### Parameters

###### Required
- **orderType** (`Stringenum: "lmt", "mkt"`)
  - The order type:- lmt - a limit order - mkt - an immediate-or-cancel order with 1% price protection

- **side** (`Stringenum: "buy", "sell"`)
  - The direction of the order

- **size** (`Number`)
  - The size associated with the order. Note that different Futures have different contract sizes.

- **symbol** (`String`)
  - The symbol of the Futures.

###### Optional
- **limitPrice** (`Number`)
  - The limit price associated with the order.

#### Sample Response

{
  "error": "MARKET_SUSPENDED",
  "estimatedLiquidationThreshold": 0.0,
  "initialMargin": 0.0,
  "price": 0.0
}

---

## GET /initialmargin/maxordersize

#### Parameters

###### Required
- **orderType** (`Stringenum: "lmt", "mkt"`)
  - The order type:- lmt - a limit order - mkt - an immediate-or-cancel order with 1% price protection

- **symbol** (`String`)
  - The symbol of the Futures.

###### Optional
- **limitPrice** (`Number`)
  - The limit price if orderType is lmt

#### Sample Response

{
  "buyPrice": 0.0,
  "maxBuySize": 0.0,
  "maxSellSize": 0.0,
  "sellPrice": 0.0
}

---

## GET /leveragepreferences

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **leveragePreferences** (``)
  - Arrayarray items: [LeveragePreference]

#### Sample Response

{
  "leveragePreferences": [
    {
      "maxLeverage": 10,
      "symbol": "PF_XBTUSD"
    }
  ],
  "result": "success",
  "serverTime": "2022-06-28T15:01:12.762Z"
}

---

## GET /notifications

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **notifications** (``)
  - Arrayarray items: [Notification]

#### Sample Response

{
  "error": "apiLimitExceeded",
  "result": "error",
  "serverTime": "2016-02-25T09:45:53.818Z"
}

---

## GET /openorders

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **openOrders** (``)
  - Arrayarray items: [OpenOrder]

#### Sample Response

{
  "openOrders": [
    {
      "filledSize": 0,
      "lastUpdateTime": "2019-09-05T17:01:17.410Z",
      "limitPrice": 10640,
      "orderType": "lmt",
      "order_id": "59302619-41d2-4f0b-941f-7e7914760ad3",
      "receivedTime": "2019-09-05T17:01:17.410Z",
      "reduceOnly": true,
      "side": "sell",
      "status": "untouched",
      "symbol": "PI_XBTUSD",
      "unfilledSize": 304
    },
    {
      "filledSize": 0,
      "lastUpdateTime": "2019-09-05T16:47:47.519Z",
      "limitPrice": 7200,
      "orderType": "lmt",
      "order_id": "022774bc-2c4a-4f26-9317-436c8d85746d",
      "receivedTime": "2019-09-05T16:41:35.173Z",
      "reduceOnly": false,
      "side": "buy",
      "status": "untouched",
      "symbol": "PI_XBTUSD",
      "unfilledSize": 1501
    },
    {
      "filledSize": 0,
      "lastUpdateTime": "2019-09-05T16:38:43.651Z",
      "limitPrice": 10640,
      "orderType": "lmt",
      "order_id": "d08021f7-58cb-4f2c-9c86-da4c60de46bb",
      "receivedTime": "2019-09-05T16:38:43.651Z",
      "reduceOnly": true,
      "side": "sell",
      "status": "untouched",
      "symbol": "PI_XBTUSD",
      "unfilledSize": 10000
    },
    {
      "filledSize": 0,
      "lastUpdateTime": "2019-09-05T16:33:50.734Z",
      "limitPrice": 9400,
      "orderType": "lmt",
      "order_id": "179f9af8-e45e-469d-b3e9-2fd4675cb7d0",
      "receivedTime": "2019-09-05T16:33:50.734Z",
      "reduceOnly": false,
      "side": "buy",
      "status": "untouched",
      "symbol": "PI_XBTUSD",
      "unfilledSize": 10000
    },
    {
      "filledSize": 0,
      "lastUpdateTime": "2019-09-05T16:41:40.996Z",
      "limitPrice": 9400,
      "orderType": "lmt",
      "order_id": "9c2cbcc8-14f6-42fe-a020-6e395babafd1",
      "receivedTime": "2019-09-04T11:45:48.884Z",
      "reduceOnly": false,
      "side": "buy",
      "status": "untouched",
      "symbol": "PI_XBTUSD",
      "unfilledSize": 1000
    },
    {
      "filledSize": 0,
      "lastUpdateTime": "2019-09-03T12:52:17.945Z",
      "limitPrice": 8500,
      "orderType": "lmt",
      "order_id": "3deea5c8-0274-4d33-988c-9e5a3895ccf8",
      "receivedTime": "2019-09-03T12:52:17.945Z",
      "reduceOnly": false,
      "side": "buy",
      "status": "untouched",
      "symbol": "PI_XBTUSD",
      "unfilledSize": 102
    },
    {
      "filledSize": 0,
      "lastUpdateTime": "2019-09-02T12:54:34.347Z",
      "limitPrice": 7200,
      "orderType": "lmt",
      "order_id": "fcbb1459-6ed2-4b3c-a58c-67c4df7412cf",
      "receivedTime": "2019-09-02T12:54:34.347Z",
      "reduceOnly": false,
      "side": "buy",
      "status": "untouched",
      "symbol": "PI_XBTUSD",
      "unfilledSize": 1501
    }
  ],
  "result": "success",
  "serverTime": "2019-09-05T17:08:18.138Z"
}

---

## GET /openpositions

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **openPositions** (``)
  - Arrayarray items: [OpenPosition]

#### Sample Response

{
  "error": "apiLimitExceeded",
  "result": "error",
  "serverTime": "2016-02-25T09:45:53.818Z"
}

---

## GET /pnlpreferences

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **preferences** (``)
  - Arrayarray items: [PnlPreference]

#### Sample Response

{
  "preferences": [
    {
      "pnlCurrency": "BTC",
      "symbol": "PF_XBTUSD"
    }
  ],
  "result": "success",
  "serverTime": "2022-06-28T15:04:06.710Z"
}

---

## GET /self-trade-strategy

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

- **strategy** (``)
  - Stringenum: "REJECT_TAKER", "CANCEL_MAKER_SELF", "CANCEL_MAKER_CHILD", "CANCEL_MAKER_ANY"

#### Sample Response

{
  "strategy": "REJECT_TAKER"
}

---

## GET /subaccount/{subaccountUid}/trading-enabled

#### Sample Response

{
  "tradingEnabled": false
}

---

## GET /subaccounts

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **masterAccountUid** (``)
  - String

- **subaccounts** (``)
  - Arrayarray items: [SubAccount]

#### Sample Response

{
  "masterAccountUid": "ba598ca1-65c1-4f48-927d-0e2b647d627a",
  "result": "success",
  "serverTime": "2022-03-31T20:38:53.677Z",
  "subaccounts": [
    {
      "accountUid": "7f5c528e-2285-45f0-95f5-83d53d4bfcd2",
      "email": "email redacted",
      "flexAccount": {
        "availableMargin": 1543.91,
        "balanceValue": 1646.58,
        "collateralValue": 1543.91,
        "currencies": [
          {
            "available": 0.49999966035931903,
            "collateral": 1543.91104875,
            "currency": "eth",
            "quantity": 0.5,
            "value": 1646.575
          },
          {
            "available": 0,
            "collateral": 0,
            "currency": "usdt",
            "quantity": 0,
            "value": 0
          },
          {
            "available": 0,
            "collateral": 0,
            "currency": "gbp",
            "quantity": 0,
            "value": 0
          },
          {
            "available": 0,
            "collateral": 0,
            "currency": "xbt",
            "quantity": 0,
            "value": 0
          },
          {
            "available": 0,
            "collateral": 0,
            "currency": "usdc",
            "quantity": 0,
            "value": 0
          },
          {
            "available": 0,
            "collateral": 0,
            "currency": "usd",
            "quantity": 0,
            "value": 0
          }
        ],
        "initialMargin": 0,
        "initialMarginWithOrders": 0,
        "maintenanceMargin": 0,
        "marginEquity": 1543.91,
        "pnl": 0,
        "portfolioValue": 1646.58,
        "totalUnrealized": 0,
        "totalUnrealizedAsMargin": 0,
        "unrealizedFunding": 0
      },
      "fullName": "fullname redacted",
      "futuresAccounts": [
        {
          "availableMargin": 16187.33210488726,
          "name": "f-xrp:usd"
        },
        {
          "availableMargin": 67.59768318324302,
          "name": "f-eth:usd"
        },
        {
          "availableMargin": -0.0009056832839642471,
          "name": "f-xbt:usd"
        },
        {
          "availableMargin": 67.51126059691163,
          "name": "f-ltc:usd"
        },
        {
          "availableMargin": 2.34e-09,
          "name": "f-xrp:xbt"
        },
        {
          "availableMargin": 47.151615710695495,
          "name": "f-bch:usd"
        }
      ],
      "holdingAccounts": [
        {
          "amount": 0,
          "currency": "gbp"
        },
        {
          "amount": 4e-05,
          "currency": "bch"
        },
        {
          "amount": 13662.85078,
          "currency": "xrp"
        },
        {
          "amount": 0,
          "currency": "usd"
        },
        {
          "amount": 3.0000485057,
          "currency": "eth"
        },
        {
          "amount": 0,
          "currency": "usdt"
        },
        {
          "amount": 2e-05,
          "currency": "ltc"
        },
        {
          "amount": 0,
          "currency": "usdc"
        },
        {
          "amount": 3.46e-09,
          "currency": "xbt"
        }
      ]
    }
  ]
}

---

## GET /unwindqueue

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **queue** (``)
  - Arrayarray items: [UnwindQueue]

#### Sample Response

{
  "queue": [
    {
      "percentile": 100,
      "symbol": "PF_GMTUSD"
    },
    {
      "percentile": 20,
      "symbol": "FI_ETHUSD_220624"
    },
    {
      "percentile": 80,
      "symbol": "PF_UNIUSD"
    }
  ],
  "result": "success",
  "serverTime": "2022-06-13T18:01:18.695Z"
}
