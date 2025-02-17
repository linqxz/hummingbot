## Kraken Perpetual API Public

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

## GET /https://futures.kraken.com/api/charts/v1/{tick_type}

#### Parameters

###### Required
- **tick_type** (`Stringenum: "spot", "mark", "trade"`)
  - Tick Types
---

## GET /https://futures.kraken.com/api/charts/v1/{tick_type}/{symbol}

#### Parameters

###### Required
- **symbol** (`String`)
  - Market symbol

- **tick_type** (`Stringenum: "spot", "mark", "trade"`)
  - Tick Types
---

## GET /https://futures.kraken.com/api/charts/v1/{tick_type}/{symbol}/{resolution}

#### Parameters

###### Required
- **resolution** (`Stringenum: "1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"`)
  - Resolution

- **symbol** (`String`)
  - Market symbol

- **tick_type** (`Stringenum: "spot", "mark", "trade"`)
  - Tick Types

###### Optional
- **count** (`Integer**min: **0`)
  - Number of candles to return.

- **from** (`Number`)
  - From date in epoch seconds

- **to** (`Number`)
  - To date in epoch seconds

#### Sample Response

{
  "candles": [
    {
      "close": "56250.00000000000",
      "high": "56475.00000000000",
      "low": "55935.00000000000",
      "open": "56294.00000000000",
      "time": 1620816960000,
      "volume": 10824
    }
  ],
  "more_candles": false
}

---

## GET /https://futures.kraken.com/api/history/v3/market/{tradeable}/executions

#### Parameters

###### Required
- **tradeable** (`String`)
  - 

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

#### Sample Response

{
  "continuationToken": "c3RyaW5n",
  "elements": [
    {
      "event": {
        "Execution": {
          "execution": {
            "limitFilled": false,
            "makerOrder": {
              "direction": "Buy",
              "lastUpdateTimestamp": 1604937694000,
              "limitPrice": "1234.56789",
              "orderType": "string",
              "quantity": "1234.56789",
              "reduceOnly": false,
              "timestamp": 1604937694000,
              "tradeable": "string",
              "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45"
            },
            "markPrice": "1234.56789",
            "oldTakerOrder": {
              "direction": "Buy",
              "lastUpdateTimestamp": 1604937694000,
              "limitPrice": "1234.56789",
              "orderType": "string",
              "quantity": "1234.56789",
              "reduceOnly": false,
              "timestamp": 1604937694000,
              "tradeable": "string",
              "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45"
            },
            "price": "1234.56789",
            "quantity": "1234.56789",
            "takerOrder": {
              "direction": "Buy",
              "lastUpdateTimestamp": 1604937694000,
              "limitPrice": "1234.56789",
              "orderType": "string",
              "quantity": "1234.56789",
              "reduceOnly": false,
              "timestamp": 1604937694000,
              "tradeable": "string",
              "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45"
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
  "len": 0
}

---

## GET /https://futures.kraken.com/api/history/v3/market/{tradeable}/orders

#### Parameters

###### Required
- **tradeable** (`String`)
  - 

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

#### Sample Response

{
  "continuationToken": "c3RyaW5n",
  "elements": [
    {
      "event": {
        "OrderPlaced": {
          "order": {
            "direction": "Buy",
            "lastUpdateTimestamp": 1604937694000,
            "limitPrice": "1234.56789",
            "orderType": "string",
            "quantity": "1234.56789",
            "reduceOnly": false,
            "timestamp": 1604937694000,
            "tradeable": "string",
            "uid": "2ceb1d31-f619-457b-870c-fd4ddbb10d45"
          },
          "reason": "string",
          "reducedQuantity": "string"
        }
      },
      "timestamp": 1604937694000,
      "uid": "string"
    }
  ],
  "len": 0
}

---

## GET /https://futures.kraken.com/api/history/v3/market/{tradeable}/price

#### Parameters

###### Required
- **tradeable** (`String`)
  - 

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

#### Sample Response

{
  "continuationToken": "c3RyaW5n",
  "elements": [
    {
      "event": {
        "price": "1234.56789"
      },
      "timestamp": 1604937694000,
      "uid": "string"
    }
  ],
  "len": 0
}

---

## GET /orderbook

#### Parameters

###### Required
- **symbol** (`String`)
  - The symbol of the Futures.

#### Sample Response

{
  "orderBook": {
    "asks": [
      [
        40186,
        5.0183
      ],
      [
        40190,
        0.4183
      ]
    ],
    "bids": [
      [
        40178,
        5
      ],
      [
        40174,
        4.2
      ],
      [
        40170,
        7.2
      ]
    ]
  }
}

---

## GET /instruments

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **instruments** (``)
  - Arrayarray items: [Instrument]

#### Sample Response

{
  "instruments": [
    {
      "category": "",
      "contractSize": 1,
      "contractValueTradePrecision": 0,
      "feeScheduleUid": "eef90775-995b-4596-9257-0917f6134766",
      "fundingRateCoefficient": 8,
      "impactMidSize": 1,
      "isin": "GB00J62YGL67",
      "marginLevels": [
        {
          "contracts": 0,
          "initialMargin": 0.02,
          "maintenanceMargin": 0.01
        },
        {
          "contracts": 500000,
          "initialMargin": 0.04,
          "maintenanceMargin": 0.02
        },
        {
          "contracts": 1000000,
          "initialMargin": 0.06,
          "maintenanceMargin": 0.03
        },
        {
          "contracts": 3000000,
          "initialMargin": 0.1,
          "maintenanceMargin": 0.05
        },
        {
          "contracts": 6000000,
          "initialMargin": 0.15,
          "maintenanceMargin": 0.075
        },
        {
          "contracts": 12000000,
          "initialMargin": 0.25,
          "maintenanceMargin": 0.125
        },
        {
          "contracts": 20000000,
          "initialMargin": 0.3,
          "maintenanceMargin": 0.15
        },
        {
          "contracts": 50000000,
          "initialMargin": 0.4,
          "maintenanceMargin": 0.2
        }
      ],
      "maxPositionSize": 1000000,
      "maxRelativeFundingRate": 0.001,
      "openingDate": "2022-01-01T00:00:00.000Z",
      "postOnly": false,
      "retailMarginLevels": [
        {
          "contracts": 0,
          "initialMargin": 0.5,
          "maintenanceMargin": 0.25
        }
      ],
      "symbol": "PI_XBTUSD",
      "tags": [],
      "tickSize": 0.5,
      "tradeable": true,
      "type": "futures_inverse",
      "underlying": "rr_xbtusd"
    },
    {
      "category": "",
      "contractSize": 1,
      "contractValueTradePrecision": 0,
      "feeScheduleUid": "eef90775-995b-4596-9257-0917f6134766",
      "impactMidSize": 1,
      "isin": "GB00JVMLP260",
      "lastTradingTime": "2022-09-30T15:00:00.000Z",
      "marginLevels": [
        {
          "contracts": 0,
          "initialMargin": 0.02,
          "maintenanceMargin": 0.01
        },
        {
          "contracts": 500000,
          "initialMargin": 0.04,
          "maintenanceMargin": 0.02
        },
        {
          "contracts": 1000000,
          "initialMargin": 0.06,
          "maintenanceMargin": 0.03
        },
        {
          "contracts": 3000000,
          "initialMargin": 0.1,
          "maintenanceMargin": 0.05
        },
        {
          "contracts": 6000000,
          "initialMargin": 0.15,
          "maintenanceMargin": 0.075
        },
        {
          "contracts": 9000000,
          "initialMargin": 0.25,
          "maintenanceMargin": 0.125
        },
        {
          "contracts": 15000000,
          "initialMargin": 0.3,
          "maintenanceMargin": 0.15
        },
        {
          "contracts": 30000000,
          "initialMargin": 0.4,
          "maintenanceMargin": 0.2
        }
      ],
      "maxPositionSize": 1000000,
      "openingDate": "2022-01-01T00:00:00.000Z",
      "postOnly": false,
      "retailMarginLevels": [
        {
          "contracts": 0,
          "initialMargin": 0.5,
          "maintenanceMargin": 0.25
        }
      ],
      "symbol": "FI_XBTUSD_220930",
      "tags": [],
      "tickSize": 0.5,
      "tradeable": true,
      "type": "futures_inverse",
      "underlying": "rr_xbtusd"
    },
    {
      "category": "Layer 1",
      "contractSize": 1,
      "contractValueTradePrecision": 4,
      "feeScheduleUid": "5b755fea-c5b0-4307-a66e-b392cd5bd931",
      "fundingRateCoefficient": 8,
      "impactMidSize": 1,
      "marginLevels": [
        {
          "initialMargin": 0.02,
          "maintenanceMargin": 0.01,
          "numNonContractUnits": 0
        },
        {
          "initialMargin": 0.04,
          "maintenanceMargin": 0.02,
          "numNonContractUnits": 500000
        },
        {
          "initialMargin": 0.1,
          "maintenanceMargin": 0.05,
          "numNonContractUnits": 2000000
        },
        {
          "initialMargin": 0.2,
          "maintenanceMargin": 0.1,
          "numNonContractUnits": 5000000
        },
        {
          "initialMargin": 0.3,
          "maintenanceMargin": 0.15,
          "numNonContractUnits": 10000000
        },
        {
          "initialMargin": 0.5,
          "maintenanceMargin": 0.25,
          "numNonContractUnits": 30000000
        }
      ],
      "maxPositionSize": 1000000,
      "maxRelativeFundingRate": 0.001,
      "openingDate": "2022-01-01T00:00:00.000Z",
      "postOnly": false,
      "retailMarginLevels": [
        {
          "initialMargin": 0.02,
          "maintenanceMargin": 0.01,
          "numNonContractUnits": 0
        },
        {
          "initialMargin": 0.04,
          "maintenanceMargin": 0.02,
          "numNonContractUnits": 500000
        },
        {
          "initialMargin": 0.1,
          "maintenanceMargin": 0.05,
          "numNonContractUnits": 2000000
        },
        {
          "initialMargin": 0.2,
          "maintenanceMargin": 0.1,
          "numNonContractUnits": 5000000
        },
        {
          "initialMargin": 0.3,
          "maintenanceMargin": 0.15,
          "numNonContractUnits": 10000000
        },
        {
          "initialMargin": 0.5,
          "maintenanceMargin": 0.25,
          "numNonContractUnits": 30000000
        }
      ],
      "symbol": "PF_XBTUSD",
      "tags": [],
      "tickSize": 1,
      "tradeable": true,
      "type": "flexible_futures"
    }
  ],
  "result": "success",
  "serverTime": "2022-06-28T09:29:04.243Z"
}

---

## GET /instruments/status

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **instrumentStatus** (``)
  - Arrayarray items: [InstrumentStatus]

#### Sample Response

{
  "instrumentStatus": [
    {
      "extremeVolatilityInitialMarginMultiplier": 0,
      "isExperiencingDislocation": false,
      "isExperiencingExtremeVolatility": false,
      "priceDislocationDirection": "ABOVE_UPPER_BOUND",
      "tradeable": "PI_XBTUSD"
    }
  ]
}

---

## GET /instruments/{symbol}/status

#### Parameters

###### Required
- **symbol** (`Stringregex pattern: [A-Z0-9_.]+`)
  - Market symbol.Market symbol

#### Sample Response

{
  "extremeVolatilityInitialMarginMultiplier": 0,
  "isExperiencingDislocation": false,
  "isExperiencingExtremeVolatility": false,
  "priceDislocationDirection": "ABOVE_UPPER_BOUND",
  "tradeable": "PI_XBTUSD"
}

---


## GET /tickers

#### Parameters

###### Required
- **result** (`"success"`)
  - Stringvalue: "success"

- **serverTime** (`"2020-08-27T17:03:33.196Z"`)
  - Stringformat: date-time

###### Optional
- **tickers** (``)
  - Arrayarray items: [Ticker]

#### Sample Response

{
  "result": "success",
  "serverTime": "2022-06-17T11:00:31.335Z",
  "tickers": [
    {
      "ask": 49289,
      "askSize": 139984,
      "bid": 8634,
      "bidSize": 1000,
      "change24h": 1.9974017538161748,
      "fundingRate": 1.18588737106e-07,
      "fundingRatePrediction": 1.1852486794e-07,
      "indexPrice": 21087.8,
      "last": 49289,
      "lastSize": 100,
      "lastTime": "2022-06-17T10:46:35.705Z",
      "markPrice": 30209.9,
      "open24h": 49289,
      "openInterest": 149655,
      "pair": "XBT:USD",
      "postOnly": false,
      "suspended": false,
      "symbol": "PI_XBTUSD",
      "tag": "perpetual",
      "vol24h": 15304,
      "volumeQuote": 7305.2
    },
    {
      "bid": 28002,
      "bidSize": 900,
      "change24h": 1.9974017538161748,
      "indexPrice": 21087.8,
      "last": 28002,
      "lastSize": 100,
      "lastTime": "2022-06-17T10:45:57.177Z",
      "markPrice": 20478.5,
      "open24h": 28002,
      "openInterest": 10087,
      "pair": "XBT:USD",
      "postOnly": false,
      "suspended": false,
      "symbol": "FI_XBTUSD_211231",
      "tag": "month",
      "vol24h": 100,
      "volumeQuote": 843.9
    },
    {
      "last": 21088,
      "lastTime": "2022-06-17T11:00:30.000Z",
      "symbol": "in_xbtusd"
    },
    {
      "last": 20938,
      "lastTime": "2022-06-16T15:00:00.000Z",
      "symbol": "rr_xbtusd"
    }
  ]
}

---

## GET /tickers/{symbol}

#### Parameters

###### Required
- **symbol** (`Stringregex pattern: [A-Z0-9_.]+`)
  - Market symbol.Market symbol

#### Sample Response

{
  "result": "success",
  "serverTime": "2022-06-17T11:00:31.335Z",
  "ticker": {
    "ask": 49289,
    "askSize": 139984,
    "bid": 8634,
    "bidSize": 1000,
    "change24h": 1.9974017538161748,
    "fundingRate": 1.18588737106e-07,
    "fundingRatePrediction": 1.1852486794e-07,
    "indexPrice": 21087.8,
    "last": 49289,
    "lastSize": 100,
    "lastTime": "2022-06-17T10:46:35.705Z",
    "markPrice": 30209.9,
    "open24h": 49289,
    "openInterest": 149655,
    "pair": "XBT:USD",
    "postOnly": false,
    "suspended": false,
    "symbol": "pi_xbtusd",
    "tag": "perpetual",
    "vol24h": 15304,
    "volumeQuote": 40351.34
  }
}

---

## GET /v4/historicalfundingrates

#### Parameters

###### Required
- **symbol** (`Stringregex pattern: [A-Z0-9_.]+`)
  - Market symbol.Market symbol

#### Sample Response

{
  "rates": [
    {
      "fundingRate": -8.15861558e-10,
      "relativeFundingRate": -1.6898883333333e-05,
      "timestamp": "2022-06-28T00:00:00.000Z"
    },
    {
      "fundingRate": -2.6115278e-11,
      "relativeFundingRate": -5.40935416667e-07,
      "timestamp": "2022-06-28T04:00:00.000Z"
    },
    {
      "fundingRate": -4.08356853e-10,
      "relativeFundingRate": -8.521190625e-06,
      "timestamp": "2022-06-28T08:00:00.000Z"
    }
  ],
  "result": "success",
  "serverTime": "2022-06-28T09:29:04.243Z"
}
