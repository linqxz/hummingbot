## Kraken Perpetual API POST/PUT

## POST /assignmentprogram/add

#### Parameters

###### Required
- **acceptLong** (`Boolean`)
  - Accept to take long positions

- **acceptShort** (`Boolean`)
  - Accept to take short positions

- **contractType** (`String`)
  - Type of contract for the assignment program preference. Options can be found in the 'accounts' structure in the Get Wallets /accounts response

- **enabled** (`Boolean`)
  - enabled assignment

- **timeFrame** (`String`)
  - When is the program preference valid

###### Optional
- **contract** (`String`)
  - A specific contract for this assignment program preference. Required for "flex" contracts if base/quote currencies are not included.

- **maxPosition** (`Number`)
  - The maximum position

- **maxSize** (`Number`)
  - The maximum size for an assignment

#### Sample Response

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

---

## POST /assignmentprogram/delete

#### Parameters

###### Required
- **id** (`Number`)
  - Id of program to delete

#### Sample Response

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

---

## POST /batchorder

#### Parameters

###### Required
- **** (`Batch OrderAs a string formatted as application/`)
  - Contains the request body as a String

###### Optional
- **processBefore** (`String`)
  - The time before which the request should be processed, otherwise it is rejected.

#### Sample Response

{
  "batchStatus": [
    {
      "dateTimeReceived": "2019-09-05T16:41:35.173Z",
      "orderEvents": [
        {
          "order": {
            "cliOrdId": null,
            "filled": 0,
            "lastUpdateTimestamp": "2019-09-05T16:41:35.173Z",
            "limitPrice": 9400,
            "orderId": "022774bc-2c4a-4f26-9317-436c8d85746d",
            "quantity": 1000,
            "reduceOnly": false,
            "side": "buy",
            "symbol": "PI_XBTUSD",
            "timestamp": "2019-09-05T16:41:35.173Z",
            "type": "lmt"
          },
          "reducedQuantity": null,
          "type": "PLACE"
        }
      ],
      "order_id": "022774bc-2c4a-4f26-9317-436c8d85746d",
      "order_tag": "1",
      "status": "placed"
    },
    {
      "orderEvents": [
        {
          "new": {
            "cliOrdId": null,
            "filled": 0,
            "lastUpdateTimestamp": "2019-09-05T16:41:40.996Z",
            "limitPrice": 9400,
            "orderId": "9c2cbcc8-14f6-42fe-a020-6e395babafd1",
            "quantity": 1000,
            "reduceOnly": false,
            "side": "buy",
            "symbol": "PI_XBTUSD",
            "timestamp": "2019-09-04T11:45:48.884Z",
            "type": "lmt"
          },
          "old": {
            "cliOrdId": null,
            "filled": 0,
            "lastUpdateTimestamp": "2019-09-04T11:45:48.884Z",
            "limitPrice": 8500,
            "orderId": "9c2cbcc8-14f6-42fe-a020-6e395babafd1",
            "quantity": 102,
            "reduceOnly": false,
            "side": "buy",
            "symbol": "PI_XBTUSD",
            "timestamp": "2019-09-04T11:45:48.884Z",
            "type": "lmt"
          },
          "reducedQuantity": null,
          "type": "EDIT"
        }
      ],
      "order_id": "9c2cbcc8-14f6-42fe-a020-6e395babafd1",
      "status": "edited"
    },
    {
      "orderEvents": [
        {
          "order": {
            "cliOrdId": null,
            "filled": 0,
            "lastUpdateTimestamp": "2019-09-02T12:54:08.005Z",
            "limitPrice": 8500,
            "orderId": "566942c8-a3b5-4184-a451-622b09493129",
            "quantity": 100,
            "reduceOnly": false,
            "side": "buy",
            "symbol": "PI_XBTUSD",
            "timestamp": "2019-09-02T12:54:08.005Z",
            "type": "lmt"
          },
          "type": "CANCEL",
          "uid": "566942c8-a3b5-4184-a451-622b09493129"
        }
      ],
      "order_id": "566942c8-a3b5-4184-a451-622b09493129",
      "status": "cancelled"
    }
  ],
  "result": "success",
  "serverTime": "2019-09-05T16:41:40.996Z"
}

---

## POST /cancelallorders

#### Parameters

###### Optional
- **symbol** (`String`)
  - A futures product to cancel all open orders.

#### Sample Response

{
  "cancelStatus": {
    "cancelOnly": "all",
    "cancelledOrders": [
      {
        "order_id": "6180adfa-e4b1-4a52-adac-ea5417620dbd"
      },
      {
        "order_id": "89e3edbe-d739-4c52-b866-6f5a8407ff6e"
      },
      {
        "order_id": "0cd37a77-1644-4960-a7fb-9a1f6e0e46f7"
      }
    ],
    "orderEvents": [
      {
        "order": {
          "cliOrdId": null,
          "filled": 0,
          "lastUpdateTimestamp": "2019-08-01T15:57:08.508Z",
          "limitPrice": 10040,
          "orderId": "89e3edbe-d739-4c52-b866-6f5a8407ff6e",
          "quantity": 890,
          "reduceOnly": false,
          "side": "buy",
          "symbol": "PI_XBTUSD",
          "timestamp": "2019-08-01T15:57:08.508Z",
          "type": "post"
        },
        "type": "CANCEL",
        "uid": "89e3edbe-d739-4c52-b866-6f5a8407ff6e"
      },
      {
        "order": {
          "cliOrdId": null,
          "filled": 0,
          "lastUpdateTimestamp": "2019-08-01T15:57:14.003Z",
          "limitPrice": 10145,
          "orderId": "0cd37a77-1644-4960-a7fb-9a1f6e0e46f7",
          "quantity": 900,
          "reduceOnly": true,
          "side": "sell",
          "symbol": "PI_XBTUSD",
          "timestamp": "2019-08-01T15:57:14.003Z",
          "type": "lmt"
        },
        "type": "CANCEL",
        "uid": "0cd37a77-1644-4960-a7fb-9a1f6e0e46f7"
      }
    ],
    "receivedTime": "2019-08-01T15:57:37.518Z",
    "status": "cancelled"
  },
  "result": "success",
  "serverTime": "2019-08-01T15:57:37.520Z"
}

---

## POST /cancelallordersafter

#### Parameters

###### Optional
- **timeout** (`Numberformat: uint32`)
  - The timeout specified in seconds.

#### Sample Response

{
  "result": "success",
  "serverTime": "2018-06-19T16:51:23.839Z",
  "status": {
    "currentTime": "2018-06-19T16:51:23.839Z",
    "triggerTime": "0"
  }
}

---

## POST /cancelorder

#### Parameters

###### Optional
- **cliOrdId** (`String`)
  - The client unique identifier of the order to be cancelled.

- **order_id** (`String`)
  - The unique identifier of the order to be cancelled.

- **processBefore** (`String`)
  - The time before which the request should be processed, otherwise it is rejected.

#### Sample Response

{
  "cancelStatus": {
    "orderEvents": [
      {
        "order": {
          "cliOrdId": "1234568",
          "filled": 0,
          "lastUpdateTimestamp": "2020-07-22T13:25:56.366Z",
          "limitPrice": 8000,
          "orderId": "cb4e34f6-4eb3-4d4b-9724-4c3035b99d47",
          "quantity": 5500,
          "reduceOnly": false,
          "side": "buy",
          "symbol": "PI_XBTUSD",
          "timestamp": "2020-07-22T13:25:56.366Z",
          "type": "lmt"
        },
        "type": "CANCEL",
        "uid": "cb4e34f6-4eb3-4d4b-9724-4c3035b99d47"
      }
    ],
    "order_id": "cb4e34f6-4eb3-4d4b-9724-4c3035b99d47",
    "receivedTime": "2020-07-22T13:26:20.806Z",
    "status": "cancelled"
  },
  "result": "success",
  "serverTime": "2020-07-22T13:26:20.806Z"
}

---

## POST /editorder

#### Parameters

###### Optional
- **cliOrdId** (`String`)
  - The order identity that is specified from the user. It must be globally unique (Required if orderId is not included)

- **limitPrice** (`Number`)
  - The limit price associated with the order. Must not exceed the tick size of the contract.

- **orderId** (`String`)
  - ID of the order you wish to edit. (Required if CliOrdId is not included)

- **processBefore** (`String`)
  - The time before which the request should be processed, otherwise it is rejected.

- **size** (`Number`)
  - The size associated with the order

- **stopPrice** (`Number`)
  - The stop price associated with a stop order. Required if old orderType is stp. Must not exceed tick size of the contract. Note that for stp orders, limitPrice is also required and denotes the worst price at which the stp order can get filled.

- **trailingStopDeviationUnit** (`Stringenum: "PERCENT", "QUOTE_CURRENCY"`)
  - Only relevant for trailing stop orders.This defines how the trailing trigger price is calculated from the requested trigger signal. For example, if the max deviation is set to 10, the unit is 'PERCENT', and the underlying order is a sell, then the trigger price will never be more then 10% below the trigger signal. Similarly, if the deviation is 100, the unit is 'QUOTE_CURRENCY', the underlying order is a sell, and the contract is quoted in USD, then the trigger price will never be more than $100 below the trigger signal.

- **trailingStopMaxDeviation** (`Numbermin: **0.1max: **50`)
  - Only relevant for trailing stop orders. Maximum value of 50%, minimum value of 0.1% for 'PERCENT' 'maxDeviationUnit'.Is the maximum distance the trailing stop's trigger price may trail behind the requested trigger signal. It defines the threshold at which the trigger price updates.

#### Sample Response

{
  "editStatus": {
    "orderEvents": [
      {
        "new": {
          "cliOrdId": null,
          "filled": 0,
          "lastUpdateTimestamp": "2019-09-05T16:47:47.519Z",
          "limitPrice": 7200,
          "orderId": "022774bc-2c4a-4f26-9317-436c8d85746d",
          "quantity": 1501,
          "reduceOnly": false,
          "side": "buy",
          "symbol": "PI_XBTUSD",
          "timestamp": "2019-09-05T16:41:35.173Z",
          "type": "lmt"
        },
        "old": {
          "cliOrdId": null,
          "filled": 0,
          "lastUpdateTimestamp": "2019-09-05T16:41:35.173Z",
          "limitPrice": 9400,
          "orderId": "022774bc-2c4a-4f26-9317-436c8d85746d",
          "quantity": 1000,
          "reduceOnly": false,
          "side": "buy",
          "symbol": "PI_XBTUSD",
          "timestamp": "2019-09-05T16:41:35.173Z",
          "type": "lmt"
        },
        "reducedQuantity": null,
        "type": "EDIT"
      }
    ],
    "orderId": "022774bc-2c4a-4f26-9317-436c8d85746d",
    "receivedTime": "2019-09-05T16:47:47.521Z",
    "status": "edited"
  },
  "result": "success",
  "serverTime": "2019-09-05T16:47:47.521Z"
}

---

## POST /orders/status
---

## POST /sendorder

#### Parameters

###### Required
- **orderType** (`Stringenum: "lmt", "post", "ioc", "mkt", "stp", "take_profit", "trailing_stop"`)
  - The order type:- lmt - a limit order - post - a post-only limit order - mkt - an immediate-or-cancel order with 1% price protection - stp - a stop order - take_profit - a take profit order - ioc - an immediate-or-cancel order - trailing_stop - a trailing stop order

- **side** (`Stringenum: "buy", "sell"`)
  - The direction of the order

- **size** (`Number`)
  - The size associated with the order. Note that different Futures have different contract sizes.

- **symbol** (`String`)
  - The symbol of the Futures

###### Optional
- **cliOrdId** (`Stringmax str len: 100`)
  - The order identity that is specified from the user. It must be globally unique.

- **limitPrice** (`Number`)
  - The limit price associated with the order. Note that for stop orders, limitPrice denotes the worst price at which the stp or take_profit order can get filled at. If no limitPrice is provided the stp or take_profit order will trigger a market order. If placing a trailing_stop order then leave undefined.

- **limitPriceOffsetUnit** (`Stringenum: "QUOTE_CURRENCY", "PERCENT"`)
  - Can only be set together with limitPriceOffsetValue. This defines the unit for the relative limit price distance from the trigger's stopPrice.

- **limitPriceOffsetValue** (`Number`)
  - Can only be set for triggers, e.g. order types take_profit, stop and trailing_stop. If set, limitPriceOffsetUnit must be set as well. This defines a relative limit price depending on the trigger stopPrice. The price is determined when the trigger is activated by the triggerSignal. The offset can be positive or negative, there might be restrictions on the value depending on the limitPriceOffsetUnit.

- **processBefore** (`String`)
  - The time before which the request should be processed, otherwise it is rejected.

- **reduceOnly** (`Boolean`)
  - Set as true if you wish the order to only reduce an existing position.Any order which increases an existing position will be rejected. Default false.

- **stopPrice** (`Number`)
  - The stop price associated with a stop or take profit order.Required if orderType is stp or take_profit, but if placing a trailing_stop then leave undefined. Note that for stop orders, limitPrice denotes the worst price at which the stp or take_profit order can get filled at. If no limitPrice is provided the stp or take_profit order will trigger a market order.

- **trailingStopDeviationUnit** (`Stringenum: "PERCENT", "QUOTE_CURRENCY"`)
  - Required if the order type is trailing_stop.This defines how the trailing trigger price is calculated from the requested trigger signal. For example, if the max deviation is set to 10, the unit is 'PERCENT', and the underlying order is a sell, then the trigger price will never be more then 10% below the trigger signal. Similarly, if the deviation is 100, the unit is 'QUOTE_CURRENCY', the underlying order is a sell, and the contract is quoted in USD, then the trigger price will never be more than $100 below the trigger signal.

- **trailingStopMaxDeviation** (`Numbermin: **0.1max: **50`)
  - Required if the order type is trailing_stop. Maximum value of 50%, minimum value of 0.1% for 'PERCENT' 'maxDeviationUnit'.Is the maximum distance the trailing stop's trigger price may trail behind the requested trigger signal. It defines the threshold at which the trigger price updates.

- **triggerSignal** (`Stringenum: "mark", "spot", "last"`)
  - If placing a stp, take_profit or trailing_stop, the signal used for trigger.- mark - the mark price - index - the index price - last - the last executed trade

#### Sample Response

{
  "result": "success",
  "sendStatus": {
    "orderEvents": [
      {
        "amount": 10,
        "executionId": "e1ec9f63-2338-4c44-b40a-43486c6732d7",
        "orderPriorEdit": null,
        "orderPriorExecution": {
          "cliOrdId": null,
          "filled": 0,
          "lastUpdateTimestamp": "2019-12-11T17:17:33.888Z",
          "limitPrice": 7500,
          "orderId": "61ca5732-3478-42fe-8362-abbfd9465294",
          "quantity": 10,
          "reduceOnly": false,
          "side": "buy",
          "symbol": "PI_XBTUSD",
          "timestamp": "2019-12-11T17:17:33.888Z",
          "type": "lmt"
        },
        "price": 7244.5,
        "takerReducedQuantity": null,
        "type": "EXECUTION"
      }
    ],
    "order_id": "61ca5732-3478-42fe-8362-abbfd9465294",
    "receivedTime": "2019-12-11T17:17:33.888Z",
    "status": "placed"
  },
  "serverTime": "2019-12-11T17:17:33.888Z"
}

---

## POST /transfer

#### Parameters

###### Required
- **amount** (`Numberformat: decimalexclusive **min: **0`)
  - The amount to transfer

- **fromAccount** (`String`)
  - The wallet (cash or margin account) from which funds should be debited

- **toAccount** (`String`)
  - The wallet (cash or margin account) to which funds should be credited

- **unit** (`String`)
  - The currency unit to transfer

#### Sample Response

{
  "result": "success",
  "serverTime": "2020-08-27T17:03:33.196Z"
}

---

## POST /transfer/subaccount

#### Parameters

###### Required
- **amount** (`String`)
  - The amount to transfer

- **fromAccount** (`String`)
  - The wallet (cash or margin account) from which funds should be debited

- **fromUser** (`String`)
  - The user account (this or a sub account) from which funds should be debited

- **toAccount** (`String`)
  - The wallet (cash or margin account) to which funds should be credited

- **toUser** (`String`)
  - The user account (this or a sub account) to which funds should be credited

- **unit** (`String`)
  - The currency unit to transfer

#### Sample Response

{
  "error": "invalidUnit",
  "result": "error",
  "serverTime": "2016-02-25T09:45:53.818Z"
}

---

## POST /withdrawal

#### Parameters

###### Required
- **amount** (`Numberformat: decimalexclusive **min: **0`)
  - The amount of currency that shall be withdrawn back to spot wallet.

- **currency** (`String`)
  - The digital asset that shall be withdrawn back to spot wallet.

###### Optional
- **sourceWallet** (`String`)
  - The wallet from which the funds shall be withdrawn back to spot wallet. Default is "cash" wallet.

#### Sample Response

{
  "error": "Unavailable",
  "result": "error",
  "serverTime": "2019-05-15T09:24:16.968Z"
}

---

## PUT /leveragepreferences

#### Parameters

###### Required
- **symbol** (`String`)
  - Symbol for the leverage preference.

###### Optional
- **maxLeverage** (`Number`)
  - Max leverage to set.When present, the symbol's margin mode will be set to "isolated". When missing, the symbol's margin mode will be set to "cross".

#### Sample Response

{
  "result": "success",
  "serverTime": "2022-06-28T14:48:58.711Z"
}

---

## PUT /pnlpreferences

#### Parameters

###### Required
- **pnlPreference** (`String`)
  - The asset in which profit will be realised for the specific symbol.

- **symbol** (`String`)
  - The symbol for the PnL preference.

#### Sample Response

{
  "result": "success",
  "serverTime": "2022-06-28T14:48:58.711Z"
}

---

## PUT /self-trade-strategy

#### Parameters

###### Required
- **strategy** (`Stringenum: "REJECT_TAKER", "CANCEL_MAKER_SELF", "CANCEL_MAKER_CHILD", "CANCEL_MAKER_ANY"`)
  - Defines self trade behaviourSelf trade matching behaviour:- REJECT_TAKER - default behaviour, rejects the taker order that would match against a maker order from any sub-account - CANCEL_MAKER_SELF - only cancels the maker order if it is from the same account that sent the taker order - CANCEL_MAKER_CHILD - only allows master to cancel its own maker orders and orders from its sub-account - CANCEL_MAKER_ANY - allows both master accounts and their subaccounts to cancel maker orders

#### Sample Response

{
  "strategy": "REJECT_TAKER"
}

---

## PUT /subaccount/{subaccountUid}/trading-enabled

#### Sample Response

{
  "tradingEnabled": false
}
