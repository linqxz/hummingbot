
# Kraken Futures API Error Codes

## General Errors
| **Error Code**           | **Description**                                                                              |
|---------------------------|----------------------------------------------------------------------------------------------|
| `Json Parse Error`        | The request failed to pass valid JSON as an argument.                                       |
| `Server Error`            | There was an error processing the request.                                                  |
| `Unavailable`             | The endpoint being called is unavailable.                                                   |
| `accountInactive`         | The Futures account the request refers to is inactive.                                      |
| `apiLimitExceeded`        | The API limit for the calling IP address has been exceeded.                                 |
| `authenticationError`     | The request could not be authenticated.                                                     |
| `insufficientFunds`       | The amount requested for transfer is below the amount of funds available.                   |
| `invalidAccount`          | The Futures account the transfer request refers to is invalid.                              |
| `invalidAmount`           | The amount the transfer request refers to is invalid.                                       |
| `invalidArgument`         | One or more arguments provided are invalid.                                                 |
| `invalidUnit`             | The unit the transfer request refers to is invalid.                                         |
| `marketUnavailable`       | The market is currently unavailable.                                                        |
| `nonceBelowThreshold`     | The provided nonce is below the threshold.                                                  |
| `nonceDuplicate`          | The provided nonce is a duplicate and has been used in a previous request.                  |
| `notFound`                | The requested information could not be found.                                               |
| `requiredArgumentMissing` | One or more required arguments are missing.                                                 |
| `unknownError`            | An unknown error has occurred.                                                              |

---

## Market or Order-Specific Errors
| **Error Code**              | **Description**                                                                                   |
|------------------------------|---------------------------------------------------------------------------------------------------|
| `LOADING_MARKET`            | The market is currently loading and unavailable.                                                 |
| `MARKET_SUSPENDED`          | The market is suspended.                                                                          |
| `MARKET_NOT_FOUND`          | The specified market was not found.                                                              |
| `INVALID_PRICE`             | The specified price is invalid.                                                                  |
| `INVALID_QUANTITY`          | The specified quantity is invalid.                                                               |
| `SMALL_ORDER_LIMIT_EXCEEDED`| The order is too small to execute.                                                                |
| `INSUFFICIENT_MARGIN`       | The margin available is insufficient for the requested operation.                                 |
| `WOULD_CAUSE_LIQUIDATION`   | The action would cause liquidation.                                                              |
| `CLIENT_ORDER_ID_IN_USE`    | The provided client order ID is already in use.                                                  |
| `CLIENT_ORDER_ID_TOO_LONG`  | The client order ID exceeds the maximum allowed length.                                           |
| `MAX_POSITION_EXCEEDED`     | The position size exceeds the maximum allowed limit.                                              |
| `PRICE_COLLAR`              | The order price falls outside the allowed range (price collar).                                   |
| `PRICE_DISLOCATION`         | The order price is dislocated compared to the market price.                                       |
| `POST_WOULD_EXECUTE`        | A post-only order would execute immediately, violating the post-only condition.                   |
| `IOC_WOULD_NOT_EXECUTE`     | An IOC (Immediate or Cancel) order would not execute.                                             |
| `WOULD_EXECUTE_SELF`        | The order would result in self-execution.                                                        |
| `REJECTED_AFTER_EXECUTION`  | The order was rejected after execution.                                                          |
| `MARKET_IS_POST_ONLY`       | The market is currently post-only.                                                               |
| `ORDER_LIMIT_EXCEEDED`      | The order limit has been exceeded.                                                               |
| `FIXED_LEVERAGE_TOO_HIGH`   | The specified leverage exceeds the maximum allowed.                                              |
