## Kraken Perpetual API Introduction

## Base URLs
- HTTP API: `https://futures.kraken.com/derivatives/api/v3/`
- WebSocket API: `wss://futures.kraken.com/ws/v1`
- Direct Access URLs (requires IP whitelisting):
  - HTTP: `https://api.futures.kraken.com`
  - WebSocket: `wss://api.futures.kraken.com`

## Authentication
Required headers for authenticated endpoints:
- `APIKey`: Your public API key
- `Authent`: Authentication string
- `Nonce`: Optional incrementing integer (e.g., timestamp in milliseconds)

## Authentication Process
1. Generate `postData`: Concatenate arguments in `key=value` format
2. Generate `Authent`:
   - Concatenate: `postData + Nonce + endpointPath`
   - Hash with SHA-256
   - Base64-decode API secret
   - Hash result with HMAC-SHA-512
   - Base64-encode final result

## Rate Limits
- /derivatives endpoints: 500 cost units per 10 seconds
- /history endpoints: 100 tokens, replenishes at 100 per 10 minutes

####Common endpoint costs:
- Send/Edit/Cancel Order: 10 units
- Batch Order: 9 + batch size
- Account Info: 2 units
- Open Positions: 2 units
- Fills: 2-25 units
- Cancel All Orders: 25 units

--
###### API Key Management
- Generate keys through account settings
- Access levels:
  - No Access
  - Read Only
  - Full Access
  - Withdrawal Access
- Maximum 50 keys per account
- Store private key securely - shown only once during generation

###### Best Practices
1. Use IP whitelisting for reduced latency
2. Implement proper error handling
3. Monitor rate limits
4. Use batch operations for multiple orders
5. Keep track of nonce values
6. Maintain secure storage of API credentials


## WebSocket API Introduction

#### Overview
The Kraken Futures WebSocket API provides real-time updates for market data, account balances, orders, and more. Accessing private feeds requires authentication via a signed challenge process.

---

#### Sign Challenge

###### What is a Challenge?
The `challenge` is a unique UUID string, provided by the server upon request, that must be signed using the user's `api_secret`. The signed challenge is then included in all subscription and unsubscription requests to private feeds.

###### Signing Process
To generate the signed challenge, follow these steps:

1. Obtain the `challenge` string from the server.
2. Hash the `challenge` using the **SHA-256 algorithm**.
3. Base64-decode your `api_secret`.
4. Use the result from step 3 to hash the result from step 2 with the **HMAC-SHA-512 algorithm**.
5. Base64-encode the result from step 4 to produce the signed challenge.

###### Example

######## Inputs:
- **Challenge**: `c100b894-1729-464d-ace1-52dbce11db42`
- **API Secret**: `7zxMEF5p/Z8l2p2U7Ghv6x14Af+Fx+92tPgUdVQ748FOIrEoT9bgT+bTRfXc5pz8na+hL/QdrCVG7bh9KpT0eMTm`

######## Output:
- **Signed Challenge**: `4JEpF3ix66GA2B+ooK128Ift4XQVtc137N9yeg4Kqsn9PI0Kpzbysl9M1IeCEdjg0zl00wkVqcsnG4bmnlMb3A==`

---

#### Subscriptions

###### Steps to Subscribe
1. Establish a WebSocket connection using the following URL:
   ```
   wss://futures.kraken.com/ws/v1
   ```

2. Send a message to request the `challenge`.

3. Solve the challenge by following the signing process outlined above.

4. Send the `subscribe` request, including the original challenge (`original_challenge`) and the signed challenge (`signed_challenge`):
   ```json
   {
       "event": "subscribe",
       "feed": "open_orders",
       "original_challenge": "c100b894-1729-464d-ace1-52dbce11db42",
       "signed_challenge": "4JEpF3ix66GA2B+ooK128Ift4XQVtc137N9yeg4Kqsn9PI0Kpzbysl9M1IeCEdjg0zl00wkVqcsnG4bmnlMb3A=="
   }
   ```

---

#### Connection Management

###### Keeping the Connection Alive
To prevent the WebSocket connection from timing out, send a **ping request** at least every 60 seconds:
```json
{
    "event": "ping"
}
```

###### Snapshots and Updates
When subscribing to a feed, the API first sends a **snapshot** of the current state, followed by real-time updates.

---

#### Limits

###### Current Limits:
| Resource       | Allowance | Replenish Period |
|----------------|-----------|------------------|
| Connections    | 100       | N/A              |
| Requests       | 100       | 1 second         |

Exceeding these limits may result in throttling or dropped connections. Ensure your application respects these limits to maintain API access.

---

#### Authentication
To access private feeds, the client must:
1. Request a `challenge` using their `api_key`.
2. Solve the challenge and include both the `original_challenge` and the `signed_challenge` in subscription requests.

---

#### Example Code

###### Python Implementation: Signing the Challenge
```python
import hmac
import hashlib
import base64

## Inputs
challenge = "c100b894-1729-464d-ace1-52dbce11db42"
api_secret = "7zxMEF5p/Z8l2p2U7Ghv6x14Af+Fx+92tPgUdVQ748FOIrEoT9bgT+bTRfXc5pz8na+hL/QdrCVG7bh9KpT0eMTm"

## Step 1: Hash the challenge with SHA-256
hashed_challenge = hashlib.sha256(challenge.encode()).digest()

## Step 2: Base64-decode the API secret
decoded_secret = base64.b64decode(api_secret)

## Step 3: Hash the result using HMAC-SHA-512
signed_challenge = hmac.new(decoded_secret, hashed_challenge, hashlib.sha512).digest()

## Step 4: Base64-encode the result
signed_challenge_encoded = base64.b64encode(signed_challenge).decode()

print("Signed Challenge:", signed_challenge_encoded)
```

---
