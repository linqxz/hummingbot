import base64
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlencode, urlparse

from hummingbot.connector.derivative.kraken_perpetual import kraken_perpetual_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest, WSRequest


class KrakenPerpetualAuthError(Exception):
    """Exception class for Kraken Perpetual authentication errors."""
    ERROR_CODES = {
        "nonceBelowThreshold": "Nonce is below threshold",
        "nonceDuplicate": "Duplicate nonce value",
        "invalidSignature": "Invalid signature",
        "invalidKey": "Invalid API key",
        "invalidChallenge": "Invalid WebSocket challenge",
        "invalidTimestamp": "Invalid timestamp",
        "apiLimitExceeded": "API rate limit exceeded",
    }

    def __init__(self, message: str, error_code: Optional[str] = None):
        super().__init__(message)
        self.error_code = error_code
        if error_code and error_code in self.ERROR_CODES:
            self.message = f"{self.ERROR_CODES[error_code]}: {message}"


class KrakenPerpetualAuth(AuthBase):
    """
    Auth class required by Kraken Perpetual API

    Authentication Process:
    1. Generate postData by concatenating arguments in key=value format
    2. Generate Authent by:
       - Concatenating: postData + Nonce + endpointPath
       - Hashing with SHA-256
       - Base64-decoding API secret
       - Hashing result with HMAC-SHA-512
       - Base64-encoding final result
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: Optional[Callable] = None):
        """
        Initialize auth instance.
        :param api_key: The API key obtained from Kraken Futures
        :param secret_key: The API secret key obtained from Kraken Futures (base64 encoded)
        :param time_provider: Time synchronizer instance
        """
        self._logger = logging.getLogger(__name__)
        # self._logger.info("=== Initializing KrakenPerpetualAuth ===")
        # self._logger.info(f"API key length: {len(api_key) if api_key else 0}")
        # self._logger.info(f"Secret key length: {len(secret_key) if secret_key else 0}")
        
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider or (lambda: time.time())
        self._last_nonce = 0  # Track last nonce value
        
        # self._logger.info("Validating auth keys...")
        self._validate_auth_keys()  # Validate keys during initialization
        # self._logger.info("Auth keys validated successfully")

    @staticmethod
    def _is_valid_api_key(api_key: str) -> bool:
        """Validates API key format."""
        logger = logging.getLogger(__name__)
        # logger.info("=== Validating API key ===")
        if not api_key:
            logger.info("API key is empty")
            return False
        # API key should be at least 32 characters and contain only valid URL-safe base64 characters
        is_valid = len(api_key) >= 32 and all(c.isalnum() or c in ['-', '_', '+', '/', '='] for c in api_key)
        # logger.info(f"API key length: {len(api_key)}")
        # logger.info(f"API key contains valid characters: {is_valid}")
        return is_valid

    @staticmethod
    def _is_valid_base64(s: str) -> bool:
        """Validates if a string contains only valid base64 characters."""
        logger = logging.getLogger(__name__)
        # logger.info("=== Validating base64 string ===")
        try:
            if not s:
                logger.info("String is empty")
                return False
            if len(s) % 4 != 0:
                logger.info(f"Invalid length (not multiple of 4): {len(s)}")
                return False
            # Try to decode the string
            base64.b64decode(s)
            # Check if string contains only valid base64 characters (including URL-safe variants)
            is_valid = all(c.isalnum() or c in ['-', '_', '+', '/', '='] for c in s)
            # logger.info(f"String length: {len(s)}")
            # logger.info(f"Contains valid base64 characters: {is_valid}")
            return is_valid
        except Exception as e:
            logger.error(f"Base64 validation error: {str(e)}")
            return False

    def _validate_auth_keys(self):
        """Validates API key and secret key format."""
        # self._logger.info("=== Validating auth keys ===")
        # Skip validation if both keys are empty (trading not required)
        if not self.api_key and not self.secret_key:
            self._logger.info("Both keys are empty - skipping validation")
            return

        # self._logger.info("Validating API key...")
        if not self._is_valid_api_key(self.api_key):
            self._logger.error("API key validation failed")
            raise KrakenPerpetualAuthError("Invalid API key format", "invalidKey")

        # self._logger.info("Validating secret key...")
        try:
            if not self._is_valid_base64(self.secret_key):
                self._logger.error("Secret key is not valid base64")
                raise ValueError("Invalid base64 characters in secret key")
            base64.b64decode(self.secret_key)
            # self._logger.info("Secret key validation successful")
        except Exception as e:
            self._logger.error(f"Secret key validation error: {str(e)}")
            raise KrakenPerpetualAuthError(f"Secret key must be base64 encoded: {str(e)}", "invalidKey")

    def _get_nonce(self) -> int:
        """
        Generates a nonce value ensuring it's always increasing.
        Uses millisecond timestamp and ensures monotonic increase.
        """
        current_nonce = int(time.time() * 1000)
        if current_nonce <= self._last_nonce:
            current_nonce = self._last_nonce + 1
        self._last_nonce = current_nonce
        return current_nonce

    def _extract_endpoint_path(self, url: str) -> str:
        """
        Extracts the endpoint path from the URL.
        Note: The /derivatives prefix will be removed in the auth string generation.
        Raises ValueError if URL is not a valid Kraken Perpetual URL.
        """
        # self._logger.info(f"Extracting endpoint path from URL: {url}")
        parsed_url = urlparse(url)
        
        # Validate that this is a Kraken Perpetual URL
        valid_domains = [urlparse(base_url).netloc for base_url in CONSTANTS.REST_URLS.values()]
        if parsed_url.netloc not in valid_domains:
            raise ValueError(f"Invalid Kraken Perpetual URL domain: {url}")
            
        path = parsed_url.path
        # self._logger.info(f"Extracted path: {path}")
        
        # Validate that path starts with one of the expected prefixes
        if not (path.startswith("/derivatives/api/v3/") or path.startswith("/api/history/v2/")):
            raise ValueError(f"Invalid Kraken Perpetual API path: {path}")
            
        return path

    def _generate_auth_string(self, post_data: str, nonce: str, endpoint_path: str) -> str:
        """
        Generate authentication string according to Kraken's requirements exactly.
        Following the sample implementation:
        1. Remove /derivatives prefix if present
        2. Concatenate: postData + nonce + endpoint
        3. Hash with SHA256
        4. Base64 decode API secret
        5. Hash result with HMAC-SHA512
        6. Base64 encode final result
        """
        # Remove /derivatives prefix if present
        if endpoint_path.startswith('/derivatives'):
            endpoint_path = endpoint_path[len('/derivatives'):]
            
        # self._logger.info("=== Generating auth string ===")
        # self._logger.info(f"Post data: {post_data}")
        # self._logger.info(f"Nonce: {nonce}")
        # self._logger.info(f"Endpoint path (after prefix removal): {endpoint_path}")

        # Step 1: Concatenate postData + nonce + endpoint
        message = f"{post_data}{nonce}{endpoint_path}"
        # self._logger.info(f"Concatenated message: {message}")

        # Step 2: Hash with SHA256
        sha256_hash = hashlib.sha256()
        sha256_hash.update(message.encode('utf8'))
        hash_digest = sha256_hash.digest()
        # self._logger.info(f"SHA-256 hash (hex): {hash_digest.hex()}")

        # Step 3: Base64 decode API secret
        secret_decoded = base64.b64decode(self.secret_key)
        # self._logger.info(f"Decoded secret length: {len(secret_decoded)}")

        # Step 4: Hash with HMAC-SHA512
        hmac_digest = hmac.new(secret_decoded, hash_digest, hashlib.sha512).digest()
        # self._logger.info(f"HMAC digest (hex): {hmac_digest.hex()}")

        # Step 5: Base64 encode final result
        auth_string = base64.b64encode(hmac_digest).decode('utf-8')
        # self._logger.info(f"Final auth string: {auth_string}")

        return auth_string

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication to the request, required for private API calls.
        Follows Kraken's authentication approach exactly.
        :param request: The request to be authenticated
        """
        self._validate_auth_keys()

        # Extract endpoint path from URL
        endpoint_path = self._extract_endpoint_path(str(request.url))

        # Generate nonce
        nonce = str(self._get_nonce())

        # Prepare post_data based on request type
        post_data = ""
        if request.method in [RESTMethod.POST, RESTMethod.PUT]:
            # For POST/PUT requests, ensure data is properly url-encoded
            if isinstance(request.data, str):
                try:
                    # If it's a JSON string, parse it firs
                    data_dict = json.loads(request.data)
                except json.JSONDecodeError:
                    # If not JSON, assume it's already url-encoded
                    post_data = request.data
                else:
                    # Convert parsed JSON to url-encoded format
                    post_data = urlencode(sorted(data_dict.items()))
            elif isinstance(request.data, dict):
                # If it's a dictionary, convert to url-encoded format
                post_data = urlencode(sorted(request.data.items()))
            else:
                post_data = str(request.data) if request.data else ""
            
            # Update request.data to match the encoded format
            request.data = post_data
            
        elif request.method == RESTMethod.GET and request.params:
            # For GET requests with params, convert to sorted key=value format
            post_data = urlencode(sorted(request.params.items()))

        # Generate auth string
        auth_string = self._generate_auth_string(post_data, nonce, endpoint_path)

        # Set required headers
        content_type = "application/x-www-form-urlencoded" if request.method in [RESTMethod.POST, RESTMethod.PUT] else "application/json"
        request.headers = {
            "Content-Type": content_type,
            "Accept": "application/json",
            "APIKey": self.api_key,
            "Authent": auth_string,
            "Nonce": nonce
        }

        return request

    async def ws_authenticate(self, ws: WSRequest) -> None:
        """
        Authenticates WebSocket connection using challenge-response mechanism.
        The signed challenge will be used in subsequent private feed subscriptions.
        """
        try:
            self._validate_auth_keys()
            
            # First receive the initial info message
            info_response = await ws.receive()
            if not isinstance(info_response.data, dict):
                raise KrakenPerpetualAuthError("Invalid info response format")
            if info_response.data.get("event") != "info":
                raise KrakenPerpetualAuthError(f"Expected info message, got: {info_response.data}")
            
            # Step 1: Request challenge from Kraken futures API
            challenge_request = {
                "event": "challenge",
                "api_key": self.api_key
            }
            self._logger.info(f"Sending challenge request: {challenge_request}")
            await ws.send(WSJSONRequest(payload=challenge_request))

            # Step 2: Wait for challenge response
            challenge_response = await ws.receive()
            if not isinstance(challenge_response.data, dict):
                raise KrakenPerpetualAuthError("Invalid challenge response format")
            if challenge_response.data.get("event") == "error":
                raise KrakenPerpetualAuthError(
                    challenge_response.data.get("message", "Unknown error"),
                    challenge_response.data.get("errorCode")
                )
            if challenge_response.data.get("event") != "challenge":
                raise KrakenPerpetualAuthError(f"Expected challenge message, got: {challenge_response.data}")
            
            challenge = challenge_response.data.get("message")
            if not challenge:
                raise KrakenPerpetualAuthError("No challenge received")

            # Step 3: Sign challenge and store for feed subscriptions
            signed_challenge = self.sign_ws_challenge(challenge)
            # Store challenge details for later subscription payloads
            self._original_challenge = challenge
            self._signed_challenge = signed_challenge
            self._logger.info("Challenge signed and stored for feed subscriptions")

        except KrakenPerpetualAuthError:
            raise
        except Exception as e:
            raise KrakenPerpetualAuthError(f"WebSocket authentication error: {str(e)}")

    def sign_ws_challenge(self, challenge: str) -> str:
        """
        Signs a WebSocket challenge string using the API secret.
        """
        try:
            if not challenge:
                raise KrakenPerpetualAuthError("Empty challenge string", "invalidChallenge")
            
            # Step 1: Hash the challenge using SHA-256.
            sha256_hash = hashlib.sha256()
            sha256_hash.update(challenge.encode("utf-8"))
            hash_digest = sha256_hash.digest()
            
            # Step 2: Base64-decode the API secret.
            decoded_secret = base64.b64decode(self.secret_key)
            
            # Step 3: Create an HMAC-SHA-512 from the sha256 digest using the decoded secret.
            hmac_digest = hmac.new(decoded_secret, hash_digest, hashlib.sha512).digest()
            
            # Step 4: Base64-encode the result and return.
            return base64.b64encode(hmac_digest).decode("utf-8")

        except KrakenPerpetualAuthError:
            raise
        except Exception as e:
            raise KrakenPerpetualAuthError(f"Error signing WebSocket challenge: {str(e)}")

    def get_headers(self) -> Dict[str, str]:
        """
        Generates authentication headers required by Kraken Perpetual REST API
        :return: A dictionary of auth headers
        """
        nonce = str(self._get_nonce())
        return {
            "APIKey": self.api_key,
            "Nonce": nonce
        }

    def get_ws_subscribe_payload(self, feed: str, challenge: str, signed_challenge: str) -> Dict[str, Any]:
        """
        Generates payload for websocket subscription with authentication
        :param feed: The feed to subscribe to
        :param challenge: The original challenge string
        :param signed_challenge: The signed challenge string
        :return: A dictionary with subscription information
        """
        return {
            "event": "subscribe",
            "feed": feed,
            "api_key": self.api_key,
            "original_challenge": challenge,
            "signed_challenge": signed_challenge,
        }

    def get_ws_challenge_payload(self) -> Dict[str, Any]:
        """
        Generates payload for requesting a challenge
        :return: A dictionary with challenge request information
        """
        return {
            "event": "subscribe",
            "feed": "heartbeat",
            "api_key": self.api_key
        }
