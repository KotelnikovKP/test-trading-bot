"""
API connector for Bybit HTTP API v.5
Idea by Official Python3 API connector for Bybit HTTP and WebSockets APIs https://github.com/bybit-exchange/pybit
There are code refactoring only with functionality of preparing request for future using in external async code
"""

import hashlib
import hmac
import json
import time

from app.config import BYBIT_API_KEY, BYBIT_API_SECRET

HTTP_URL = "https://{SUBDOMAIN}.{DOMAIN}.com"
SUBDOMAIN_TESTNET = "api-testnet"
SUBDOMAIN_MAINNET = "api"
DOMAIN = "bybit"


class Bybit:
    __object = None

    def __new__(cls, *args, **kwargs):
        if cls.__object is None:
            cls.__object = super().__new__(cls)
        return cls.__object

    def __init__(self,
                 testnet: bool = False,
                 api_key: str = None,
                 api_secret: str = None,
                 recv_window: int = 5000):

        if hasattr(self, 'testnet'):
            return

        self.testnet = testnet
        self.api_key = api_key
        self.api_secret = api_secret
        self.recv_window = recv_window

        subdomain = SUBDOMAIN_TESTNET if self.testnet else SUBDOMAIN_MAINNET
        self.endpoint = HTTP_URL.format(SUBDOMAIN=subdomain, DOMAIN=DOMAIN)

        if not self.api_key:
            self.api_key = BYBIT_API_KEY

        if not self.api_secret:
            self.api_secret = BYBIT_API_SECRET

    @staticmethod
    def prepare_payload(method, parameters):
        """
        Prepares the request payload and validates parameter value types.
        """

        def cast_values():
            string_params = [
                "qty",
                "price",
                "triggerPrice",
                "takeProfit",
                "stopLoss",
            ]
            integer_params = [
                "positionIdx"
            ]

            for key, value in parameters.items():
                if key in string_params:
                    if type(value) != str:
                        parameters[key] = str(value)
                elif key in integer_params:
                    if type(value) != int:
                        parameters[key] = int(value)

        if method == "GET":
            payload = "&".join(
                [
                    str(k) + "=" + str(v)
                    for k, v in sorted(parameters.items())
                    if v is not None
                ]
            )
            return payload
        else:
            cast_values()
            return json.dumps(parameters)

    def _auth(self, payload, recv_window, timestamp):
        """
        Generates authentication signature per Bybit API specifications.
        """

        api_key = self.api_key
        api_secret = self.api_secret

        if api_key is None or api_secret is None:
            raise PermissionError("Authenticated endpoints require keys.")

        param_str = str(timestamp) + api_key + str(recv_window) + payload
        auth_hash = hmac.new(
            bytes(api_secret, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        )
        return auth_hash.hexdigest()

    def _prepare_request(self, method=None, path=None, query=None, auth=False):
        """
        Prepare the request to the API.
        Notes
        -------------------
        We use the params argument for the GET method, and data argument for
        the POST method. Dicts passed to the data argument must be
        JSONified prior to submitting request.
        """

        if query is None:
            query = {}

        # Change floating whole numbers to integers to prevent auth signature errors.
        if query is not None:
            for i in query.keys():
                if isinstance(query[i], float) and query[i] == int(query[i]):
                    query[i] = int(query[i])

        req_params = self.prepare_payload(method, query)

        # Authenticate if we are using a private endpoint.
        if auth:
            # Prepare signature.
            timestamp = int(time.time() * 10 ** 3)
            signature = self._auth(
                payload=req_params,
                recv_window=self.recv_window,
                timestamp=timestamp,
            )
            headers = {
                "Content-Type": "application/json",
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-RECV-WINDOW": str(self.recv_window),
            }
        else:
            headers = {}

        if method == "GET":
            url = path + f'?{req_params}' if req_params else path
            data = None
        else:
            data = req_params
            url = path

        return method, url, data, headers

    def get_kline(self, **kwargs):
        """
        Query the kline data. Charts are returned in groups based on the requested interval
        Required args:
            category (string): Product type: spot, linear, inverse
            symbol (string): Symbol name
            interval (string): Kline interval.
        Returns parameters for request:
            method (string): request method: GET, POST
            url (string): endpoint
            data (dict): parameters
            headers (dict): request headers
        Additional information:
            https://bybit-exchange.github.io/docs/v5/market/kline
        """
        return self._prepare_request(
            method='GET',
            path=f'{self.endpoint}/v5/market/kline',
            query=kwargs,
            auth=False,
        )

    def get_instruments_info(self, **kwargs):
        """
        Query a list of instruments of online trading pair
        Required args:
            category (string): Product type: spot, linear, inverse, option
        Returns parameters for request:
            method (string): request method: GET, POST
            url (string): endpoint
            data (dict): parameters
            headers (dict): request headers
        Additional information:
            https://bybit-exchange.github.io/docs/v5/market/instrument
        """
        return self._prepare_request(
            method='GET',
            path=f'{self.endpoint}/v5/market/instruments-info',
            query=kwargs,
            auth=False,
        )

    def get_public_trade_history(self, **kwargs):
        """
        Query recent public trading data in Bybit.
        Required args:
            category (string): Product type: spot, linear, inverse, option
            symbol (string): Symbol name
        Returns parameters for request:
            method (string): request method: GET, POST
            url (string): endpoint
            data (dict): parameters
            headers (dict): request headers
        Additional information:
            https://bybit-exchange.github.io/docs/v5/market/recent-trade
        """
        return self._prepare_request(
            method='GET',
            path=f'{self.endpoint}/v5/market/recent-trade',
            query=kwargs,
            auth=False,
        )
