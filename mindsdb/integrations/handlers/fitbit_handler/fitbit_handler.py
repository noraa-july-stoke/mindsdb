import re
import os
import datetime as dt
import ast
import time
from collections import defaultdict
import pytz
import io
import requests

# import fitbit
import pandas as pd
from collections import defaultdict
from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

class FitbitHandler:

    def __init__(self, client_id, client_secret, access_token, refresh_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.api = None
        self.is_connected = False

    def connect(self):
        if self.is_connected:
            return self.api

        self.api = fitbit.Fitbit(
            self.client_id,
            self.client_secret,
            access_token=self.access_token,
            refresh_token=self.refresh_token,
        )

        self.is_connected = True
        return self.api

    def check_connection(self):
        response = {"success": False}

        try:
            api = self.connect()

            # Call any method that requires authentication to test the connection
            api.get_devices()

            response["success"] = True

        except Exception as e:
            response["error_message"] = f"Error connecting to Fitbit API: {e}. Check your credentials."

        if response["success"] is False and self.is_connected is True:
            self.is_connected = False

        return response

    def call_fitbit_api(self, method_name: str = None, params: Dict = None):

        api = self.connect()
        method = getattr(api, method_name)

        data = []
        includes = defaultdict(list)

        limit_exec_time = time.time() + 60

        try:
            resp = method(**params)

            if isinstance(resp, list):
                data.extend(resp)
            else:
                data.append(resp)

        except Exception as e:
            raise RuntimeError(f"Error calling Fitbit API: {e}")

        return pd.DataFrame(data)

# Usage example:

client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"
access_token = "YOUR_ACCESS_TOKEN"
refresh_token = "YOUR_REFRESH_TOKEN"

fitbit_handler = FitbitHandler(client_id, client_secret, access_token, refresh_token)

# Test the connection
print(fitbit_handler.check_connection())

# Example API call
df = fitbit_handler.call_fitbit_api(method_name="time_series", params={"resource": "activities/steps", "base_date": "2023-04-01", "end_date": "2023-04-30"})
print(df)
