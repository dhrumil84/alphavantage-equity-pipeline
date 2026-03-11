import os
import requests
import logging

# Configure a basic logger for this module
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

BASE_URL = "https://www.alphavantage.co/query"

class AlphaVantageError(Exception):
    """Exception raised for API-level errors returned by Alpha Vantage."""
    pass

class AlphaVantageHTTPError(Exception):
    """Exception raised for non-200 HTTP responses."""
    pass

def fetch(params: dict) -> dict:
    """
    Makes a GET request to the Alpha Vantage API.

    Args:
        params: Dictionary of query parameters (e.g., {'function': 'TIME_SERIES_DAILY', 'symbol': 'AAPL'}).
                The 'apikey' parameter will be added automatically from the environment.

    Returns:
        The parsed JSON dictionary response.

    Raises:
        ValueError: If ALPHAVANTAGE_API_KEY is not set in the environment.
        AlphaVantageHTTPError: If the HTTP request fails (non-200 status code).
        AlphaVantageError: If the API returns a 200 response containing an error message or rate limit info.
    """
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY environment variable is not set.")

    # Create a copy so we don't mutate the caller's dict
    req_params = params.copy()
    req_params['apikey'] = api_key

    function_name = req_params.get('function', 'UNKNOWN_FUNCTION')
    symbol = req_params.get('symbol')
    
    log_msg = f"Calling Alpha Vantage: function={function_name}"
    if symbol:
        log_msg += f", symbol={symbol}"
    logger.info(log_msg)

    response = requests.get(BASE_URL, params=req_params)

    # Check for HTTP-level errors
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise AlphaVantageHTTPError(f"HTTP Error {response.status_code}: {response.text}") from e
    except requests.exceptions.RequestException as e:
        raise AlphaVantageHTTPError(f"Request failed: {e}") from e

    # Parse JSON
    try:
        data = response.json()
    except ValueError as e:
        raise AlphaVantageError(f"Failed to parse JSON response: {response.text}") from e

    # Alpha Vantage returns 200 OK even for errors, but embeds them in specific keys.
    # Check for 'Error Message' (e.g., invalid symbol or function)
    if "Error Message" in data:
        raise AlphaVantageError(f"Alpha Vantage API Error: {data['Error Message']}")

    # Check for 'Information' (e.g., standard rate limit hit or premium endpoint error)
    if "Information" in data:
        raise AlphaVantageError(f"Alpha Vantage API Information Message: {data['Information']}")
        
    # Check for standard rate limit string directly, just in case they change the key
    if "Note" in data and "call frequency" in data["Note"]:
         raise AlphaVantageError(f"Alpha Vantage API Rate Limit Note: {data['Note']}")

    return data
