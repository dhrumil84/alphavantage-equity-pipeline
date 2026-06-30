import os
import time
import random
import requests
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# Configure a basic logger for this module
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

BASE_URL = "https://www.alphavantage.co/query"

# (connect, read) timeout in seconds. Read timeout is generous because some AV
# endpoints (e.g. full TIME_SERIES_DAILY_ADJUSTED) can take 20s+ to stream.
_HTTP_TIMEOUT = (5, 30)
# Total attempts = 1 initial + 4 retries. With base 1.5s doubling, the worst-case
# total backoff is ~22s of sleep across 5 attempts.
_MAX_ATTEMPTS = 5
_BACKOFF_BASE = 1.5

# Lightweight in-process API call counter. Observability hooks read this at
# process exit to record per-run API usage. Keyed by the `function` param
# (e.g. 'OVERVIEW', 'TIME_SERIES_DAILY_ADJUSTED'). Errors are tracked separately.
# Note: each retry attempt is counted as a separate call, since each one consumes
# an AV rate-limit slot.
_CALL_COUNTS: dict[str, int] = {}
_ERROR_COUNTS: dict[str, int] = {}

def _record_call(function_name: str) -> None:
    _CALL_COUNTS[function_name] = _CALL_COUNTS.get(function_name, 0) + 1

def _record_error(function_name: str) -> None:
    _ERROR_COUNTS[function_name] = _ERROR_COUNTS.get(function_name, 0) + 1

def get_call_stats() -> dict:
    """Return a snapshot of API call counts since process start."""
    return {
        "calls_by_function": dict(_CALL_COUNTS),
        "errors_by_function": dict(_ERROR_COUNTS),
        "total_calls": sum(_CALL_COUNTS.values()),
        "total_errors": sum(_ERROR_COUNTS.values()),
    }

class AlphaVantageError(Exception):
    """Exception raised for API-level errors returned by Alpha Vantage."""
    pass

class AlphaVantageHTTPError(Exception):
    """Exception raised for non-200 HTTP responses."""
    pass


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter. `attempt` is 0-indexed."""
    return _BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1)


def _http_get_with_retry(req_params: dict, function_name: str) -> requests.Response:
    """
    GET BASE_URL with bounded timeout and exponential backoff on transient failures.

    Retries on connection errors, read timeouts, and 5xx server responses.
    Does NOT retry on 4xx — those are deterministic client errors that won't
    resolve with another attempt.

    Raises AlphaVantageHTTPError if all attempts are exhausted.
    """
    last_error: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        _record_call(function_name)
        try:
            response = requests.get(BASE_URL, params=req_params, timeout=_HTTP_TIMEOUT)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            _record_error(function_name)
            last_error = e
            if attempt + 1 < _MAX_ATTEMPTS:
                delay = _backoff_delay(attempt)
                logger.warning(
                    f"Transient HTTP error on {function_name} "
                    f"(attempt {attempt + 1}/{_MAX_ATTEMPTS}): "
                    f"{type(e).__name__}: {e} — retrying in {delay:.1f}s"
                )
                time.sleep(delay)
                continue
            raise AlphaVantageHTTPError(
                f"Request failed after {_MAX_ATTEMPTS} attempts: {e}"
            ) from e
        except requests.exceptions.RequestException as e:
            _record_error(function_name)
            raise AlphaVantageHTTPError(f"Request failed: {e}") from e

        if 500 <= response.status_code < 600:
            _record_error(function_name)
            last_error = AlphaVantageHTTPError(
                f"HTTP {response.status_code}: {response.text[:200]}"
            )
            if attempt + 1 < _MAX_ATTEMPTS:
                delay = _backoff_delay(attempt)
                logger.warning(
                    f"Server error on {function_name} "
                    f"(attempt {attempt + 1}/{_MAX_ATTEMPTS}): "
                    f"HTTP {response.status_code} — retrying in {delay:.1f}s"
                )
                time.sleep(delay)
                continue
            raise last_error

        return response

    # Defensive — loop always returns or raises above.
    raise AlphaVantageHTTPError(
        f"Exhausted retries for {function_name}: {last_error}"
    )


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
        AlphaVantageHTTPError: If the HTTP request fails (non-200 status code, or all retries exhausted).
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

    response = _http_get_with_retry(req_params, function_name)

    # 5xx is handled inside the retry helper; this catches 4xx.
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        _record_error(function_name)
        raise AlphaVantageHTTPError(f"HTTP Error {response.status_code}: {response.text}") from e

    # Parse JSON
    try:
        data = response.json()
    except ValueError as e:
        _record_error(function_name)
        raise AlphaVantageError(f"Failed to parse JSON response: {response.text}") from e

    # Alpha Vantage returns 200 OK even for errors, but embeds them in specific keys.
    # Check for 'Error Message' (e.g., invalid symbol or function)
    if "Error Message" in data:
        _record_error(function_name)
        raise AlphaVantageError(f"Alpha Vantage API Error: {data['Error Message']}")

    # Check for 'Information' (e.g., standard rate limit hit or premium endpoint error)
    if "Information" in data:
        _record_error(function_name)
        raise AlphaVantageError(f"Alpha Vantage API Information Message: {data['Information']}")

    # Check for standard rate limit string directly, just in case they change the key
    if "Note" in data and "call frequency" in data["Note"]:
         _record_error(function_name)
         raise AlphaVantageError(f"Alpha Vantage API Rate Limit Note: {data['Note']}")

    return data

def fetch_csv(params: dict) -> bytes:
    """
    Makes a GET request to the Alpha Vantage API for endpoints returning CSV data.

    Args:
        params: Dictionary of query parameters. 'apikey' will be added automatically.

    Returns:
        The raw bytes of the CSV response.

    Raises:
        ValueError: If ALPHAVANTAGE_API_KEY is not set.
        AlphaVantageHTTPError: If the HTTP request fails (or all retries exhausted).
        AlphaVantageError: If the API returns a JSON error message instead of CSV.
    """
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY environment variable is not set.")

    req_params = params.copy()
    req_params['apikey'] = api_key

    function_name = req_params.get('function', 'UNKNOWN_FUNCTION')
    symbol = req_params.get('symbol')

    log_msg = f"Calling Alpha Vantage (CSV): function={function_name}"
    if symbol:
        log_msg += f", symbol={symbol}"
    logger.info(log_msg)

    response = _http_get_with_retry(req_params, function_name)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        _record_error(function_name)
        raise AlphaVantageHTTPError(f"HTTP Error {response.status_code}: {response.text}") from e

    content_type = response.headers.get("Content-Type", "")

    # If returned as JSON, it's typically an error or rate limit hit.
    if "application/json" in content_type:
        _record_error(function_name)
        try:
            data = response.json()
            if "Error Message" in data:
                raise AlphaVantageError(f"Alpha Vantage API Error: {data['Error Message']}")
            if "Information" in data:
                raise AlphaVantageError(f"Alpha Vantage API Information Message: {data['Information']}")
            if "Note" in data and "call frequency" in data["Note"]:
                raise AlphaVantageError(f"Alpha Vantage API Rate Limit Note: {data['Note']}")

            raise AlphaVantageError(f"Unexpected JSON response for a CSV endpoint: {data}")
        except ValueError as e:
            raise AlphaVantageError(f"Failed to parse unexpected JSON response: {response.text}") from e

    return response.content


if __name__ == '__main__':
    import json
    result = fetch({'function': 'OVERVIEW', 'symbol': 'IBM'})
    print(json.dumps(result, indent=2))
