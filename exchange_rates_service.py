import requests
import json
import html
import datetime
import os
from bs4 import BeautifulSoup
import pytz
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import logging
from mongo_repository import MongoRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Source URLs
RAIFFEISEN_URL = "https://raiffeisen.ua/"
PRIVATBANK_URL = "https://api.privatbank.ua/p24api/pubinfo?exchange&json&coursid=11"
BESTOBMIN_URL = "https://bestobmin.com.ua"

# MongoDB configuration from environment variables
MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME = os.getenv("DB_NAME", "currency_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "exchange_rates")

# Initialize MongoDB repository
mongo_repo = MongoRepository(MONGO_URI, DB_NAME, COLLECTION_NAME)

# Type definitions for better code readability
RateData = Dict[str, Any]
NormalizedRates = List[Dict[str, Any]]


def fetch_url_content(url: str, timeout: int = 10) -> Optional[requests.Response]:
    """
    Fetch content from a given URL with error handling.

    Args:
        url: The URL to fetch
        timeout: Request timeout in seconds

    Returns:
        Response object or None if the request failed
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"Error fetching URL {url}: {e}")
        return None


def parse_raiffeisen_exchange_rates(html_text: str) -> List[Dict[str, Any]]:
    """
    Parse exchange rates from Raiffeisen Bank's HTML.

    Args:
        html_text: HTML content from Raiffeisen website

    Returns:
        List of currency exchange rate dictionaries
    """
    soup = BeautifulSoup(html_text, 'html.parser')
    div_currency = soup.find('div', id="currency-table")
    if not div_currency:
        logger.warning("Block with id 'currency-table' not found.")
        return []

    currency_elem = div_currency.find('currency-table')
    if not currency_elem:
        logger.warning("Element <currency-table> not found.")
        return []

    currencies_attr = currency_elem.get(':currencies')
    if not currencies_attr:
        logger.warning("Attribute ':currencies' not found.")
        return []

    currencies_json_str = html.unescape(currencies_attr)
    try:
        return json.loads(currencies_json_str)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from Raiffeisen: {e}")
        return []


def fetch_raiffeisen_exchange_rates() -> List[Dict[str, Any]]:
    """
    Fetch and parse exchange rates from Raiffeisen Bank.

    Returns:
        List of currency exchange rate dictionaries
    """
    response = fetch_url_content(RAIFFEISEN_URL)
    if response:
        return parse_raiffeisen_exchange_rates(response.text)
    return []


def fetch_privatbank_exchange_rates() -> List[Dict[str, Any]]:
    """
    Fetch exchange rates from PrivatBank's API.

    Returns:
        List of currency exchange rate dictionaries
    """
    response = fetch_url_content(PRIVATBANK_URL)
    if response:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error from PrivatBank: {e}")
    return []


def parse_bestobmin_exchange_rates(html_text: str) -> List[Dict[str, Any]]:
    """
    Parse exchange rates from Bestobmin's HTML.

    Args:
        html_text: HTML content from Bestobmin website

    Returns:
        List of currency exchange rate dictionaries
    """
    soup = BeautifulSoup(html_text, 'html.parser')
    container = soup.find('div', id="opt")
    if not container:
        logger.warning("Container with id 'opt' not found on Bestobmin.")
        return []

    rows = container.find_all("div", class_="row")
    rates = []

    for row in rows:
        try:
            left_div = row.find("div", class_="digit_bg left_digit_bg")
            if not left_div:
                continue

            buy_p = left_div.find("p")
            if not buy_p:
                continue

            rate_buy = buy_p.get_text(strip=True)

            currency_elem = row.find("p", class_="currency")
            if not currency_elem:
                continue

            currency = currency_elem.get_text(strip=True)

            right_div = row.find("div", class_="digit_bg right_digit_bg")
            if not right_div:
                continue

            sell_p = right_div.find("p")
            if not sell_p:
                continue

            rate_sell = sell_p.get_text(strip=True)

            rates.append({
                "currency": currency,
                "base_currency": "UAH",
                "rate_buy": rate_buy,
                "rate_sell": rate_sell
            })
        except Exception as e:
            logger.error(f"Error parsing Bestobmin row: {e}")
            continue

    return rates


def fetch_bestobmin_exchange_rates() -> List[Dict[str, Any]]:
    """
    Fetch and parse exchange rates from Bestobmin.

    Returns:
        List of currency exchange rate dictionaries
    """
    response = fetch_url_content(BESTOBMIN_URL)
    if response:
        return parse_bestobmin_exchange_rates(response.text)
    return []


def normalize_raiffeisen_rates(raw_rates: List[Dict[str, Any]]) -> NormalizedRates:
    """
    Normalize Raiffeisen Bank exchange rate data format.

    Args:
        raw_rates: Raw data from Raiffeisen

    Returns:
        Normalized list of exchange rate dictionaries
    """
    normalized = []
    for item in raw_rates:
        normalized.append({
            "currency": item.get("currency"),
            "base_currency": "UAH",
            "rate_buy": item.get("rate_buy"),
            "rate_sell": item.get("rate_sell")
        })
    return normalized


def normalize_privatbank_rates(raw_rates: List[Dict[str, Any]]) -> NormalizedRates:
    """
    Normalize PrivatBank exchange rate data format.

    Args:
        raw_rates: Raw data from PrivatBank

    Returns:
        Normalized list of exchange rate dictionaries
    """
    normalized = []
    for item in raw_rates:
        normalized.append({
            "currency": item.get("ccy"),
            "base_currency": item.get("base_ccy"),
            "rate_buy": item.get("buy"),
            "rate_sell": item.get("sale")
        })
    return normalized


def update_exchange_rates() -> Dict[str, Any]:
    """
    Update exchange rates from all sources and save to database.

    Returns:
        Dictionary with normalized data from all sources
    """
    try:
        tz = pytz.timezone("Europe/Kiev")
        current_timestamp = datetime.datetime.now(tz).isoformat()

        raw_raiffeisen = fetch_raiffeisen_exchange_rates()
        raw_privatbank = fetch_privatbank_exchange_rates()
        raw_bestobmin = fetch_bestobmin_exchange_rates()

        normalized_data = {
            "Raiffeisen": normalize_raiffeisen_rates(raw_raiffeisen),
            "PrivatBank": normalize_privatbank_rates(raw_privatbank),
            "Bestobmin": raw_bestobmin
        }

        # Use the repository to save data
        success = mongo_repo.save_exchange_rates(current_timestamp, normalized_data)
        if success:
            logger.info(f"Exchange rates updated at {current_timestamp}")
        else:
            logger.warning("Failed to save exchange rates to database")

        return {
            "timestamp": current_timestamp,
            "data": normalized_data
        }
    except Exception as e:
        logger.error(f"Error updating exchange rates: {e}")
        return {
            "timestamp": datetime.datetime.now(pytz.timezone("Europe/Kiev")).isoformat(),
            "error": str(e),
            "data": {}
        }


def get_exchange_rates_by_currency(selected_currency: str) -> Dict[str, Any]:
    """
    Get the latest exchange rates for a specific currency.

    Args:
        selected_currency: Currency code (e.g., 'USD', 'EUR')

    Returns:
        Dictionary with the latest exchange rates for the selected currency
    """
    return mongo_repo.get_latest_exchange_rates(selected_currency)


def get_exchange_rates_for_period(selected_currency: str, period_days: int) -> Dict[str, Any]:
    """
    Get exchange rates for a specific currency over a period of time.

    Args:
        selected_currency: Currency code (e.g., 'USD', 'EUR')
        period_days: Number of days to look back

    Returns:
        Dictionary with exchange rate data over the specified period
    """
    tz = pytz.timezone("Europe/Kiev")
    current_datetime = datetime.datetime.now(tz)
    cutoff_datetime = current_datetime - datetime.timedelta(days=period_days)
    cutoff_iso = cutoff_datetime.isoformat()

    return mongo_repo.get_exchange_rates_for_period(selected_currency, period_days, cutoff_iso)
