import requests
import json
import html
import datetime
import os
from bs4 import BeautifulSoup
from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pytz
import sys
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import logging

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


def get_mongodb_client() -> MongoClient:
    """
    Get a MongoDB client connection.

    Returns:
        MongoDB client instance

    Raises:
        SystemExit: If the MongoDB URI is invalid or connection fails
    """
    if not MONGO_URI:
        logger.error("MongoDB URI is not set. Please check environment variables.")
        sys.exit(1)

    try:
        return MongoClient(MONGO_URI, server_api=ServerApi('1'))
    except errors.ConfigurationError as e:
        logger.error(f"MongoDB configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        sys.exit(1)


def save_to_mongodb(timestamp: str, normalized_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Save exchange rate data to MongoDB.

    Args:
        timestamp: ISO format timestamp
        normalized_data: Dictionary with normalized exchange rate data
    """
    client = None
    try:
        client = get_mongodb_client()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        document = {
            "timestamp": timestamp,
            "Raiffeisen": normalized_data.get("Raiffeisen", []),
            "PrivatBank": normalized_data.get("PrivatBank", []),
            "Bestobmin": normalized_data.get("Bestobmin", [])
        }

        result = collection.insert_one(document)
        logger.info(f"Data saved to MongoDB with document id: {result.inserted_id}")
    except Exception as e:
        logger.error(f"Error saving data to MongoDB: {e}")
    finally:
        if client:
            client.close()


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

        save_to_mongodb(current_timestamp, normalized_data)
        logger.info(f"Exchange rates updated at {current_timestamp}")

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
    client = None
    try:
        client = get_mongodb_client()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        latest_doc = collection.find_one(sort=[("timestamp", -1)])
        if not latest_doc:
            logger.warning(f"No exchange rate data found for {selected_currency}")
            return {}

        filtered_data = {"timestamp": latest_doc.get("timestamp")}
        for bank in ["Raiffeisen", "PrivatBank", "Bestobmin"]:
            bank_rates = latest_doc.get(bank, [])
            filtered_data[bank] = [rate for rate in bank_rates if rate.get("currency") == selected_currency]

        return filtered_data
    except Exception as e:
        logger.error(f"Error retrieving exchange rates for {selected_currency}: {e}")
        return {}
    finally:
        if client:
            client.close()


def get_exchange_rates_for_period(selected_currency: str, period_days: int) -> Dict[str, Any]:
    """
    Get exchange rates for a specific currency over a period of time.

    Args:
        selected_currency: Currency code (e.g., 'USD', 'EUR')
        period_days: Number of days to look back

    Returns:
        Dictionary with exchange rate data over the specified period
    """
    client = None
    try:
        tz = pytz.timezone("Europe/Kiev")
        cutoff_datetime = datetime.datetime.now(tz) - datetime.timedelta(days=period_days)
        cutoff_iso = cutoff_datetime.isoformat()

        client = get_mongodb_client()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        docs = list(collection.find({"timestamp": {"$gte": cutoff_iso}}).sort("timestamp", 1))

        result = {
            "currency": selected_currency,
            "period_days": period_days,
            "data": []
        }

        for doc in docs:
            timestamp = doc.get("timestamp")
            banks_data = {}

            for bank in ["Raiffeisen", "PrivatBank", "Bestobmin"]:
                bank_rates = doc.get(bank, [])
                filtered_rates = [rate for rate in bank_rates if rate.get("currency") == selected_currency]
                if filtered_rates:
                    banks_data[bank] = filtered_rates

            if banks_data:
                result["data"].append({
                    "timestamp": timestamp,
                    "rates": banks_data
                })

        return result
    except Exception as e:
        logger.error(f"Error retrieving exchange rates for period {period_days} days: {e}")
        return {
            "currency": selected_currency,
            "period_days": period_days,
            "error": str(e),
            "data": []
        }
    finally:
        if client:
            client.close()