import requests
import json
import html
import datetime
from bs4 import BeautifulSoup
from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pytz
import sys

# Source URLs
RAIFFEISEN_URL = "https://raiffeisen.ua/"
PRIVATBANK_URL = "https://api.privatbank.ua/p24api/pubinfo?exchange&json&coursid=11"
BESTOBMIN_URL = "https://bestobmin.com.ua"

# MongoDB configuration
MONGO_URI = "mongodb+srv://vpm2000:RKyaR4V38M3C5sWP@cluster0.pel3e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "currency_db"
COLLECTION_NAME = "exchange_rates"


def fetch_url_content(url, timeout=10):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None


def parse_raiffeisen_exchange_rates(html_text):
    soup = BeautifulSoup(html_text, 'html.parser')
    div_currency = soup.find('div', id="currency-table")
    if not div_currency:
        print("Block with id 'currency-table' not found.")
        return []
    currency_elem = div_currency.find('currency-table')
    if not currency_elem:
        print("Element <currency-table> not found.")
        return []
    currencies_attr = currency_elem.get(':currencies')
    if not currencies_attr:
        print("Attribute ':currencies' not found.")
        return []
    currencies_json_str = html.unescape(currencies_attr)
    try:
        return json.loads(currencies_json_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []


def fetch_raiffeisen_exchange_rates():
    response = fetch_url_content(RAIFFEISEN_URL)
    if response:
        return parse_raiffeisen_exchange_rates(response.text)
    return []


def fetch_privatbank_exchange_rates():
    response = fetch_url_content(PRIVATBANK_URL)
    if response:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            print(f"JSON decoding error from PrivatBank: {e}")
    return []


def parse_bestobmin_exchange_rates(html_text):
    soup = BeautifulSoup(html_text, 'html.parser')
    container = soup.find('div', id="opt")
    if not container:
        print("Container with id 'opt' not found on Bestobmin.")
        return []
    rows = container.find_all("div", class_="row")
    rates = []
    for row in rows:
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
    return rates


def fetch_bestobmin_exchange_rates():
    response = fetch_url_content(BESTOBMIN_URL)
    if response:
        return parse_bestobmin_exchange_rates(response.text)
    return []


def normalize_raiffeisen_rates(raw_rates):
    normalized = []
    for item in raw_rates:
        normalized.append({
            "currency": item.get("currency"),
            "base_currency": "UAH",
            "rate_buy": item.get("rate_buy"),
            "rate_sell": item.get("rate_sell")
        })
    return normalized


def normalize_privatbank_rates(raw_rates):
    normalized = []
    for item in raw_rates:
        normalized.append({
            "currency": item.get("ccy"),
            "base_currency": item.get("base_ccy"),
            "rate_buy": item.get("buy"),
            "rate_sell": item.get("sale")
        })
    return normalized


def save_to_mongodb(timestamp, normalized_data):
    try:
        client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
    except errors.ConfigurationError:
        print(f"An Invalid URI host error was received. Is your Atlas host name correct in your connection string?")
        sys.exit(1)

    document = {
        "timestamp": timestamp,
        "Raiffeisen": normalized_data.get("Raiffeisen", []),
        "PrivatBank": normalized_data.get("PrivatBank", []),
        "Bestobmin": normalized_data.get("Bestobmin", [])
    }

    result = collection.insert_one(document)
    print(f"Data saved to MongoDB with document id: {result.inserted_id}")
    client.close()


def update_exchange_rates():
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
    print(f"Data updated at {current_timestamp}")


def get_exchange_rates_by_currency(selected_currency: str) -> dict:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    latest_doc = collection.find_one(sort=[("timestamp", -1)])
    client.close()
    if not latest_doc:
        return {}
    filtered_data = {"timestamp": latest_doc.get("timestamp")}
    for bank in ["Raiffeisen", "PrivatBank", "Bestobmin"]:
        bank_rates = latest_doc.get(bank, [])
        filtered_data[bank] = [rate for rate in bank_rates if rate.get("currency") == selected_currency]
    return filtered_data


def get_exchange_rates_for_period(selected_currency: str, period_days: int) -> dict:
    tz = pytz.timezone("Europe/Kiev")
    cutoff_datetime = datetime.datetime.now(tz) - datetime.timedelta(days=period_days)
    cutoff_iso = cutoff_datetime.isoformat()
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    docs = list(collection.find({"timestamp": {"$gte": cutoff_iso}}).sort("timestamp", 1))
    client.close()
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
