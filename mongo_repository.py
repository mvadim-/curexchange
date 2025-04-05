import sys
import logging
import datetime
from typing import Dict, List, Any
from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class MongoRepository:
    """Repository class for MongoDB operations related to exchange rates."""

    def __init__(self, uri: str, db_name: str, collection_name: str):
        """
        Initialize the MongoDB repository.

        Args:
            uri: MongoDB connection URI
            db_name: Database name
            collection_name: Collection name
        """
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name

    def _get_client(self) -> MongoClient:
        """
        Get a MongoDB client connection.

        Returns:
            MongoDB client instance

        Raises:
            SystemExit: If the MongoDB URI is invalid or connection fails
        """
        if not self.uri:
            logger.error("MongoDB URI is not set.")
            sys.exit(1)

        try:
            return MongoClient(self.uri, server_api=ServerApi('1'))
        except errors.ConfigurationError as e:
            logger.error(f"MongoDB configuration error: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            sys.exit(1)

    def _get_collection(self) -> tuple[MongoClient, Collection]:
        """
        Get the MongoDB collection to operate on.

        Returns:
            A tuple containing the MongoDB client and collection

        Raises:
            SystemExit: If connection fails
        """
        client = self._get_client()
        collection = client[self.db_name][self.collection_name]
        return client, collection

    def save_exchange_rates(self, timestamp: str, normalized_data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Save exchange rate data to MongoDB.

        Args:
            timestamp: ISO format timestamp
            normalized_data: Dictionary with normalized exchange rate data

        Returns:
            True if saved successfully, False otherwise
        """
        client = None
        try:
            client, collection = self._get_collection()

            document = {
                "timestamp": timestamp,
                "Raiffeisen": normalized_data.get("Raiffeisen", []),
                "PrivatBank": normalized_data.get("PrivatBank", []),
                "Bestobmin": normalized_data.get("Bestobmin", [])
            }

            result = collection.insert_one(document)
            logger.info(f"Data saved to MongoDB with document id: {result.inserted_id}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to MongoDB: {e}")
            return False
        finally:
            if client:
                client.close()

    def get_latest_exchange_rates(self, selected_currency: str) -> Dict[str, Any]:
        """
        Get the latest exchange rates for a specific currency.

        Args:
            selected_currency: Currency code (e.g., 'USD', 'EUR')

        Returns:
            Dictionary with the latest exchange rates for the selected currency
        """
        client = None
        try:
            client, collection = self._get_collection()

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

    def get_exchange_rates_for_period(self, selected_currency: str, period_days: int,
                                      cutoff_iso: str) -> Dict[str, Any]:
        """
        Get exchange rates for a specific currency over a period of time.

        Args:
            selected_currency: Currency code (e.g., 'USD', 'EUR')
            period_days: Number of days to look back
            cutoff_iso: ISO format date string for the cutoff date

        Returns:
            Dictionary with exchange rate data over the specified period
        """
        client = None
        try:
            client, collection = self._get_collection()

            # Get all documents for the period
            docs = list(collection.find({"timestamp": {"$gte": cutoff_iso}}).sort("timestamp", 1))

            result = {
                "currency": selected_currency,
                "period_days": period_days,
                "data": []
            }

            # Different sampling strategies based on period_days
            filtered_docs = self._filter_docs_by_period(docs, period_days)

            # Process the filtered documents
            for doc in filtered_docs:
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

    @staticmethod
    def _filter_docs_by_period(docs: List[Dict[str, Any]], period_days: int) -> List[Dict[str, Any]]:
        """
        Filter documents based on the period strategy.

        Args:
            docs: List of documents from MongoDB
            period_days: Number of days to look back

        Returns:
            Filtered list of documents
        """
        if period_days == 1:
            # For 1-day period: return rates during working hours (8:00 to 20:00) once per hour
            # Group documents by hour
            hour_groups = {}
            for doc in docs:
                timestamp = doc.get("timestamp")
                doc_datetime = datetime.datetime.fromisoformat(timestamp)

                # Check if it's within working hours (8:00 - 20:00)
                if 8 <= doc_datetime.hour < 20:
                    # Key by hour to get one sample per hour
                    hour_key = doc_datetime.strftime("%Y-%m-%d %H")

                    # Keep only the first document for each hour
                    if hour_key not in hour_groups:
                        hour_groups[hour_key] = doc

            # Use these documents for the result
            filtered_docs = list(hour_groups.values())
            filtered_docs.sort(key=lambda x: x.get("timestamp"))  # Ensure chronological order
        else:
            # For other periods: one sample per day (closest to 18:00)
            # Group documents by day
            day_groups = {}
            for doc in docs:
                timestamp = doc.get("timestamp")
                doc_datetime = datetime.datetime.fromisoformat(timestamp)
                day_key = doc_datetime.strftime("%Y-%m-%d")

                # For each day, find the document closest to 18:00
                target_time = 18  # 18:00

                if day_key not in day_groups:
                    day_groups[day_key] = {"doc": doc, "time_diff": abs(doc_datetime.hour - target_time)}
                else:
                    time_diff = abs(doc_datetime.hour - target_time)
                    if time_diff < day_groups[day_key]["time_diff"]:
                        day_groups[day_key] = {"doc": doc, "time_diff": time_diff}

            # Extract just the documents
            filtered_docs = [item["doc"] for item in day_groups.values()]
            filtered_docs.sort(key=lambda x: x.get("timestamp"))  # Ensure chronological order

        return filtered_docs
