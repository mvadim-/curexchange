import os
import logging
import atexit
import pytz
from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from apscheduler.schedulers.background import BackgroundScheduler
from exchange_rates_service import (
    update_exchange_rates,
    get_exchange_rates_by_currency,
    get_exchange_rates_for_period
)
from dotenv import load_dotenv
from flask_talisman import Talisman

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Create Flask app
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False  # Preserve key order in JSON responses
app.debug = os.getenv("DEBUG", "False").lower() in ('true', '1', 't')

# Setup authentication
auth = HTTPBasicAuth()

# Setup security
# In development
if app.debug:
    Talisman(app, content_security_policy=None)
else:
    # Production CSP
    Talisman(app, content_security_policy={'default-src': ['\'self\'']})

# Get credentials from environment variables
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
USER_PASSWORD = os.getenv("USER_PASSWORD")

if not ADMIN_PASSWORD or not USER_PASSWORD:
    logger.error("Missing required environment variables: ADMIN_PASSWORD and/or USER_PASSWORD")
    raise ValueError("Admin and user passwords must be set in environment variables")

# Create users dictionary with hashed passwords
users = {
    "admin": generate_password_hash(ADMIN_PASSWORD, method='pbkdf2:sha256'),
    "user": generate_password_hash(USER_PASSWORD, method='pbkdf2:sha256')
}


@auth.verify_password
def verify_password(username, password):
    """Verify username and password for HTTP Basic Auth"""
    if username in users and check_password_hash(users.get(username), password):
        return username
    return None


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint that doesn't require authentication"""
    return jsonify({"status": "ok"})


@app.route('/api/exchange_rates', methods=['GET'])
@auth.login_required
def api_get_exchange_rates():
    """
    API endpoint to get the latest exchange rates for a selected currency.

    Query parameters:
        currency: The currency code (e.g., 'USD', 'EUR')
    """
    selected_currency = request.args.get('currency')
    if not selected_currency:
        return jsonify({"error": "Parameter 'currency' is required."}), 400

    rates = get_exchange_rates_by_currency(selected_currency)
    if not rates:
        return jsonify({"error": f"No data found for currency: {selected_currency}"}), 404

    return jsonify(rates)


@app.route('/api/exchange_rates_period', methods=['GET'])
@auth.login_required
def api_get_exchange_rates_period():
    """
    API endpoint to get exchange rates for the selected currency over a specified period.

    Query parameters:
        currency: The currency code (e.g., 'USD', 'EUR')
        period: The number of days to look back (1, 3, 7, 30, 90, 180, 360)
    """
    selected_currency = request.args.get('currency')
    period = request.args.get('period')

    if not selected_currency or not period:
        return jsonify({"error": "Parameters 'currency' and 'period' (in days) are required."}), 400

    try:
        period_days = int(period)
    except ValueError:
        return jsonify({"error": "Parameter 'period' must be numeric."}), 400

    allowed_periods = [1, 3, 7, 30, 90, 180, 360]
    if period_days not in allowed_periods:
        return jsonify({"error": f"Allowed period values: {allowed_periods}"}), 400

    rates = get_exchange_rates_for_period(selected_currency, period_days)

    # Check if we have data for the requested period
    if not rates.get("data"):
        return jsonify({
            "warning": f"No data found for currency {selected_currency} in the last {period_days} days.",
            "result": rates
        }), 200

    return jsonify(rates)


@app.route('/api/supported_currencies', methods=['GET'])
@auth.login_required
def api_get_supported_currencies():
    """
    API endpoint to get a list of all supported currencies.

    Returns a list of currency codes that are available in the system.
    """
    # Get the latest data to extract supported currencies
    # We use USD as a common currency to fetch all data
    all_data = get_exchange_rates_by_currency("USD")

    if not all_data:
        # If no USD data, try EUR
        all_data = get_exchange_rates_by_currency("EUR")

    if not all_data:
        # No data at all, respond with empty list
        return jsonify({"supported_currencies": []}), 200

    # Extract unique currencies from all banks
    currencies = set()
    for bank in ["Raiffeisen", "PrivatBank", "Bestobmin"]:
        if bank in all_data:
            for rate in all_data[bank]:
                if "currency" in rate:
                    currencies.add(rate["currency"])

    return jsonify({"supported_currencies": sorted(list(currencies))}), 200


def run_scheduler() -> None:
    """Setup and start the background scheduler for regular updates"""
    try:
        # Setup the scheduler to update exchange rates every 15 minutes
        scheduler = BackgroundScheduler(timezone=pytz.timezone("Europe/Kiev"))
        scheduler.add_job(func=update_exchange_rates, trigger="interval", minutes=15)
        scheduler.start()

        if scheduler.running:
            logger.info("Scheduler started successfully.")
            # Register shutdown handler
            atexit.register(lambda: scheduler.shutdown())
        else:
            logger.error("Failed to start scheduler.")
    except Exception as e:
        logger.error(f"Error setting up scheduler: {e}")


@app.errorhandler(404)
def page_not_found(e):
    """Handle 404 errors"""
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_server_error(e):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {e}")
    return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    # Optionally update data at startup
    run_scheduler()
    update_exchange_rates()
    port = int(os.getenv("PORT", 5001))
    app.run(use_reloader=False, host='0.0.0.0', port=port)
