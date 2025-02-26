import os
from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import pytz
from exchange_rates_service import (
    update_exchange_rates,
    get_exchange_rates_by_currency,
    get_exchange_rates_for_period
)
from dotenv import load_dotenv
from flask_talisman import Talisman

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
auth = HTTPBasicAuth()

Talisman(app, content_security_policy={
    'default-src': ['\'self\'']
})

# Get credentials from environment variables
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
USER_PASSWORD = os.getenv("USER_PASSWORD")

# Create users dictionary with hashed passwords
users = {
    "admin": generate_password_hash(ADMIN_PASSWORD, method='pbkdf2:sha256'),
    "user": generate_password_hash(USER_PASSWORD, method='pbkdf2:sha256')
}


@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username


# API endpoint to get the latest exchange rates for a selected currency
@app.route('/api/exchange_rates', methods=['GET'])
@auth.login_required
def api_get_exchange_rates():
    selected_currency = request.args.get('currency')
    if not selected_currency:
        return jsonify({"error": "Parameter 'currency' is required."}), 400
    rates = get_exchange_rates_by_currency(selected_currency)
    return jsonify(rates)


# API endpoint to get exchange rates for the selected currency over a specified period
@app.route('/api/exchange_rates_period', methods=['GET'])
@auth.login_required
def api_get_exchange_rates_period():
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
    return jsonify(rates)


def run_scheduler() -> None:
    # Setup the scheduler to update exchange rates every 15 minutes
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Europe/Kiev"))
    scheduler.add_job(func=update_exchange_rates, trigger="interval", minutes=15)
    scheduler.start()
    if scheduler.running:
        return print("Scheduler started successfully.")
    atexit.register(lambda: scheduler.shutdown())


if __name__ == '__main__':
    # Optionally update data at startup
    run_scheduler()
    update_exchange_rates()
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5001)
