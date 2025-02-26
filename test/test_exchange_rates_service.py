import unittest
from exchange_rates_service import (
    normalize_raiffeisen_rates,
    normalize_privatbank_rates,
    get_exchange_rates_by_currency,
    get_exchange_rates_for_period
)


class TestExchangeRatesService(unittest.TestCase):
    def test_normalize_raiffeisen_rates(self):
        raw_data = [
            {"currency": "USD", "rate_buy": "41.45", "rate_sell": "41.77"},
            {"currency": "EUR", "rate_buy": "43.13", "rate_sell": "43.82"}
        ]
        normalized = normalize_raiffeisen_rates(raw_data)
        self.assertEqual(len(normalized), 2)
        self.assertEqual(normalized[0]["currency"], "USD")
        self.assertEqual(normalized[0]["base_currency"], "UAH")

    def test_normalize_privatbank_rates(self):
        raw_data = [
            {"ccy": "USD", "base_ccy": "UAH", "buy": "41.40", "sale": "42.02"},
            {"ccy": "EUR", "base_ccy": "UAH", "buy": "43.17", "sale": "44.05"}
        ]
        normalized = normalize_privatbank_rates(raw_data)
        self.assertEqual(len(normalized), 2)
        self.assertEqual(normalized[1]["currency"], "EUR")
        self.assertEqual(normalized[1]["base_currency"], "UAH")

    # Note: Testing functions that access the database (get_exchange_rates_by_currency,
    # get_exchange_rates_for_period) would typically require a test database or mocking.
    # Here we assume that there is no data and simply check for empty returns.

    def test_get_exchange_rates_by_currency_empty(self):
        # Expect empty dictionary if no document is found.
        result = get_exchange_rates_by_currency("USD")
        self.assertEqual(result, {})

    def test_get_exchange_rates_for_period_empty(self):
        # Expect empty period data if no documents exist.
        result = get_exchange_rates_for_period("USD", 7)
        self.assertIn("data", result)
        self.assertEqual(result["data"], [])


if __name__ == '__main__':
    unittest.main()
