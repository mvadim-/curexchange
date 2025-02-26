import unittest
from api import app


class ApiTestCase(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()
        # Replace 'your_admin_password' with the actual value from your .env for testing purposes.
        self.auth_header = {
            'Authorization': 'Basic YWRtaW46eGFidGFxLWp1bXB5ay1Lb3FoaTQ='
        }

    def test_exchange_rates_endpoint_no_auth(self):
        # Request without authentication should return 401 Unauthorized
        response = self.client.get('/api/exchange_rates?currency=USD')
        self.assertEqual(response.status_code, 401)

    def test_exchange_rates_endpoint_auth(self):
        response = self.client.get('/api/exchange_rates?currency=USD', headers=self.auth_header)
        # For testing purposes, even if the database is empty, we expect 200 OK
        self.assertEqual(response.status_code, 200)

    def test_exchange_rates_period_endpoint_auth(self):
        response = self.client.get('/api/exchange_rates_period?currency=USD&period=7', headers=self.auth_header)
        self.assertEqual(response.status_code, 200)

    def test_exchange_rates_period_invalid_period(self):
        response = self.client.get('/api/exchange_rates_period?currency=USD&period=5', headers=self.auth_header)
        self.assertEqual(response.status_code, 400)


if __name__ == '__main__':
    unittest.main()
