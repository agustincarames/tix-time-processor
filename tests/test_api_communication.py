import json
import random
import socket
import unittest
from datetime import datetime, timezone

import jsonschema
import requests
import requests_mock

from processor import api_communication


class TestApiCommunication(unittest.TestCase):

    TIX_API_RESULTS_SCHEMA = {
        "type": "object",
        "properties": {
            "timestamp": {"type": "integer"},
            "upUsage": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0
            },
            "upQuality": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0
            },
            "downUsage": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0
            },
            "downQuality": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0
            },
            "hurstUpRs": {"type": "number"},
            "hurstUpWavelet": {"type": "number"},
            "hurstDownRs": {"type": "number"},
            "hurstDownWavelet": {"type": "number"},
            "ipAddress": {
                "type": "string",
                "format": "ipv4"
            }
        }
    }

    @staticmethod
    def get_ip_address():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = '{ip}:{port}'.format(ip=s.getsockname()[0], port=s.getsockname()[1])
        s.close()
        return ip

    def setUp(self):
        self.results = {
            'timestamp': int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()),
            'upstream': {
                'usage': random.random(),
                'quality': random.random(),
                'hurst': {
                    'rs': random.random() * .5 + .5,
                    'wavelet': random.random() * .5 + .5
                }
            },
            'downstream': {
                'usage': random.random(),
                'quality': random.random(),
                'hurst': {
                    'rs': random.random() * .5 + .5,
                    'wavelet': random.random() * .5 + .5
                }
            }
        }
        self.ip = self.get_ip_address()

    def test_prepare_results_for_api(self):
        results_for_api = api_communication.prepare_results_for_api(self.results, self.ip)
        jsonschema.validate(results_for_api, self.TIX_API_RESULTS_SCHEMA)

    def test_prepare_url(self):
        user_id = random.randint(1, 10)
        installation_id = random.randint(1, 10)
        custom_port = random.randint(1025, 2**16)
        for expected_proto, expected_host, tix_api_ssl, tix_api_host, tix_api_port in [
            ('http', 'localhost', False, 'localhost', None),
            ('https', 'localhost', True, 'localhost', None),
            ('http', 'localhost:{}'.format(custom_port), False, 'localhost', str(custom_port)),
            ('https', 'localhost:{}'.format(custom_port), True, 'localhost', str(custom_port)),
            ('http', 'sarasa', False, 'sarasa', None)
        ]:
            expected_url = api_communication.TIX_API_URL_TEMPLATE.format(
                proto=expected_proto,
                api_host=expected_host,
                user_id=user_id,
                installation_id=installation_id
            )
            url = api_communication.prepare_url(user_id=user_id,
                                                installation_id=installation_id,
                                                tix_api_ssl=tix_api_ssl,
                                                tix_api_host=tix_api_host,
                                                tix_api_port=tix_api_port)
            self.assertEqual(url, expected_url)

    def test_post_results(self):
        user_id = random.randint(1, 10)
        installation_id = random.randint(1, 10)
        result = api_communication.post_results(self.ip, self.results, user_id, installation_id, None, None)
        self.assertFalse(result)
        with requests_mock.mock() as m:
            tix_api_user = 'test-admin-user'
            tix_api_pass = 'test-admin-pass'
            expected_url = api_communication.prepare_url(user_id, installation_id)
            expected_results = api_communication.prepare_results_for_api(self.results, self.ip)

            def _verify_request_results(request, context):
                self.assertEqual(request.body.decode(), json.dumps(expected_results))
                return request.body.decode()

            m.register_uri('POST', expected_url, json=_verify_request_results, status_code=200)
            result = api_communication.post_results(self.ip, self.results, user_id, installation_id, tix_api_user,
                                                    tix_api_pass)
            self.assertTrue(result)
            m.register_uri('POST', expected_url, json=_verify_request_results, status_code=204)
            result = api_communication.post_results(self.ip, self.results, user_id, installation_id, tix_api_user,
                                                    tix_api_pass)
            self.assertTrue(result)
            m.register_uri('POST', expected_url, status_code=403)
            result = api_communication.post_results(self.ip, self.results, user_id, installation_id, tix_api_user,
                                                    tix_api_pass)
            self.assertFalse(result)
            m.register_uri('POST', expected_url, exc=requests.RequestException)
            result = api_communication.post_results(self.ip, self.results, user_id, installation_id, tix_api_user,
                                                    tix_api_pass)
            self.assertFalse(result)
