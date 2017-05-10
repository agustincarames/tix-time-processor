import unittest
from datetime import datetime, timezone

import dateutil.parser

from processor import analysis


class TestHurst(unittest.TestCase):

    def setUp(self):
        self.reports_data = []
        with open('tests/test_analysis_data.txt') as data_file:
            for line in data_file:
                datetime_string, observation_data = line.split(' ')
                date_str, time_str = datetime_string.split('|')
                date = dateutil.parser.parse(date_str).date()
                time = dateutil.parser.parse(time_str).time()
                timestamp = datetime.combine(date, time).replace(tzinfo=timezone.utc).timestamp()
                empty, size, t1, t2, t3, t4 = observation_data.split('|')
                observation = {
                    'day_timestamp': int(timestamp),
                    'type': 'S',
                    'size': 64,
                    'initial_timestamp': int(t1),
                    'reception_timestamp': int(t2),
                    'sent_timestamp': int(t3),
                    'final_timestamp': int(t4)
                }
                self.reports_data.append(observation)

    def test_process_observations(self):
        expected_results = {
            'timestamp': 0,
            'downstream_usage': .0,
            'upstream_usage': .0,
            'upstream_quality': .0,
            'downstream_quality': .0,
            'upstream_hurst': {
                'wavelet': .0,
                'rs': .0
            },
            'downstream_hurst': {
                'wavelet': .0,
                'rs': .0
            }
        }
        results = analysis.process_observations(self.reports_data)
        print(results)
        self.assertAlmostEqual()
        pass
