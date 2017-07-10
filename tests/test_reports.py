import json
import random
import socket
import unittest

import datetime

import jsonschema

from processor import reports


def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = '{ip}:{port}'.format(ip=s.getsockname()[0], port=s.getsockname()[1])
    s.close()
    return ip


class TestReports(unittest.TestCase):
    DEFAULT_REPORT_DELTA = datetime.timedelta(minutes=1)
    DEFAULT_OBSERVATIONS_DELTA = datetime.timedelta(seconds=1)
    USER_ID = random.randint(1, 10)
    INSTALLATION_ID = random.randint(1, 10)

    @staticmethod
    def generateObservations():
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(minutes=1)
        observations_time_delta = datetime.timedelta(seconds=1)
        current_time = start_time
        observations = []
        while current_time < end_time:
            day_timestamp = int(current_time.timestamp())
            type_identifier = 'S'.encode()
            packet_size = 64
            start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            nanos_in_a_day = 24 * 60 * 60 * (10 ** 9)
            initial_timestamp = int((current_time - start_of_day).total_seconds() * 10 ** 9) % nanos_in_a_day
            transmission_time = random.randint(1, 10 ** 9)
            reception_timestamp = (initial_timestamp + transmission_time) % nanos_in_a_day
            processing_time = random.randint(1, 10 ** 6)
            sent_timestamp = (reception_timestamp + processing_time) % nanos_in_a_day
            transmission_time = random.randint(1, 10 ** 9)
            final_timestamp = (sent_timestamp + transmission_time) % nanos_in_a_day
            observation = reports.Observation(day_timestamp, type_identifier, packet_size,
                                              initial_timestamp, reception_timestamp, sent_timestamp, final_timestamp)
            observations.append(observation)
            current_time += observations_time_delta
        return observations

    def generateReport(self):
        from_dir = get_ip_address()
        to_dir = '8.8.8.8:4500'
        packet_type = 'S'
        initial_timestamp = received_timestamp = sent_timestamp = final_timestamp = 0
        public_key = signature = 'a'
        observations = self.generateObservations()
        return reports.Report(from_dir=from_dir,
                              to_dir=to_dir,
                              packet_type=packet_type,
                              initial_timestamp=initial_timestamp,
                              received_timestamp=received_timestamp,
                              sent_timestamp=sent_timestamp,
                              final_timestamp=final_timestamp,
                              public_key=public_key,
                              observations=observations,
                              signature=signature,
                              user_id=self.USER_ID,
                              installation_id=self.INSTALLATION_ID)

    def testJSONCoDec(self):
        report = self.generateReport()
        json_report_string = json.dumps(report, cls=reports.ReportJSONEncoder)
        other_report = json.loads(json_report_string, cls=reports.ReportJSONDecoder)
        self.assertEqual(report, other_report)
        naive_json_report = json.loads(json_report_string)
        jsonschema.validate(naive_json_report, reports.JSON_REPORT_SCHEMA)
