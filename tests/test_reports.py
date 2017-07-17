import json
import random
import socket
import tempfile
import unittest

import datetime

import jsonschema

from processor import reports

DEFAULT_REPORT_DELTA = datetime.timedelta(minutes=1)
DEFAULT_OBSERVATIONS_DELTA = datetime.timedelta(seconds=1)
NANOS_IN_A_DAY = 24 * 60 * 60 * (10 ** 9)

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = '{ip}:{port}'.format(ip=s.getsockname()[0], port=s.getsockname()[1])
    s.close()
    return ip


def generate_observations(start_time):
    end_time = start_time + DEFAULT_REPORT_DELTA
    observations_time_delta = DEFAULT_OBSERVATIONS_DELTA
    current_time = start_time
    observations = []
    while current_time < end_time:
        day_timestamp = int(current_time.timestamp())
        type_identifier = 'S'.encode()
        packet_size = 64
        start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        initial_timestamp = int((current_time - start_of_day).total_seconds() * 10 ** 9) % NANOS_IN_A_DAY
        transmission_time = random.randint(1, 10 ** 9)
        reception_timestamp = (initial_timestamp + transmission_time) % NANOS_IN_A_DAY
        processing_time = random.randint(1, 10 ** 6)
        sent_timestamp = (reception_timestamp + processing_time) % NANOS_IN_A_DAY
        transmission_time = random.randint(1, 10 ** 9)
        final_timestamp = (sent_timestamp + transmission_time) % NANOS_IN_A_DAY
        observation = reports.Observation(day_timestamp, type_identifier, packet_size,
                                          initial_timestamp, reception_timestamp, sent_timestamp, final_timestamp)
        observations.append(observation)
        current_time += observations_time_delta
    return observations


def generate_report(from_dir, to_dir, user_id, installation_id,
                    start_time=datetime.datetime.now(datetime.timezone.utc)):
    packet_type = 'S'
    initial_timestamp = received_timestamp = sent_timestamp = final_timestamp = 0
    public_key = signature = 'a'
    observations = generate_observations(start_time)
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
                          user_id=user_id,
                          installation_id=installation_id)

USER_ID = random.randint(1, 10)
INSTALLATION_ID = random.randint(1, 10)
FROM_DIR = get_ip_address()
TO_DIR = '8.8.8.8:4500'


class TestReports(unittest.TestCase):
    def test_JSONCoDec(self):
        report = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID)
        json_report_string = json.dumps(report, cls=reports.ReportJSONEncoder)
        other_report = json.loads(json_report_string, cls=reports.ReportJSONDecoder)
        self.assertEqual(report, other_report)
        naive_json_report = json.loads(json_report_string)
        jsonschema.validate(naive_json_report, reports.ReportJSONDecoder.JSON_REPORT_SCHEMA)


class TestReport(unittest.TestCase):
    def test_load(self):
        with tempfile.NamedTemporaryFile() as report_file:
            original_report = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID)
            json.dump(original_report, report_file, cls=reports.ReportJSONEncoder)
            report_file_path = report_file.name
            loaded_report = reports.Report.load(report_file_path)
            self.assertEqual(original_report, loaded_report)

    def test_get_report_gap(self):
        report = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID)
        gap = reports.Report.get_report_gap(report)
        expected_gap = DEFAULT_REPORT_DELTA.microseconds * 10 ** 3
        self.assertEqual(gap, expected_gap)

    def test_get_gap_between_reports(self):
        current_time = datetime.datetime.now(datetime.timezone.utc)
        reports_gap = DEFAULT_REPORT_DELTA
        report1 = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID, current_time)
        report2 = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID, current_time + reports_gap)
        gap = reports.Report.get_gap_between_reports(report2, report1)
        expected_gap = reports_gap.microseconds * 10 ** 3
        self.assertEqual(gap, expected_gap)


class TestReportsHandler(unittest.TestCase):
    pass
