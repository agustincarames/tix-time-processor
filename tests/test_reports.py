import json
import random
import socket
import tempfile
import unittest

import datetime

import jsonschema
from os import unlink

from os.path import join, exists

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


def generate_observations(start_time, report_delta, observations_delta):
    end_time = start_time + report_delta
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
        current_time += observations_delta
    return observations


def generate_report(from_dir, to_dir, user_id, installation_id,
                    start_time=datetime.datetime.now(datetime.timezone.utc),
                    report_delta=DEFAULT_REPORT_DELTA,
                    observations_delta=DEFAULT_OBSERVATIONS_DELTA):
    packet_type = 'S'
    initial_timestamp = received_timestamp = sent_timestamp = final_timestamp = 0
    public_key = signature = 'a'
    observations = generate_observations(start_time, report_delta, observations_delta)
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
        jsonschema.validate(naive_json_report, reports.JSON_REPORT_SCHEMA)


class TestReport(unittest.TestCase):
    def test_load(self):
        report_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        original_report = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID)
        json.dump(original_report, report_file, cls=reports.ReportJSONEncoder)
        report_file_path = report_file.name
        report_file.close()
        loaded_report = reports.Report.load(report_file_path)
        self.assertIsNotNone(loaded_report.file_path)
        self.assertNotEquals(original_report, loaded_report)
        loaded_report.file_path = None
        self.assertEquals(original_report, loaded_report)
        unlink(report_file_path)

    def test_get_report_gap(self):
        report = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID)
        gap = reports.Report.get_report_gap(report)
        expected_gap = DEFAULT_REPORT_DELTA.total_seconds() - 1
        self.assertEqual(gap, expected_gap)

    def test_get_gap_between_reports(self):
        current_time = datetime.datetime.now(datetime.timezone.utc)
        reports_gap = DEFAULT_REPORT_DELTA
        report1 = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID, current_time)
        report2 = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID, current_time + reports_gap)
        gap = reports.Report.get_gap_between_reports(report2, report1)
        expected_gap = reports_gap.total_seconds()
        self.assertEqual(gap, expected_gap)


class TestReportsHandler(unittest.TestCase):

    @staticmethod
    def create_report_files(dir_path, total_observations_qty, start_time, reports_delta, observations_delta):
        observations_qty = 0
        expected_processable_reports = []
        while observations_qty < total_observations_qty:
            report = generate_report(FROM_DIR, TO_DIR, USER_ID, INSTALLATION_ID,
                                     start_time=start_time,
                                     report_delta=reports_delta,
                                     observations_delta=observations_delta)
            report_file_name = 'tix-report-{timestamp}.json'.format(timestamp=report.observations[0].day_timestamp)
            report_path = join(dir_path, report_file_name)
            with open(report_path, 'w') as report_fp:
                json.dump(report, report_fp, cls=reports.ReportJSONEncoder)
            report.file_path = report_path
            observations_qty += len(report.observations)
            start_time += reports_delta
            expected_processable_reports.append(report)
        return expected_processable_reports

    def setUp(self):
        self.working_dir = tempfile.TemporaryDirectory()
        self.reports_handler = reports.ReportHandler(self.working_dir.name)

    def tearDown(self):
        self.working_dir.cleanup()

    def test_init(self):
        expected_installation_dir_path = self.working_dir.name
        expected_back_up_dir_path = join(expected_installation_dir_path,
                                         reports.ReportHandler.BACK_UP_REPORTS_DIR_NAME)
        expected_failed_results_path = join(expected_installation_dir_path,
                                            reports.ReportHandler.FAILED_RESULTS_DIR_NAME)
        self.assertEquals(self.reports_handler.installation_dir_path,
                          expected_installation_dir_path)
        self.assertEquals(self.reports_handler.back_up_reports_dir_path,
                          expected_back_up_dir_path)
        self.assertTrue(exists(self.reports_handler.back_up_reports_dir_path))
        self.assertEquals(self.reports_handler.failed_results_dir_path,
                          expected_failed_results_path)
        self.assertTrue(exists(self.reports_handler.failed_results_dir_path))

    def test_returns_reports_if_enough_processable_reports(self):
        expected_processable_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                                total_observations_qty=reports.ReportHandler.MINIMUM_OBSERVATIONS_QTY,
                                                                start_time=datetime.datetime.now(tz=datetime.timezone.utc),
                                                                reports_delta=DEFAULT_REPORT_DELTA,
                                                                observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        processable_reports = self.reports_handler.get_processable_reports()
        self.assertListEqual(processable_reports, expected_processable_reports)

    def test_returns_reports_if_not_enough_processable_but_back_up(self):
        processable_observations_qty = reports.ReportHandler.MINIMUM_OBSERVATIONS_QTY - \
                                       reports.ReportHandler.BACK_UP_OBSERVATIONS_QTY_PROCESSING_THRESHOLD - 1
        back_up_observations_qty = reports.ReportHandler.BACK_UP_OBSERVATIONS_QTY_PROCESSING_THRESHOLD
        back_up_reports_start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        expected_back_up_reports = self.create_report_files(dir_path=self.reports_handler.back_up_reports_dir_path,
                                                            total_observations_qty=back_up_observations_qty,
                                                            start_time=back_up_reports_start_time,
                                                            reports_delta=DEFAULT_REPORT_DELTA,
                                                            observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        last_timestamp = expected_back_up_reports[-1].observations[-1].day_timestamp
        processable_observations_start_time = datetime.datetime.fromtimestamp(last_timestamp, tz=datetime.timezone.utc)\
            + DEFAULT_OBSERVATIONS_DELTA
        expected_processable_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                                total_observations_qty=processable_observations_qty,
                                                                start_time=processable_observations_start_time,
                                                                reports_delta=DEFAULT_REPORT_DELTA,
                                                                observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        expected_reports = list()
        expected_reports.extend(expected_back_up_reports)
        expected_reports.extend(expected_processable_reports)
        processable_reports = self.reports_handler.get_processable_reports()
        self.assertListEqual(processable_reports, expected_reports)

    def test_doesnt_return_reports_if_gap_threshold_exceeded_in_reports(self):
        first_processable_reports_qty = reports.ReportHandler.MINIMUM_OBSERVATIONS_QTY / 2
        second_processable_reports_qty = reports.ReportHandler.MINIMUM_OBSERVATIONS_QTY / 2
        first_processable_reports_start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        first_processable_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                             total_observations_qty=first_processable_reports_qty,
                                                             start_time=first_processable_reports_start_time,
                                                             reports_delta=DEFAULT_REPORT_DELTA,
                                                             observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        last_timestamp = first_processable_reports[-1].observations[-1].day_timestamp
        second_processable_reports_start_time = datetime.datetime.fromtimestamp(last_timestamp, tz=datetime.timezone.utc)\
            + datetime.timedelta(seconds=reports.ReportHandler.GAP_THRESHOLD)
        second_processable_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                              total_observations_qty=second_processable_reports_qty,
                                                              start_time=second_processable_reports_start_time,
                                                              reports_delta=DEFAULT_REPORT_DELTA,
                                                              observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        processable_reports = self.reports_handler.get_processable_reports()
        self.assertEquals(len(processable_reports), 0)

    def test_doesnt_return_reports_if_not_enough_processable_reports_and_not_enough_back_up_reports(self):
        processable_observations_qty = (reports.ReportHandler.MINIMUM_OBSERVATIONS_QTY -
                                        reports.ReportHandler.BACK_UP_OBSERVATIONS_QTY_PROCESSING_THRESHOLD - 1) / 2
        back_up_observations_qty = reports.ReportHandler.MAXIMUM_OBSERVATIONS_QTY
        back_up_reports_start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        expected_back_up_reports = self.create_report_files(dir_path=self.reports_handler.back_up_reports_dir_path,
                                                            total_observations_qty=back_up_observations_qty,
                                                            start_time=back_up_reports_start_time,
                                                            reports_delta=DEFAULT_REPORT_DELTA,
                                                            observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        last_timestamp = expected_back_up_reports[-1].observations[-1].day_timestamp
        processable_observations_start_time = datetime.datetime.fromtimestamp(last_timestamp, tz=datetime.timezone.utc) \
                                              + DEFAULT_OBSERVATIONS_DELTA
        expected_processable_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                                total_observations_qty=processable_observations_qty,
                                                                start_time=processable_observations_start_time,
                                                                reports_delta=DEFAULT_REPORT_DELTA,
                                                                observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        expected_reports = list()
        expected_reports.extend(expected_back_up_reports)
        expected_reports.extend(expected_processable_reports)
        processable_reports = self.reports_handler.get_processable_reports()
        self.assertEquals(len(processable_reports), 0)
        self.assertTrue(self.reports_handler.back_up_dir_is_empty())
        processable_reports = self.reports_handler.get_processable_reports()
        self.assertEquals(len(processable_reports), 0)

    def test_only_returns_max_observations_reports(self):
        processable_observations_qty = reports.ReportHandler.MAXIMUM_OBSERVATIONS_QTY * 2
        total_processable_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                             total_observations_qty=processable_observations_qty,
                                                             start_time=datetime.datetime.now(tz=datetime.timezone.utc),
                                                             reports_delta=DEFAULT_REPORT_DELTA,
                                                             observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        expected_processable_reports = list()
        while reports.ReportHandler.calculate_observations_quantity(expected_processable_reports) < reports.ReportHandler.MAXIMUM_OBSERVATIONS_QTY:
            report = total_processable_reports.pop(0)
            expected_processable_reports.append(report)
        remaining_processable_reports = total_processable_reports
        processable_reports = self.reports_handler.get_processable_reports()
        self.assertListEqual(processable_reports, expected_processable_reports)

    def test_back_up_reports(self):
        created_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                   total_observations_qty=reports.ReportHandler.MINIMUM_OBSERVATIONS_QTY,
                                                   start_time=datetime.datetime.now(tz=datetime.timezone.utc),
                                                   reports_delta=DEFAULT_REPORT_DELTA,
                                                   observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        self.assertTrue(self.reports_handler.back_up_dir_is_empty())
        self.reports_handler.back_up_reports(created_reports)
        self.assertFalse(self.reports_handler.back_up_dir_is_empty())
        processable_reports = self.reports_handler.get_processable_reports()
        self.assertEquals(len(processable_reports), 0)

    def test_collect_observations(self):
        created_reports = self.create_report_files(dir_path=self.reports_handler.installation_dir_path,
                                                   total_observations_qty=reports.ReportHandler.MINIMUM_OBSERVATIONS_QTY,
                                                   start_time=datetime.datetime.now(tz=datetime.timezone.utc),
                                                   reports_delta=DEFAULT_REPORT_DELTA,
                                                   observations_delta=DEFAULT_OBSERVATIONS_DELTA)
        expected_observations = set()
        for report in created_reports:
            expected_observations.update(report.observations)
        ip, observations = self.reports_handler.collect_observations(created_reports)
        self.assertEquals(ip, FROM_DIR)
        self.assertEquals(observations, expected_observations)
