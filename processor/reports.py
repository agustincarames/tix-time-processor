from processor.report_parser import *

import datetime
import json
import os
from os import listdir, unlink, mkdir, rename

from os.path import join, exists, isfile, islink

import logging

logger = logging.getLogger(__name__)

class NotEnoughObservationsError(Exception):
    pass

class ReportHandler:
    MINIMUM_OBSERVATIONS_QTY = 1024 + 60  # We need 1024 observation points plus a minute for analysis
    MAXIMUM_OBSERVATIONS_QTY = 1200
    BACK_UP_OBSERVATIONS_QTY_PROCESSING_THRESHOLD = 540
    GAP_THRESHOLD = int(datetime.timedelta(minutes=5).total_seconds())

    FAILED_RESULTS_DIR_NAME = 'failed-results'
    FAILED_REPORT_FILE_NAME_TEMPLATE = 'failed-report-{timestamp}.json'

    @staticmethod
    def reports_sorting_key(report):
        return report.observations[0].day_timestamp

    @staticmethod
    def max_gap_in_reports(reports):
        gap = 0
        if len(reports) > 0:
            for index in range(1, len(reports)):
                reports_gap = Report.get_gap_between_reports(reports[index], reports[index - 1])
                gap = max([gap, reports_gap])
            last_report_gap = reports[-1].get_observations_gap()
            gap = max([gap, last_report_gap])
        return gap

    @staticmethod
    def divide_gapped_reports(reports, gap):
        reports_before = list()
        reports_after = list()
        if reports[-1].get_observations_gap() == gap:
            reports_after.append(reports[-1])
            reports_before = reports[:-1]
        else:
            for index in sorted(range(1, len(reports)), reverse=True):
                reports_gap = Report.get_gap_between_reports(reports[index], reports[index - 1])
                if gap == reports_gap:
                    reports_before = reports[:index]
                    reports_after = reports[index:]
                    break
        return reports_before, reports_after

    @staticmethod
    def delete_reports_files(reports):
        for report in reports:
            if exists(report.file_path):
                unlink(report.file_path)

    @staticmethod
    def calculate_observations_quantity(reports):
        return sum([len(report.observations) for report in reports])

    @classmethod
    def fetch_reports(cls, reports_dir_path, last_first=False):
        reports = []
        reports_files = sorted(listdir(reports_dir_path), reverse=last_first)
        for file_name in reports_files:
            if file_name.endswith('.json'):
                file_path = join(reports_dir_path, file_name)
                if isfile(file_path) and not islink(file_path):
                    report_object = Report.load(file_path)
                    reports.append(report_object)
        return reports

    @classmethod
    def collect_observations(cls, reports):
        data_per_ip = {}
        for report in reports:
            socket_dir = report.from_dir
            ip = socket_dir.split(':')[0]
            if ip not in data_per_ip:
                data_per_ip[ip] = set()
            data_per_ip[ip].update(report.observations)
        for ip, observations in data_per_ip.items():
            return ip, observations

    def __init__(self, installation_dir_path):
        self.logger = logger.getChild('ReportHandler')
        self.installation_dir_path = installation_dir_path
        self.failed_results_dir_path = join(self.installation_dir_path, self.FAILED_RESULTS_DIR_NAME)
        if not exists(self.failed_results_dir_path):
            mkdir(self.failed_results_dir_path)
        self.reports_files = list()
        self.processable_reports = list()
        self.__update_reports_files()

    def __update_reports_files(self):
        self.reports_files = [join(self.installation_dir_path, report_file_name)
                              for report_file_name in sorted(listdir(self.installation_dir_path))
                              if report_file_name.endswith('.json')]

    def __divide_reports_by_gap_threshold(self, reports):
        gap = self.max_gap_in_reports(reports)
        if self.GAP_THRESHOLD < gap:
            reports_before, reports_after = self.divide_gapped_reports(reports, gap)
        else:
            reports_before = reports
            reports_after = list()
        return reports_before, reports_after

    def update_processable_reports(self):
        self.__update_reports_files()
        processable_reports = list()
        while (self.calculate_observations_quantity(processable_reports) < self.MINIMUM_OBSERVATIONS_QTY and
               len(self.reports_files) > 0):
            new_report = Report.load(self.reports_files.pop(0))
            # Ensure all processable reports are from the same IP
            if len(processable_reports) > 0:
                processable_reports_ip = processable_reports[0].from_dir.split(':')[0]
                new_report_ip = new_report.from_dir.split(':')[0]
                if new_report_ip != processable_reports_ip:
                    self.delete_reports_files(processable_reports)
                    processable_reports.clear()
            processable_reports.append(new_report)
            if self.calculate_observations_quantity(processable_reports) > self.MINIMUM_OBSERVATIONS_QTY:
                # Ensure that the reports have no irrecoverable gaps
                reports_before_gap, reports_after_gap = self.__divide_reports_by_gap_threshold(processable_reports)
                if self.calculate_observations_quantity(reports_before_gap) < self.MINIMUM_OBSERVATIONS_QTY:
                    self.delete_reports_files(reports_before_gap)
                    processable_reports = reports_after_gap
                else:
                    processable_reports = reports_before_gap
        if self.calculate_observations_quantity(processable_reports) < self.MINIMUM_OBSERVATIONS_QTY:
            self.processable_reports = list()
        else:
            self.processable_reports = processable_reports

    def get_ip_and_processable_observations(self):
        self.update_processable_reports()
        if len(self.processable_reports) == 0:
            ip, observations = None, None
        else:
            ip, observations = self.collect_observations(self.processable_reports)
        return ip, observations

    def delete_unneeded_reports(self):
        reports_to_delete_qty = len(self.processable_reports) // 2
        reports_to_delete = self.processable_reports[:reports_to_delete_qty]
        self.delete_reports_files(reports_to_delete)

    def failed_results_dir_is_empty(self):
        return not exists(self.failed_results_dir_path) or len(listdir(self.failed_results_dir_path)) == 0

    def back_up_failed_results(self, results, ip):
        json_failed_results = {
            'results': results,
            'ip': ip
        }
        failed_result_file_name = self.FAILED_REPORT_FILE_NAME_TEMPLATE.format(timestamp=results['timestamp'])
        failed_result_file_path = join(self.failed_results_dir_path, failed_result_file_name)
        with open(failed_result_file_path, 'w') as failed_result_file:
            json.dump(json_failed_results, failed_result_file)
