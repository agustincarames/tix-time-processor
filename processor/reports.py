import base64
import json
from operator import itemgetter
from os import listdir, unlink, mkdir

from os.path import join, exists, isfile, islink

import logging

import struct

from processor.ip_to_as import get_as_by_ip

OBSERVATIONS_PER_REPORT = 60
MINIMUM_OBSERVATIONS_QTY = 1200
MINIMUM_REPORTS_QTY = MINIMUM_OBSERVATIONS_QTY / OBSERVATIONS_PER_REPORT
BACK_UP_REPORTS_PROCESSING_THRESHOLD = 10
REPORTS_GAP_THRESHOLD = OBSERVATIONS_PER_REPORT * 5

BACK_UP_REPORTS_DIR_NAME = 'backup-reports'
FAILED_REPORTS_DIR_NAME = 'failed-results'
FAILED_REPORT_FILE_NAME_TEMPLATE = 'failed-report-{timestamp}.json'

logger = logging.getLogger(__name__)


class ReportFieldTypes:
    class ReportFieldType:
        def __init__(self, name, byte_size, struct_type):
            self.name = name
            self.byte_size = byte_size
            self.struct_type = struct_type
    endian_type = '>'
    Integer = ReportFieldType('int', 4, 'i')
    Char = ReportFieldType('char', 1, 'b')
    Long = ReportFieldType('long', 8, 'q')


class ReportField:
    def __init__(self, name, report_field_type):
        self.name = name
        self.type = report_field_type


class ReportLine:
    fields = [
        ReportField('day_timestamp', ReportFieldTypes.Long),
        ReportField('type_identifier', ReportFieldTypes.Char),
        ReportField('packet_size', ReportFieldTypes.Integer),
        ReportField('initial_timestamp', ReportFieldTypes.Long),
        ReportField('reception_timestamp', ReportFieldTypes.Long),
        ReportField('sent_timestamp', ReportFieldTypes.Long),
        ReportField('final_timestamp', ReportFieldTypes.Long),
    ]
    byte_size = sum([field.type.byte_size for field in fields])


def get_reports_files(installation_dir_path):
    reports_files = []
    for file_name in sorted(listdir(installation_dir_path)):
        if file_name.endswith('.json'):
            file_path = join(installation_dir_path, file_name)
            if isfile(file_path) and not islink(file_path):
                reports_files.append((file_path, file_name))
    return reports_files


def back_up_dir_is_empty(installation_dir_path):
    back_up_reports_path = join(installation_dir_path, BACK_UP_REPORTS_DIR_NAME)
    return not exists(back_up_reports_path) or len(listdir(back_up_reports_path)) == 0


def get_back_up_reports(installation_dir_path, reports_quantity):
    back_up_reports_path = join(installation_dir_path, BACK_UP_REPORTS_DIR_NAME)
    back_up_reports = []
    for file_name in sorted(listdir(back_up_reports_path), reverse=True)[:reports_quantity]:
        if file_name.endswith('.json'):
            file_path = join(back_up_reports_path, file_name)
            if isfile(file_path) and not islink(file_path):
                back_up_reports.append((file_path, file_name))
    return back_up_reports


def clean_back_up_dir(installation_dir_path):
    back_up_reports_path = join(installation_dir_path, BACK_UP_REPORTS_DIR_NAME)
    for file_name in listdir(back_up_reports_path):
        file_path = join(back_up_reports_path, file_name)
        if isfile(file_path) and not islink(file_path):
            unlink(file_path)


def get_report_first_timestamp(report_file):
    with open(report_file[0]) as fp:
        report = json.load(fp)
        message_bytes = base64.b64decode(report['message'])
        field = ReportLine.fields[0]
        struct_format = ReportFieldTypes.endian_type + field.type.struct_type
        bytes_to_read = field.type.byte_size
        first_timestamp = struct.unpack(struct_format, message_bytes[:bytes_to_read])[0]
        return first_timestamp


def get_report_last_timestamp(report_file):
    with open(report_file[0]) as fp:
        report = json.load(fp)
        message_bytes = base64.b64decode(report['message'])
        last_line = message_bytes[-ReportLine.byte_size:]
        field = ReportLine.fields[0]
        struct_format = ReportFieldTypes.endian_type + field.type.struct_type
        bytes_to_read = field.type.byte_size
        last_timestamp = struct.unpack(struct_format, last_line[:bytes_to_read])[0]
        return last_timestamp


def get_report_gap(report_file):
    first_timestamp = get_report_first_timestamp(report_file)
    last_timestamp = get_report_last_timestamp(report_file)
    return last_timestamp - first_timestamp


def get_gap_between_reports(second_report_file, first_report_file):
    first_report_first_timestamp = get_report_first_timestamp(first_report_file)
    second_report_first_timestamp = get_report_first_timestamp(second_report_file)
    return second_report_first_timestamp - first_report_first_timestamp


def max_gap_in_reports(reports_files):
    gap = 0
    for index in range(1, len(reports_files)):
        reports_gap = get_gap_between_reports(reports_files[index], reports_files[index - 1])
        gap = max([gap, reports_gap])
    last_report_gap = get_report_gap(reports_files[-1])
    gap = max([gap, last_report_gap])
    return gap


def delete_files_before_last_gap_occurrence(gap, reports_files):
    reports_files_to_delete = []
    if get_report_gap(reports_files[-1]) == gap:
        reports_files_to_delete = reports_files
    else:
        for index in sorted(range(1, len(reports_files)), reverse=True):
            reports_gap = get_gap_between_reports(reports_files[index], reports_files[index - 1])
            if gap == reports_gap:
                reports_files_to_delete = reports_files[:index]
                break
    for report_file in reports_files_to_delete:
        unlink(report_file[0])


def extract_observations_from_report_message(report_message):
    bytes_message = base64.b85decode(report_message)
    observations = []
    for message_index in range(0, len(bytes_message), ReportLine.byte_size):
        line = bytes_message[message_index:message_index+ReportLine.byte_size]
        observation = {}
        line_struct_format = ReportFieldTypes.endian_type
        for field in ReportLine.fields:
            line_struct_format += field.type.struct_type
        line_tuple = struct.unpack(line_struct_format, line)
        for field_index in range(len(ReportLine.fields)):
            field = ReportLine.fields[field_index]
            observation[field.name] = line_tuple[field_index]
        observations.append(observation)
    return observations


def get_processable_report_files(installation_dir_path):
    reports_files = get_reports_files(installation_dir_path)
    if len(reports_files) < MINIMUM_REPORTS_QTY:
        # If there are less than the minimum reports needed to process in the installation directory and there are
        # no reports in the back up directory, then we assume that this might be a fresh process and leave
        if back_up_dir_is_empty(installation_dir_path):
            return []
        # If there are less than the minimum reports needed to process in the installation directory
        # and the amount of back up reports needed for processing is too much we simply establish that there
        # was a connection loss or something that renders the last minutes impossible to measure.
        # So we clean the back up files, but we leave the main reports in hopes that there's a burst with which we can
        # work
        needed_back_up_reports = MINIMUM_REPORTS_QTY - len(reports_files)
        if needed_back_up_reports > BACK_UP_REPORTS_PROCESSING_THRESHOLD:
            clean_back_up_dir(installation_dir_path)
            return []
        back_up_reports = get_back_up_reports(installation_dir_path, MINIMUM_REPORTS_QTY - len(reports_files))
        reports_files.extend(back_up_reports)
        reports_files.sort(key=itemgetter(1))
    # If there is gap between observations among the reports that makes it impossible to calculate, we simply delete
    # everything in the back up folder and all the reports until the one where the gap occurs. We also delete that one.
    gap = max_gap_in_reports(reports_files)
    if gap > REPORTS_GAP_THRESHOLD:
        delete_files_before_last_gap_occurrence(reports_files, gap)
        clean_back_up_dir(installation_dir_path)
        return []
    return reports_files


def extract_processable_data(reports_files):
    data_per_as = {}
    for report_file in reports_files:
        with open(report_file[0]) as report_pf:
            report = json.load(report_pf)
            as_info = get_as_by_ip(report['from'])
            if as_info['id'] not in data_per_as:
                data_per_as[as_info['id']] = {
                    'as_id': as_info['id'],
                    'as_owner': as_info['owner'],
                    'observations': list()
                }
            report_observations = extract_observations_from_report_message(report['message'])
            data_per_as[as_info['id']]['observations'].extend(report_observations)
    if len(data_per_as) > 1:
        raise ValueError('Expected 1 AS, found {}'.format(len(data_per_as)))
    as_id, data = data_per_as.items()
    return data


def get_data(installation_dir_path):
    log = logger.getChild('get_datapoints')
    log.info('getting datapoints')
    reports_files = get_processable_report_files(installation_dir_path)
    data = extract_processable_data(reports_files)
    return data


def failed_results_empty(installation_dir_path):
    failed_results_dir_path = join(installation_dir_path, FAILED_REPORTS_DIR_NAME)
    return not exists(failed_results_dir_path) or len(listdir(failed_results_dir_path)) == 0


def back_up_failed_results(installation_dir_path, results, as_info):
    failed_results_dir_path = join(installation_dir_path, FAILED_REPORTS_DIR_NAME)
    json_failed_results = {
        'results': results,
        'as_info': as_info
    }
    if not exists(failed_results_dir_path):
        mkdir(failed_results_dir_path)
    failed_result_file_name = FAILED_REPORT_FILE_NAME_TEMPLATE.format(timestamp=results['timestamp'])
    failed_result_file_path = join(failed_results_dir_path, failed_result_file_name)
    with open(failed_result_file_path) as failed_result_file:
        json.dump(json_failed_results, failed_result_file)
