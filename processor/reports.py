import base64
import datetime
import json
import os
from operator import attrgetter
from os import listdir, unlink, mkdir, rename

from os.path import join, exists, isfile, islink

import logging

import struct

import inflection
import jsonschema

logger = logging.getLogger(__name__)


class ReportFieldTypes:
    class ReportFieldType:
        def __init__(self, name, byte_size, struct_type):
            self.name = name
            self.byte_size = byte_size
            self.struct_type = struct_type

        def get_struct_representation(self):
            return ReportFieldTypes.endian_type + self.struct_type
    endian_type = '>'
    Integer = ReportFieldType('int', 4, 'i')
    Char = ReportFieldType('char', 1, 'c')
    Long = ReportFieldType('long', 8, 'q')


class FieldTranslation:
    def __init__(self, original, translation, translator=None, reverse_translator=None):
        self.original = original
        self.translation = translation
        self.translator = translator
        self.reverse_translator = reverse_translator

    def translate(self, field_value):
        if self.translator is None:
            return field_value
        return self.translator(field_value)

    def reverse_translate(self, field_value):
        if self.reverse_translator is None:
            return field_value
        return self.reverse_translator(field_value)


class Observation:
    def __init__(self, day_timestamp, type_identifier, packet_size,
                 initial_timestamp, reception_timestamp, sent_timestamp, final_timestamp):
        self.day_timestamp = day_timestamp
        self.type_identifier = type_identifier
        self.packet_size = packet_size
        self.initial_timestamp = initial_timestamp
        self.reception_timestamp = reception_timestamp
        self.sent_timestamp = sent_timestamp
        self.final_timestamp = final_timestamp

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __hash__(self):
        return hash((self.day_timestamp,
                     self.type_identifier,
                     self.packet_size,
                     self.initial_timestamp,
                     self.reception_timestamp,
                     self.sent_timestamp,
                     self.final_timestamp))


class SerializedObservationField:
    def __init__(self, name, report_field_type):
        self.name = name
        self.type = report_field_type


class SerializedObservation:
    fields = [
        # Timestamp in seconds since UNIX epoch of the time the packet was sent to the server
        SerializedObservationField('day_timestamp', ReportFieldTypes.Long),
        # Byte indicating if is a Long or Short packet
        SerializedObservationField('type_identifier', ReportFieldTypes.Char),
        # Size of the packet when it was sent to the server
        SerializedObservationField('packet_size', ReportFieldTypes.Integer),
        # The timestamp in nanoseconds since the start of the day in local time
        # from when the packet was sent by the client to the server
        SerializedObservationField('initial_timestamp', ReportFieldTypes.Long),
        # The timestamp in nanoseconds since the start of the day in local time
        # from when the packet was received by the server
        SerializedObservationField('reception_timestamp', ReportFieldTypes.Long),
        # The timestamp in nanoseconds since the start of the day in local time
        # from when the packet was sent by the server to the client
        SerializedObservationField('sent_timestamp', ReportFieldTypes.Long),
        # The timestamp in nanoseconds since the start of the day in local time
        # from when the packet was received by the client
        SerializedObservationField('final_timestamp', ReportFieldTypes.Long),
    ]
    byte_size = sum([field.type.byte_size for field in fields])


def serialize_observations(observations):
    bytes_message = bytes()
    for observation in observations:
        observation_bytes = bytes()
        for field in SerializedObservation.fields:
            field_bytes = struct.pack(field.type.get_struct_representation(), getattr(observation, field.name))
            observation_bytes = b''.join([observation_bytes, field_bytes])
        bytes_message = b''.join([bytes_message, observation_bytes])
    return base64.b64encode(bytes_message).decode()


def deserialize_observations(message):
    bytes_message = base64.b64decode(message)
    observations = []
    for message_index in range(0, len(bytes_message), SerializedObservation.byte_size):
        line = bytes_message[message_index:message_index + SerializedObservation.byte_size]
        observation_dict = {}
        line_struct_format = ReportFieldTypes.endian_type
        for field in SerializedObservation.fields:
            line_struct_format += field.type.struct_type
        line_tuple = struct.unpack(line_struct_format, line)
        for field_index in range(len(SerializedObservation.fields)):
            field = SerializedObservation.fields[field_index]
            observation_dict[field.name] = line_tuple[field_index]
        observations.append(Observation(**observation_dict))
    return observations


JSON_FIELDS_TRANSLATIONS = [
    FieldTranslation("from", "from_dir"),
    FieldTranslation("to", "to_dir"),
    FieldTranslation("type", "packet_type"),
    FieldTranslation("message", "observations", deserialize_observations, serialize_observations)
]

JSON_REPORT_SCHEMA = {
    "type": "object",
    "properties": {
        "from": {
            "anyOf": [
                {"type": "string", "format": "ipv4"},
                {"type": "string", "format": "ipv6"},
                {"type": "string", "format": "hostname"}
            ]
        },
        "to": {
            "anyOf": [
                {"type": "string", "format": "ipv4"},
                {"type": "string", "format": "ipv6"},
                {"type": "string", "format": "hostname"}
            ]
        },
        "type": {
            "type": "string",
            "enum": ["S", "L"]
        },
        "initialTimestamp": {"type": "integer"},
        "receivedTimestamp": {"type": "integer"},
        "sentTimestamp": {"type": "integer"},
        "finalTimestamp": {"type": "integer"},
        "publicKey": {"type": "string"},
        "message": {"type": "string"},
        "signature": {"type": "string"},
        "userId": {"type": "integer"},
        "installationId": {"type": "integer"}
    },
    "required": [
        "from", "to", "type",
        "initialTimestamp", "receivedTimestamp", "sentTimestamp", "finalTimestamp",
        "publicKey", "message", "signature",
        "userId", "installationId"
    ]
}


class ReportJSONEncoder(json.JSONEncoder):
    def object_to_json(self, report_object):
        report_dict = report_object.__dict__.copy()
        for field_translation in JSON_FIELDS_TRANSLATIONS:
            field_value = report_dict.pop(field_translation.translation)
            report_dict[field_translation.original] = field_translation.reverse_translate(field_value)
        report_dict_fields = report_dict.keys()
        for field in report_dict_fields:
            inflexed_key = inflection.camelize(field, False)
            report_dict[inflexed_key] = report_dict.pop(field)
        fields_to_delete = []
        for field in report_dict_fields:
            if field not in JSON_REPORT_SCHEMA['required']:
                fields_to_delete.append(field)
        for field in fields_to_delete:
            report_dict.pop(field)
        return report_dict

    def default(self, obj):
        if isinstance(obj, Report):
            json_dict = self.object_to_json(obj)
        else:
            json_dict = json.JSONEncoder.default(self, obj)
        return json_dict


class ReportJSONDecoder(json.JSONDecoder):
    @staticmethod
    def json_to_object(json_dict):
        json_dict_keys = json_dict.keys()
        for key in json_dict_keys:
            new_key = inflection.underscore(key)
            json_dict[new_key] = json_dict.pop(key)
        for field_translation in JSON_FIELDS_TRANSLATIONS:
            if field_translation.original in json_dict.keys():
                field_value = json_dict.pop(field_translation.original)
                json_dict[field_translation.translation] = field_translation.translate(field_value)
        return Report(**json_dict)

    def dict_to_object(self, d):
        try:
            jsonschema.validate(d, JSON_REPORT_SCHEMA)
            inst = self.json_to_object(d)
        except jsonschema.ValidationError:
            inst = d
        return inst

    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)


class Report:
    @staticmethod
    def load(report_file_path):
        with open(report_file_path) as fp:
            report = json.load(fp, cls=ReportJSONDecoder)
        report.file_path = report_file_path
        return report

    @staticmethod
    def get_report_gap(report):
        return report.observations[-1].day_timestamp - report.observations[0].day_timestamp

    @staticmethod
    def get_gap_between_reports(second_report, first_report):
        return second_report.observations[0].day_timestamp - first_report.observations[0].day_timestamp

    def __init__(self,
                 from_dir, to_dir, packet_type,
                 initial_timestamp, received_timestamp, sent_timestamp, final_timestamp,
                 public_key, observations, signature,
                 user_id, installation_id, file_path=None):
        self.from_dir = from_dir
        self.to_dir = to_dir
        self.packet_type = packet_type
        self.initial_timestamp = initial_timestamp
        self.received_timestamp = received_timestamp
        self.sent_timestamp = sent_timestamp
        self.final_timestamp = final_timestamp
        self.public_key = public_key
        self.observations = observations
        self.signature = signature
        self.user_id = user_id
        self.installation_id = installation_id
        self.file_path = file_path

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __hash__(self):
        return hash((self.from_dir,
                     self.to_dir,
                     self.packet_type,
                     self.initial_timestamp,
                     self.received_timestamp,
                     self.sent_timestamp,
                     self.final_timestamp,
                     self.public_key,
                     self.observations,
                     self.signature,
                     self.user_id,
                     self.installation_id))

    def __repr__(self):
        return '{0!s}({1!r})'.format(self.__class__, self.__dict__)


class ReportHandler:
    MINIMUM_OBSERVATIONS_QTY = 1024
    MAXIMUM_OBSERVATIONS_QTY = 1200
    BACK_UP_OBSERVATIONS_QTY_PROCESSING_THRESHOLD = 540
    GAP_THRESHOLD = int(datetime.timedelta(minutes=5).total_seconds())

    BACK_UP_REPORTS_DIR_NAME = 'backup-reports'
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
            last_report_gap = Report.get_report_gap(reports[-1])
            gap = max([gap, last_report_gap])
        return gap

    @staticmethod
    def divide_gapped_reports(reports, gap):
        reports_before = list()
        reports_after = list()
        if Report.get_report_gap(reports[-1]) == gap:
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
    def fetch_reports(reports_dir_path, last_first=False, reports_quantity=None):
        reports = []
        reports_files = sorted(listdir(reports_dir_path), reverse=last_first)
        if reports_quantity is not None:
            reports_files = reports_files[:reports_quantity]
        for file_name in reports_files:
            if file_name.endswith('.json'):
                file_path = join(reports_dir_path, file_name)
                if isfile(file_path) and not islink(file_path):
                    report_object = Report.load(file_path)
                    reports.append(report_object)
        return reports

    @staticmethod
    def calculate_observations_quantity(reports):
        return sum([len(report.observations) for report in reports])

    @classmethod
    def collect_observations(cls, reports):
        data_per_ip = {}
        for report in reports:
            ip = report.from_dir
            if ip not in data_per_ip:
                data_per_ip[ip] = set()
            data_per_ip[ip].update(report.observations)
        for ip, observations in data_per_ip.items():
            if len(observations) >= cls.MINIMUM_OBSERVATIONS_QTY:
                return ip, observations
        raise ValueError('Expected at least 1 AS with {} observations. None found.'.format(cls.MINIMUM_OBSERVATIONS_QTY))

    def __init__(self, installation_dir_path):
        self.installation_dir_path = installation_dir_path
        self.back_up_reports_dir_path = join(self.installation_dir_path, self.BACK_UP_REPORTS_DIR_NAME)
        if not exists(self.back_up_reports_dir_path):
            mkdir(self.back_up_reports_dir_path)
        self.failed_results_dir_path = join(self.installation_dir_path, self.FAILED_RESULTS_DIR_NAME)
        if not exists(self.failed_results_dir_path):
            mkdir(self.failed_results_dir_path)

    def get_unprocessed_reports(self):
        return self.fetch_reports(self.installation_dir_path, last_first=False)

    def back_up_dir_is_empty(self):
        return not exists(self.back_up_reports_dir_path) or len(listdir(self.back_up_reports_dir_path)) == 0

    def get_back_up_reports(self, reports_quantity):
        return self.fetch_reports(self.back_up_reports_dir_path, last_first=False, reports_quantity=reports_quantity)

    def clean_back_up_dir(self):
        for file_name in listdir(self.back_up_reports_dir_path):
            file_path = join(self.back_up_reports_dir_path, file_name)
            if isfile(file_path) and not islink(file_path):
                unlink(file_path)

    def get_gapless_reports(self, reports):
        gap = self.max_gap_in_reports(reports)
        if self.GAP_THRESHOLD < gap:
            reports_before, reports_after = self.divide_gapped_reports(reports, gap)
            return reports_before
        else:
            return reports

    def clean_reports(self, reports):
        gapless_reports = self.get_gapless_reports(reports)
        observations_qty = self.calculate_observations_quantity(gapless_reports)
        if self.MAXIMUM_OBSERVATIONS_QTY < observations_qty:
            clean_reports = self.drop_unnecessary_reports(gapless_reports)
        else:
            clean_reports = gapless_reports
        return clean_reports

    def add_back_up_reports(self, reports):
        # If there are less than the minimum observations needed to process in the installation directory and there
        # are no reports in the back up directory, then we assume that this might be a fresh process and leave
        if self.back_up_dir_is_empty():
            return reports
        # If there are less than the minimum observations needed to process in the installation directory
        # and the amount of back up reports needed for processing is too much we simply establish that there
        # was a connection loss or something that renders the last minutes impossible to measure.
        # So we clean the back up files, but we leave the main reports in hopes that there's a burst with which we
        # can work
        observations_qty = self.calculate_observations_quantity(reports)
        needed_back_up_observations = self.MINIMUM_OBSERVATIONS_QTY - observations_qty
        if needed_back_up_observations > self.BACK_UP_OBSERVATIONS_QTY_PROCESSING_THRESHOLD:
            self.clean_back_up_dir()
            return reports
        back_up_reports = self.get_back_up_reports(reports_quantity=None)
        needed_back_up_reports = []
        while needed_back_up_observations > 0:
            back_up_report = back_up_reports.pop()
            needed_back_up_reports.append(back_up_report)
            needed_back_up_observations -= len(back_up_report.observations)
        reports.extend(needed_back_up_reports)
        reports.sort(key=self.reports_sorting_key)
        return reports

    def drop_unnecessary_reports(self, reports):
        observations_qty = 0
        processable_reports = list()
        while observations_qty < self.MAXIMUM_OBSERVATIONS_QTY:
            report = reports.pop(0)
            processable_reports.append(report)
            observations_qty = self.calculate_observations_quantity(processable_reports)
        return processable_reports

    def get_processable_reports(self):
        reports = self.get_unprocessed_reports()
        clean_reports = self.clean_reports(reports)
        observations_qty = self.calculate_observations_quantity(clean_reports)
        if observations_qty < self.MINIMUM_OBSERVATIONS_QTY:
            reports_with_back_up = self.add_back_up_reports(clean_reports)
            clean_reports_with_back_up = self.clean_reports(reports_with_back_up)
            observations_qty = self.calculate_observations_quantity(clean_reports_with_back_up)
            if self.MINIMUM_OBSERVATIONS_QTY <= observations_qty:
                processable_reports = clean_reports_with_back_up
            else:
                self.delete_reports_files(clean_reports)
                self.clean_back_up_dir()
                processable_reports = list()
        else:
            processable_reports = clean_reports
        return processable_reports

    def get_processable_observations(self):
        log = logger.getChild('get_datapoints')
        log.info('getting datapoints')
        reports = self.get_processable_reports()
        observations = self.collect_observations(reports)
        return observations

    def back_up_reports(self, reports):
        for report in reports:
            if not report.file_path.startswith(self.back_up_reports_dir_path) and exists(report.file_path):
                split_file_path = report.file_path.split(os.sep)
                report_file_name = split_file_path[-1]
                new_report_file_path = join(self.back_up_reports_dir_path, report_file_name)
                rename(report.file_path, new_report_file_path)
                report.file_path = new_report_file_path

    def failed_results_dir_is_empty(self):
        return not exists(self.failed_results_dir_path) or len(listdir(self.failed_results_dir_path)) == 0

    def back_up_failed_results(self, results, ip):
        json_failed_results = {
            'results': results,
            'ip': ip
        }
        failed_result_file_name = self.FAILED_REPORT_FILE_NAME_TEMPLATE.format(timestamp=results['timestamp'])
        failed_result_file_path = join(self.failed_results_dir_path, failed_result_file_name)
        with open(failed_result_file_path) as failed_result_file:
            json.dump(json_failed_results, failed_result_file)
