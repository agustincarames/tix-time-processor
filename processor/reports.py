import base64
import datetime
import json
import os
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


def nanos_to_micros(nanos):
    return nanos // 10 ** 3


class Observation:
    def __init__(self, day_timestamp, type_identifier, packet_size,
                 initial_timestamp, reception_timestamp, sent_timestamp, final_timestamp):
        self.day_timestamp = day_timestamp
        self.type_identifier = type_identifier
        self.packet_size = packet_size
        self.initial_timestamp_nanos = initial_timestamp
        self.reception_timestamp_nanos = reception_timestamp
        self.sent_timestamp_nanos = sent_timestamp
        self.final_timestamp_nanos = final_timestamp
        self.upstream_phi = 0.0
        self.downstream_phi = 0.0
        self.estimated_phi = 0.0

    @property
    def initial_timestamp(self):
        return self.initial_timestamp_nanos

    @property
    def reception_timestamp(self):
        return self.reception_timestamp_nanos

    @property
    def sent_timestamp(self):
        return self.sent_timestamp_nanos

    @property
    def final_timestamp(self):
        return self.final_timestamp_nanos

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __hash__(self):
        return hash((self.day_timestamp,
                     self.type_identifier,
                     self.packet_size,
                     self.initial_timestamp_nanos,
                     self.reception_timestamp_nanos,
                     self.sent_timestamp_nanos,
                     self.final_timestamp_nanos))

    def __repr__(self):
        return '{0!s}({1!r})'.format(self.__class__, self.__dict__)


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
            "enum": ["LONG"]
        },
        "initialTimestamp": {"type": "integer"},
        "receptionTimestamp": {"type": "integer"},
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
        "initialTimestamp", "receptionTimestamp", "sentTimestamp", "finalTimestamp",
        "publicKey", "message", "signature",
        "userId", "installationId"
    ]
}


class ReportJSONEncoder(json.JSONEncoder):
    @staticmethod
    def report_to_dict(report_object):
        report_dict = report_object.__dict__.copy()
        for field_translation in JSON_FIELDS_TRANSLATIONS:
            field_value = report_dict.pop(field_translation.translation)
            report_dict[field_translation.original] = field_translation.reverse_translate(field_value)
        report_dict_fields = list(report_dict.keys())
        for field in report_dict_fields:
            inflexed_key = inflection.camelize(field, False)
            report_dict[inflexed_key] = report_dict.pop(field)
        fields_to_delete = []
        report_dict_fields = list(report_dict.keys())
        for field in report_dict_fields:
            if field not in JSON_REPORT_SCHEMA['required']:
                fields_to_delete.append(field)
        for field in fields_to_delete:
            report_dict.pop(field)
        return report_dict

    def default(self, obj):
        if isinstance(obj, Report):
            json_dict = self.report_to_dict(obj)
        else:
            json_dict = json.JSONEncoder.default(self, obj)
        return json_dict


class ReportJSONDecoder(json.JSONDecoder):
    @staticmethod
    def dict_to_report(json_dict):
        json_dict_keys = list(json_dict.keys())
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
            inst = self.dict_to_report(d)
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
    def get_gap_between_reports(second_report, first_report):
        return second_report.observations[0].day_timestamp - first_report.observations[0].day_timestamp

    def __init__(self,
                 from_dir, to_dir, packet_type,
                 initial_timestamp, reception_timestamp, sent_timestamp, final_timestamp,
                 public_key, observations, signature,
                 user_id, installation_id, file_path=None):
        self.from_dir = from_dir
        self.to_dir = to_dir
        self.packet_type = packet_type
        self.initial_timestamp = initial_timestamp
        self.reception_timestamp = reception_timestamp
        self.sent_timestamp = sent_timestamp
        self.final_timestamp = final_timestamp
        self.public_key = public_key
        self.observations = observations
        self.signature = signature
        self.user_id = user_id
        self.installation_id = installation_id
        self.file_path = file_path

    def get_observations_gap(self):
        return self.observations[-1].day_timestamp - self.observations[0].day_timestamp

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __hash__(self):
        return hash((self.from_dir,
                     self.to_dir,
                     self.packet_type,
                     self.initial_timestamp,
                     self.reception_timestamp,
                     self.sent_timestamp,
                     self.final_timestamp,
                     self.public_key,
                     self.observations,
                     self.signature,
                     self.user_id,
                     self.installation_id))

    def __repr__(self):
        return '{0!s}({1!r})'.format(self.__class__, self.__dict__)


class NotEnoughObservationsError(Exception):
    pass


class ReportHandler:
    MINIMUM_OBSERVATIONS_QTY = 1024
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
        self.__processable_reports = list()
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

    def __get_processable_reports(self):
        self.__update_reports_files()
        processable_reports = list()
        while self.calculate_observations_quantity(processable_reports) < self.MINIMUM_OBSERVATIONS_QTY and \
                        len(self.reports_files) > 0:
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
            self.__processable_reports = list()
        else:
            self.__processable_reports = processable_reports

    def get_ip_and_processable_observations(self):
        self.__get_processable_reports()
        if len(self.__processable_reports) == 0:
            ip, observations = None, None
        else:
            ip, observations = self.collect_observations(self.__processable_reports)
        return ip, observations

    def delete_unneeded_reports(self):
        reports_to_delete_qty = len(self.__processable_reports) // 2
        reports_to_delete = list()
        for i in range(reports_to_delete_qty):
            reports_to_delete.append(self.__processable_reports.pop(i))
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
