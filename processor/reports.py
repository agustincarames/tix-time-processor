import base64
import json
from operator import itemgetter
from os import listdir, unlink, mkdir

from os.path import join, exists, isfile, islink

import logging

import struct

import inflection
import jsonschema

OBSERVATIONS_PER_REPORT = 60
MINIMUM_OBSERVATIONS_QTY = 1024
MINIMUM_REPORTS_QTY = int(MINIMUM_OBSERVATIONS_QTY * 1.2 / OBSERVATIONS_PER_REPORT)
BACK_UP_REPORTS_PROCESSING_THRESHOLD = 5
REPORTS_GAP_THRESHOLD = OBSERVATIONS_PER_REPORT * 3

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
JSON_FIELDS_TRANSLATIONS = [
    FieldTranslation("from", "from_dir"),
    FieldTranslation("to", "to_dir"),
    FieldTranslation("type", "packet_type"),
    FieldTranslation("message", "observations", deserialize_observations, serialize_observations)
]


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
        return report_dict

    def default(self, obj):
        if isinstance(obj, Report):
            json_dict = self.object_to_json(obj)
        else:
            json_dict = json.JSONEncoder.default(self, obj)
        return json_dict


class ReportJSONDecoder(json.JSONDecoder):
    def json_to_object(self, json_dict):
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
    def __init__(self,
                 from_dir, to_dir, packet_type,
                 initial_timestamp, received_timestamp, sent_timestamp, final_timestamp,
                 public_key, observations, signature,
                 user_id, installation_id):
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

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented


class ReportFileMetadata:
    def __init__(self, file_path, file_name):
        self.file_path = file_path
        self.file_name = file_name


def get_reports_files(installation_dir_path):
    reports_files = []
    for file_name in sorted(listdir(installation_dir_path)):
        if file_name.endswith('.json'):
            file_path = join(installation_dir_path, file_name)
            if isfile(file_path) and not islink(file_path):
                reports_files.append(ReportFileMetadata(file_path, file_name))
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
                back_up_reports.append(ReportFileMetadata(file_path, file_name))
    return back_up_reports


def clean_back_up_dir(installation_dir_path):
    back_up_reports_path = join(installation_dir_path, BACK_UP_REPORTS_DIR_NAME)
    for file_name in listdir(back_up_reports_path):
        file_path = join(back_up_reports_path, file_name)
        if isfile(file_path) and not islink(file_path):
            unlink(file_path)


def get_report_object(report_file):
    with open(report_file.file_path) as fp:
        report = json.load(fp, cls=ReportJSONDecoder)
    return report


def get_report_object_message_bytes(report_object):
    message_bytes = base64.b64decode(report_object.message)
    return message_bytes


def get_report_gap(report_file):
    report = get_report_object(report_file)
    return report.observations[-1].day_timestamp - report.observations[0].day_timestamp


def get_gap_between_reports(second_report_file, first_report_file):
    second_report = get_report_object(second_report_file)
    first_report = get_report_object(first_report_file)
    return second_report.observations[0].day_timestamp - first_report.observations[0].day_timestamp


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
        unlink(report_file.file_path)


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


def extract_processable_data(reports_files_metadata):
    data_per_ip = {}
    for report_file_metadata in reports_files_metadata:
        report_object = get_report_object(report_file_metadata)
        ip = report_object.from_dir
        if ip not in data_per_ip:
            data_per_ip[ip] = list()
        data_per_ip[ip].extend(report_object.observations)
    for ip, observations in data_per_ip.items():
        if len(observations) >= MINIMUM_OBSERVATIONS_QTY:
            return ip, observations
    raise ValueError('Expected at least 1 AS with {} observations. None found.'.format(MINIMUM_OBSERVATIONS_QTY))


def get_data(installation_dir_path):
    log = logger.getChild('get_datapoints')
    log.info('getting datapoints')
    reports_files = get_processable_report_files(installation_dir_path)
    data = extract_processable_data(reports_files)
    return data


def failed_results_dir_is_empty(installation_dir_path):
    failed_results_dir_path = join(installation_dir_path, FAILED_REPORTS_DIR_NAME)
    return not exists(failed_results_dir_path) or len(listdir(failed_results_dir_path)) == 0


def back_up_failed_results(installation_dir_path, results, ip):
    failed_results_dir_path = join(installation_dir_path, FAILED_REPORTS_DIR_NAME)
    json_failed_results = {
        'results': results,
        'ip': ip
    }
    if not exists(failed_results_dir_path):
        mkdir(failed_results_dir_path)
    failed_result_file_name = FAILED_REPORT_FILE_NAME_TEMPLATE.format(timestamp=results['timestamp'])
    failed_result_file_path = join(failed_results_dir_path, failed_result_file_name)
    with open(failed_result_file_path) as failed_result_file:
        json.dump(json_failed_results, failed_result_file)
