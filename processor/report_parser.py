import base64
import json

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
    def loads(report_json):
        report = json.loads(fp, cls=ReportJSONDecoder)
        report.file_path = None

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
