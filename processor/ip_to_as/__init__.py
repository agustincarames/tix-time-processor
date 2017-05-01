import os

from processor.ip_to_as.funciones import findasxip, findnombrexas

DATABASE_HOST = os.environ.get('TIX_DB_HOST', 'localhost')
DATABASE_USER = os.environ.get('TIX_DB_USER', 'root')
DATABASE_PASS = os.environ.get('TIX_DB_PASS')
DATABASE_SCHEMA = os.environ.get('TIX_DB_SCHEMA', 'ip_to_as')


def get_as_by_ip(from_socket_address):
    ip_address = from_socket_address.split(':')[0]
    as_id = findasxip(ip_address)
    as_owner = findnombrexas(as_id)
    as_info = {
        'id': as_id,
        'owner': as_owner
    }
    return as_info
