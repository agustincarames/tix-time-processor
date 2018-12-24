import logging
import os

RABBITMQ_USER = os.environ.get('TIX_RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.environ.get('TIX_RABBITMQ_PASS', 'guest')
RABBITMQ_HOST = os.environ.get('TIX_RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = os.environ.get('TIX_RABBITMQ_PORT', '5672')

RABBITMQ_INCOMING_QUEUE = os.environ.get('TIX_CONDENSER_PROCESSOR_QUEUE')

LOG_LEVEL = os.environ.get('TIX_LOG_LEVEL', 'INFO')
log_levels = {
    'FATAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARN': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'ALL': 1
}

logger = logging.getLogger()
level = log_levels.get(LOG_LEVEL, logging.DEBUG);
logger.fatal('Log level at {level}'.format(level=level))
logging.basicConfig(level=level)
