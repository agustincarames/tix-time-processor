import logging
import os

from celery import Celery

REPORTS_BASE_PATH = os.environ.get('TIX_REPORTS_BASE_PATH', '/tmp/reports')
PROCESSING_PERIOD = int(os.environ.get('TIX_PROCESSING_PERIOD', '5'))
RABBITMQ_USER = os.environ.get('TIX_RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.environ.get('TIX_RABBITMQ_PASS', 'guest')
RABBITMQ_HOST = os.environ.get('TIX_RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = os.environ.get('TIX_RABBITMQ_PORT', '5672')

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

app = Celery('processor.tasks',
             broker='amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:{rabbitmq_port}//'.format(
                 rabbitmq_user=RABBITMQ_USER,
                 rabbitmq_pass=RABBITMQ_PASS,
                 rabbitmq_host=RABBITMQ_HOST,
                 rabbitmq_port=RABBITMQ_PORT
             ))
