import logging
import os

from celery import Celery

REPORTS_BASE_PATH = os.environ.get('TIX_REPORTS_BASE_PATH', '/tmp/reports')
PROCESSING_PERIOD = int(os.environ.get('TIX_PROCESSING_PERIOD', '5'))
RABBITMQ_USER = os.environ.get('TIX_RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.environ.get('TIX_RABBITMQ_PASS', 'guest')
RABBITMQ_HOST = os.environ.get('TIX_RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = os.environ.get('TIX_RABBITMQ_PORT', '5672')


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = Celery('processor.tasks',
             broker='amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:{rabbitmq_port}//'.format(
                 rabbitmq_user=RABBITMQ_USER,
                 rabbitmq_pass=RABBITMQ_PASS,
                 rabbitmq_host=RABBITMQ_HOST,
                 rabbitmq_port=RABBITMQ_PORT
             ))
