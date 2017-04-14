import os

import logging
from celery import Celery
from celery.schedules import crontab

REPORTS_BASE_PATH = os.environ.get('TIX_REPORTS_BASE_PATH', '/tmp/reports')

app = Celery("processing",
             broker="amqp://guest:guest@localhost:5672//")

logger = logging.getLogger('processing.__init__')


