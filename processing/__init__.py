import os

import logging
from celery import Celery
from celery.schedules import crontab

REPORTS_BASE_PATH = os.environ.get('TIX_REPORTS_BASE_PATH', '/tmp/reports')

app = Celery("processing",
             broker="amqp://guest:guest@localhost:5672//")

logger = logging.getLogger('processing.__init__')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(minute='*/10'),
        process_users_data.s(),
        name='process_users_data')


@app.task
def process_installation(installation_dir_path):
    logger.info(installation_dir_path)


@app.task
def test_task(string):
    logger.info(string)
    print(string)


@app.task
def process_users_data():
    for first_file in os.listdir(REPORTS_BASE_PATH):
        first_file_path = os.path.join(REPORTS_BASE_PATH, first_file)
        if os.path.isdir(first_file_path):
            user_dir_path = first_file_path
            for second_file in os.listdir(user_dir_path):
                second_file_path = os.path.join(user_dir_path, second_file)
                if os.path.isdir(second_file_path):
                    installation_dir_path = second_file_path
                    process_installation.delay(installation_dir_path)


