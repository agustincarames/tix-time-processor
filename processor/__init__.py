import logging
import os
from os import listdir
from os.path import join, isdir

from celery import Celery
from celery.schedules import crontab

REPORTS_BASE_PATH = os.environ.get('TIX_REPORTS_BASE_PATH', '/tmp/reports')
RABBITMQ_USER = os.environ.get('TIX_RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.environ.get('TIX_RABBITMQ_PASS', 'guest')
RABBITMQ_HOST = os.environ.get('TIX_RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = os.environ.get('TIX_RABBITMQ_PORT', '5672')


logging.basicConfig(level=logging.DEBUG)
tasks_logger = logging.getLogger(__name__)

app = Celery("processor",
             broker="amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:{rabbitmq_port}//".format(
                 rabbitmq_user=RABBITMQ_USER,
                 rabbitmq_pass=RABBITMQ_PASS,
                 rabbitmq_host=RABBITMQ_HOST,
                 rabbitmq_port=RABBITMQ_PORT
             ))


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(minute='*/1'),  # crontab(minute='*/10'),
        process_users_data.s(REPORTS_BASE_PATH),
        name='process_users_data')


@app.task
def process_installation(installation_dir_path):
    logger = tasks_logger.getChild('process_installation')
    logger.info('installation_dir_path: {installation_dir_path}'.format(installation_dir_path=installation_dir_path))


@app.task
def process_users_data(reports_base_path):
    logger = tasks_logger.getChild('process_users_data')
    logger.info('Processing users data')
    logger.debug('reports_base_path: {reports_base_path}'.format(reports_base_path=reports_base_path))
    for first_file in listdir(reports_base_path):
        first_file_path = join(reports_base_path, first_file)
        if isdir(first_file_path):
            user_dir_path = first_file_path
            logger.debug('user_dir_path: {user_dir_path}'.format(user_dir_path=user_dir_path))
            for second_file in listdir(user_dir_path):
                second_file_path = join(user_dir_path, second_file)
                if isdir(second_file_path):
                    installation_dir_path = second_file_path
                    logger.debug('installation_dir_path: {installation_dir_path}'
                                 .format(installation_dir_path=installation_dir_path))
                    process_installation.delay(installation_dir_path)

