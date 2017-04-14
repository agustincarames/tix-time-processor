import logging
from os import listdir

from celery.schedules import crontab
from os.path import isdir, join

from processing import app, REPORTS_BASE_PATH

tasks_logger = logging.getLogger(__name__)


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(minute='*/10'),
        process_users_data.s(),
        name='process_users_data')


@app.task
def process_installation(installation_dir_path):
    logger = tasks_logger.getChild('process_installation')
    logger.info(installation_dir_path)


@app.task
def test_task(string):
    logger = tasks_logger.getChild('test_task')
    logger.info(string)
    print(string)


@app.task
def process_users_data():
    for first_file in listdir(REPORTS_BASE_PATH):
        first_file_path = join(REPORTS_BASE_PATH, first_file)
        if isdir(first_file_path):
            user_dir_path = first_file_path
            for second_file in listdir(user_dir_path):
                second_file_path = join(user_dir_path, second_file)
                if isdir(second_file_path):
                    installation_dir_path = second_file_path
                    process_installation.delay(installation_dir_path)
