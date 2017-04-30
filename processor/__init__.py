import json
import logging
import os
from os import listdir
from os.path import join, isdir

from celery import Celery
from math import sqrt, floor

REPORTS_BASE_PATH = os.environ.get('TIX_REPORTS_BASE_PATH', '/tmp/reports')
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


def get_datapoints(installation_dir_path):
    log = logger.getChild('get_datapoints')
    log.info('getting datapoints')
    # reports = {}
    # for file_name in sorted(listdir(installation_dir_path)):
    #     if file_name.endswith('.json'):
    #         file_path = join(installation_dir_path, file_name)
    #         if not isdir(file_path):
    #             with open(file_path) as report_file:
    #                 report = json.load(report_file)
    #             report_as = get_as(report['from'])
    #             if report_as['as_id'] not in reports:
    #                 reports[report_as['as_id']] = {
    #                     'as_id': report_as['as_id'],
    #                     'as_owner': report_as['as_owner'],
    #                     'report': list()
    #                 }
    #             reports[report_as['as_id']]['report'].append({
    #                 'file_name': file_name,
    #                 'message': deserealize_message(report['message'])
    #             })
    pass


def datapoint_rtt_key_function(datapoint):
    return datapoint['t4'] - datapoint['t1']


def generate_histogram(datapoints):
    datapoints = sorted(datapoints, key=datapoint_rtt_key_function)
    bines_qty = int(floor(sqrt(len(datapoints))))
    datapoints_per_bin = len(datapoints) / bines_qty
    histogram = [[]] * bines_qty
    index = 0
    # Create a histogram with the same amount of datapoint in each bin
    for hbin in histogram:
        hbin.extend(datapoints[index:datapoints_per_bin])
        index += datapoints_per_bin
    # If there still some datapoints left, we add them to the last bin
    if index < len(datapoints):
        histogram[-1].extend(datapoints[index:])
    return histogram


def bin_width(hbin):
    max_rtt = max(hbin, key=datapoint_rtt_key_function)
    min_rtt = min(hbin, key=datapoint_rtt_key_function)
    bin_width = max_rtt - min_rtt
    return bin_width, max_rtt, min_rtt


def get_bins_probabilities(histogram):
    probabilities = []
    total_datapoints = sum([len(hbin) for hbin in histogram])
    for hbin in histogram:
        hbin_width = bin_width(hbin)[0]
        probabilities.append(len(hbin) / (total_datapoints * hbin_width))
    return probabilities


def process_data_points(datapoints):
    log = logger.getChild('process_data_points')
    log.info('processing data points')
    histogram = generate_histogram(datapoints)
    probabilities = get_bins_probabilities(histogram)
    mode = max(probabilities)
    if probabilities[0] == mode:
        hbin_width, max_rtt, min_rtt = bin_width(histogram[1])
        threshold = min_rtt + int(hbin_width / 2)
    else:
        ALPHA = 1
        mode_index = probabilities.index(mode)
        min_bin_min_rtt = bin_width(histogram[0])[2]
        threshold = mode_index + ALPHA * min_bin_min_rtt
    pass


def post_results(results, user_id, installation_id):
    log = logger.getChild('post_results')
    log.info('posting results')
    pass
