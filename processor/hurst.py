import logging

from math import floor, sqrt, log as log_function

logger = logging.getLogger(__name__)


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


def separate_datapoints(datapoints):
    short_packets_datapoints = []
    long_packets_datapoints = []
    for datapoint in datapoints:
        if datapoint['type'] == 'S':
            short_packets_datapoints.append(datapoint)
        elif datapoint['type'] == 'L':
            long_packets_datapoints.append(datapoint)
        else:
            raise ValueError('Unknown data point packet type: {}'.format(datapoint['type']))
    return short_packets_datapoints, long_packets_datapoints


def calculate_desired_length(incoming_length):
    return int(2 ** floor(log_function(incoming_length, 2)))


def cap_observations(observations):
    desired_length = calculate_desired_length(len(observations))
    capped_observations = observations[-desired_length:]
    return capped_observations


def analyze_data_points(observations):
    capped_observations = cap_observations(observations)
    histogram = generate_histogram(capped_observations)
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


def process_observations(datapoints):
    log = logger.getChild('process_data_points')
    log.info('processing data points')
    short_packets_datapoints, long_packets_datapoints = separate_datapoints(datapoints)
    analyze_data_points(short_packets_datapoints)
    analyze_data_points(long_packets_datapoints)
    pass
