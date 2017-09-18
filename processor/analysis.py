from datetime import datetime, timedelta, timezone
import logging
import statistics
from functools import partial
from operator import attrgetter

import numpy as np
from scipy import stats
from math import floor, sqrt, log as log_function

from processor import hurst

UPSTREAM_SERIALIZATION_TIME = 15 * (10 ** 3)  # 15 micro
DOWNSTREAM_SERIALIZATION_TIME = 15 * (10 ** 3)  # 15 micro

MEANINGFUL_OBSERVATIONS_DELTA = timedelta(minutes=10)

CONGESTION_THRESHOLD = 0.5
HURST_CONGESTION_THRESHOLD = 0.7

logger = logging.getLogger(__name__)


def observation_rtt_key_function(observation):
    return observation.final_timestamp - observation.initial_timestamp


def upstream_time_function(observation, phi_function):
    return observation.reception_timestamp - phi_function(observation.day_timestamp) \
           - observation.initial_timestamp


def downstream_time_function(observation, phi_function):
    return observation.final_timestamp - observation.sent_timestamp \
           + phi_function(observation.day_timestamp)


def generate_histogram(observations, histogram_sorting_key_function):
    observations = sorted(observations, key=histogram_sorting_key_function)
    bines_qty = int(floor(sqrt(len(observations))))
    datapoints_per_bin = len(observations) // bines_qty
    histogram = list()
    for _ in range(bines_qty):
        histogram.append(list())
    index = 0
    # Create a histogram with the same amount of observation in each bin
    for hbin in histogram:
        threshold = index + datapoints_per_bin
        hbin.extend(observations[index:threshold])
        index += datapoints_per_bin
    # If there still some observations left, we add them to the last bin
    if index < len(observations):
        histogram[-1].extend(observations[index:])
    return histogram


def bin_info(hbin, histogram_sorting_key_function):
    max_rtt = histogram_sorting_key_function(max(hbin, key=histogram_sorting_key_function))
    min_rtt = histogram_sorting_key_function(min(hbin, key=histogram_sorting_key_function))
    bin_width = max_rtt - min_rtt
    return bin_width, max_rtt, min_rtt


def get_bins_probabilities(histogram, histogram_sorting_key_function):
    probabilities = []
    total_datapoints = sum([len(hbin) for hbin in histogram])
    for hbin in histogram:
        hbin_width = bin_info(hbin, histogram_sorting_key_function)[0]
        probabilities.append(len(hbin) / (total_datapoints * hbin_width))
    return probabilities


def calculate_desired_length(incoming_length):
    return int(2 ** floor(log_function(incoming_length, 2)))


def cap_observations(observations):
    desired_length = calculate_desired_length(len(observations))
    capped_observations = observations[-desired_length:]
    return capped_observations


def get_mode_and_threshold(histogram, histogram_sorting_key_function, alpha=1):
    probabilities = get_bins_probabilities(histogram, histogram_sorting_key_function)
    mode = max(probabilities)
    mode_index = probabilities.index(mode)
    mode_bin_width, mode_bin_max_value, mode_bin_min_value = bin_info(histogram[mode_index],
                                                                      histogram_sorting_key_function)
    mode_value = mode_bin_min_value + mode_bin_width / 2
    if probabilities[0] == mode:
        bin_width, bin_max_value, bin_min_value = bin_info(histogram[1], histogram_sorting_key_function)
        threshold = bin_min_value + int(bin_width / 2)
    else:
        min_bin_width, min_bin_max_value, min_bin_min_value = bin_info(histogram[0], histogram_sorting_key_function)
        threshold = mode_value + alpha * (min_bin_min_value + int(min_bin_width / 2))
    return mode_value, threshold


def characterize_observations(observations, characterization_key_function):
    histogram = generate_histogram(observations, histogram_sorting_key_function=characterization_key_function)
    mode_characterization, threshold = get_mode_and_threshold(histogram, characterization_key_function)
    # characterizations = {characterization_key_function(observation) for observation in observations}
    # observations_by_characterization = dict()
    # for observation in observations:
    #     characterization = characterization_key_function(observation)
    #     if characterization not in observations_by_characterization:
    #         observations_by_characterization[characterization] = list()
    #     observations_by_characterization[characterization].append(observation)
    # mode_characterization = max(observations_by_characterization, key=(lambda characterization:
    #                                                   len(observations_by_characterization[characterization])))
    # min_characterization = min(observations_by_characterization)
    # threshold = mode_characterization + min_characterization + ((mode_characterization - min_characterization) // 2)
    return mode_characterization, threshold


def divide_observations_into_minutes(observations):
    observations_per_minute = {}
    for observation in observations:
        observation_datetime = datetime.fromtimestamp(observation.day_timestamp, timezone.utc)
        observation_minute = observation_datetime.replace(second=0, microsecond=0).timestamp()
        if observation_minute not in observations_per_minute:
            observations_per_minute[observation_minute] = []
        observations_per_minute[observation_minute].append(observation)
    return observations_per_minute


def get_phi_function_parameters(phis_per_minute):
    minutes, phis = tuple(zip(*phis_per_minute))
    minutes_arr = np.asarray(minutes)
    phis_arr = np.asarray(phis)
    slope, intercept, r_value, p_value, std_err = stats.linregress(minutes_arr, phis_arr)
    return slope, intercept


def get_phis_per_minute(observations):
    observations_per_minute = divide_observations_into_minutes(observations)
    phis = []
    for minute, observations in observations_per_minute.items():
        bucket_phi = statistics.median([observation.estimated_phi for observation in observations])
        phis.append((minute, bucket_phi))
    return phis


def generate_observations_with_clocks_corrections(observations, tau,
                                                  downstream_serialization_time=DOWNSTREAM_SERIALIZATION_TIME,
                                                  upstream_serialization_time=UPSTREAM_SERIALIZATION_TIME):
    for observation in observations:
        upstream_phi = observation.reception_timestamp - observation.initial_timestamp \
                       - upstream_serialization_time - tau
        downstream_phi = observation.sent_timestamp - observation.final_timestamp \
                         + downstream_serialization_time + tau
        estimated_phi = (downstream_phi + upstream_phi) / 2
        observation.upstream_phi = upstream_phi
        observation.downstream_phi = downstream_phi
        observation.estimated_phi = estimated_phi
    return observations


def base_phi_function(slope, intercept, x):
    return x * slope + intercept


def get_phi_function(observations, tau):
    observations_with_phi = generate_observations_with_clocks_corrections(observations, tau)
    phis_per_minute = get_phis_per_minute(observations_with_phi)
    slope, intercept = get_phi_function_parameters(phis_per_minute)
    return partial(base_phi_function, slope, intercept)


def get_hurst_value(data):
    wavelet_hurst = hurst.wavelet(data)
    rs_hurst = hurst.rs(data)
    return {
        'wavelet': wavelet_hurst,
        'rs': rs_hurst
    }


def calculate_effective_hurst(hurst_dict):
    return (hurst_dict['wavelet'] + hurst_dict['rs']) / 2


def get_hurst_values(observations, phi_function):
    capped_observations = cap_observations(observations)
    upstream_times = []
    downstream_times = []
    for observation in capped_observations:
        upstream_time = upstream_time_function(observation, phi_function)
        downstream_time = downstream_time_function(observation, phi_function)
        upstream_times.append(upstream_time)
        downstream_times.append(downstream_time)
    upstream_hurst = get_hurst_value(upstream_times)
    downstream_hurst = get_hurst_value(downstream_times)
    return upstream_hurst, downstream_hurst


def get_meaningful_observations(observations):
    sorted_observations = sorted(observations, key=attrgetter('day_timestamp'))
    first_observation = sorted_observations[0]
    last_observation = sorted_observations[-1]
    observations_delta = timedelta(seconds=(last_observation.day_timestamp - first_observation.day_timestamp))
    if observations_delta < MEANINGFUL_OBSERVATIONS_DELTA:
        raise ValueError('Meaningful observations time delta is lower than expected. Expected {}, got {}'\
                         .format(MEANINGFUL_OBSERVATIONS_DELTA, observations_delta))
    meaningful_threshold_timestamp = last_observation.day_timestamp - MEANINGFUL_OBSERVATIONS_DELTA.total_seconds()
    meaningful_observations = [observation for observation in observations
                               if observation.day_timestamp > meaningful_threshold_timestamp]
    return meaningful_observations


def get_usage(observations, phi_function):
    upstream_time_key_function = partial(upstream_time_function, phi_function=phi_function)
    downstream_time_key_function = partial(downstream_time_function, phi_function=phi_function)
    upstream_mode, upstream_threshold = characterize_observations(observations, upstream_time_key_function)
    downstream_mode, downstream_threshold = characterize_observations(observations, downstream_time_key_function)
    upstream_over_threshold = 0
    downstream_over_threshold = 0
    for observation in observations:
        upstream_time = upstream_time_key_function(observation)
        downstream_time = downstream_time_key_function(observation)
        if upstream_time > upstream_threshold:
            upstream_over_threshold += 1
        if downstream_time > downstream_threshold:
            downstream_over_threshold += 1
    upstream_usage = upstream_over_threshold / len(observations)
    downstream_usage = downstream_over_threshold / len(observations)
    return upstream_usage, downstream_usage


def get_quality(observations, upstream_hurst, downstream_hurst, phi_function):
    observations_per_minute = divide_observations_into_minutes(observations)
    upstream_congestion = 0
    downstream_congestion = 0
    effective_upstream_hurst = calculate_effective_hurst(upstream_hurst)
    effective_downstream_hurst = calculate_effective_hurst(downstream_hurst)
    obspm_items = list(observations_per_minute.items())
    for minute, m_observations in obspm_items:
        if len(m_observations) < 30:
            observations_per_minute.pop(minute, None)
    for minute, m_observations in observations_per_minute.items():
        upstream_usage, downstream_usage = get_usage(m_observations, phi_function)
        if upstream_usage < CONGESTION_THRESHOLD and effective_upstream_hurst > HURST_CONGESTION_THRESHOLD:
            upstream_congestion += 1
        if downstream_usage < CONGESTION_THRESHOLD and effective_downstream_hurst > HURST_CONGESTION_THRESHOLD:
            downstream_congestion += 1
    upstream_quality = (len(observations_per_minute) - upstream_congestion) / len(observations_per_minute)
    downstream_quality = (len(observations_per_minute) - downstream_congestion) / len(observations_per_minute)
    return upstream_quality, downstream_quality


def process_observations(observations):
    log = logger.getChild('process_data_points')
    log.info('processing data points')
    short_packets_observations = [observation for observation in observations if observation.type_identifier == b'S']
    tau, tau_threshold = characterize_observations(short_packets_observations, observation_rtt_key_function)
    phi_function = get_phi_function(short_packets_observations, tau)
    upstream_hurst, downstream_hurst = get_hurst_values(short_packets_observations, phi_function)
    meaningful_observations = get_meaningful_observations(short_packets_observations)
    upstream_usage, downstream_usage = get_usage(meaningful_observations, phi_function)
    upstream_quality, downstream_quality = get_quality(meaningful_observations,
                                                       upstream_hurst=upstream_hurst,
                                                       downstream_hurst=downstream_hurst,
                                                       phi_function=phi_function)
    return {
        'timestamp': meaningful_observations[-1].day_timestamp,
        'upstream': {
            'usage': upstream_usage,
            'quality': upstream_quality,
            'hurst': upstream_hurst
        },
        'downstream': {
            'usage': downstream_usage,
            'quality': downstream_quality,
            'hurst': downstream_hurst
        }
    }
