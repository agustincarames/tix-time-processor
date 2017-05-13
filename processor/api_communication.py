import logging
import os

import requests
from requests import RequestException
from requests.auth import HTTPBasicAuth

TIX_API_SSL = os.environ.get('TIX_API_SSL', None) is not None
TIX_API_HOST = os.environ.get('TIX_API_HOST', 'localhost')
TIX_API_PORT = os.environ.get('TIX_API_PORT', '80')
TIX_API_USER = os.environ.get('TIX_API_USER')
TIX_API_PASS = os.environ.get('TIX_API_PASSWORD')

logger = logging.getLogger(__name__)


def prepare_results_for_api(results, ip):
    return {
        'timestamp': results['timestamp'],
        'upUsage':  results['upstream']['usage'],
        'upQuality': results['upstream']['quality'],
        'downUsage': results['downstream']['usage'],
        'downQuality': results['downstream']['quality'],
        'hurstUpRs':  results['upstream']['hurst']['rs'],
        'hurstUpWavelet': results['upstream']['hurst']['wavelet'],
        'hurstDownRs': results['downstream']['hurst']['rs'],
        'hurstDownWavelet': results['downstream']['hurst']['wavelet'],
        'ipAddress': ip
    }


def prepare_url(user_id, installation_id):
    if TIX_API_SSL:
        proto = 'https'
    else:
        proto = 'http'
    url = '{proto}://{api_host}:{api_port}/api/user/{user_id}/installation/{installation_id}/report'.format(
        proto=proto,
        api_host=TIX_API_HOST,
        api_port=TIX_API_PORT,
        user_id=user_id,
        installation_id=installation_id
    )
    return url


def post_results(ip, results, user_id, installation_id):
    log = logger.getChild('post_results')
    log.info('posting results for user {user_id} installation {installation_id}'.format(user_id=user_id,
                                                                                        installation_id=installation_id))
    json_data = prepare_results_for_api(results, ip)
    log.debug('json_data={json_data}'.format(json_data=json_data))
    url = prepare_url(user_id, installation_id)
    log.debug('url={url}'.format(url=url))
    if not TIX_API_USER or not TIX_API_PASS:
        log.warn('No user nor password supplied for API Connection')
        return False
    try:
        response = requests.post(url=url,
                                 json=json_data,
                                 auth=HTTPBasicAuth(TIX_API_USER, TIX_API_PASS))
        if response.status_code not in (200, 204):
            log.error('Error while trying to post to API, got status code {status_code} for url {url}'
                      .format(status_code=response.status_code,
                              url=url))
            return False
    except RequestException as re:
        log.error('Error while trying to post to API')
        log.error(re)
        return False
    return True
