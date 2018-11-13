import logging
import os

import requests
from requests import RequestException
from requests.auth import HTTPBasicAuth

TIX_API_SSL = os.environ.get('TIX_API_SSL') is not None
TIX_API_HOST = os.environ.get('TIX_API_HOST', 'localhost')
TIX_API_PORT = os.environ.get('TIX_API_PORT')
TIX_API_URL_TEMPLATE = '{proto}://{api_host}/api/user/{user_id}/installation/{installation_id}/reports'

logger = logging.getLogger(__name__)


def prepare_results_for_api(results, ip):
    return {
        'timestamp': results['timestamp'],
        'version': '1.0.0',
        'upUsage':  results['upstream']['usage'],
        'upQuality': results['upstream']['quality'],
        'downUsage': results['downstream']['usage'],
        'downQuality': results['downstream']['quality'],
        'hurstUpRs':  results['upstream']['hurst']['rs'],
        'hurstUpWavelet': results['upstream']['hurst']['wavelet'],
        'hurstDownRs': results['downstream']['hurst']['rs'],
        'hurstDownWavelet': results['downstream']['hurst']['wavelet'],
        'ip': ip
    }


def prepare_url(user_id, installation_id, tix_api_ssl=TIX_API_SSL, tix_api_host=TIX_API_HOST, tix_api_port=TIX_API_PORT):
    if tix_api_ssl:
        proto = 'https'
        default_port = 443
    else:
        proto = 'http'
        default_port = 80
    if tix_api_port and int(tix_api_port) != default_port:
        api_port = ':' + tix_api_port
    else:
        api_port = ''
    url = TIX_API_URL_TEMPLATE.format(
        proto=proto,
        api_host=tix_api_host + api_port,
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
    try:
        response = requests.post(url=url,
                                 json=json_data)
        if response.status_code not in (200, 204):
            log.error('Error while trying to post to API, got status code {status_code} for url {url}'
                      .format(status_code=response.status_code,
                              url=url))
            return False
    except RequestException as re:
        log.error('Error while trying to post to API for url {url}'.format(url=url))
        log.error(re)
        return False
    return True
