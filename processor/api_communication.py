import logging
import os

import requests

TIX_API_SSL = os.environ.get('TIX_API_SSL', None) is not None
TIX_API_HOST = os.environ.get('TIX_API_HOST', 'localhost')
TIX_API_PORT = os.environ.get('TIX_API_PORT', '80')

logger = logging.getLogger(__name__)


def prepare_results_for_api(results, as_info):
    return {
        'timestamp': results['timestamp'],
        'upstream': {
            'usage': results['upstream_usage'],
            'quality': results['upstream_quality']
        },
        'downstream': {
            'usage': results['downstream_usage'],
            'quality': results['downstream_quality']
        },
        'hurst': {
            'rs': results['rs'],
            'wavelet': results['wavelet']
        },
        'as': {
            'as_id': as_info['id'],
            'as_owner': as_info['owner']
        }
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


def post_results(results, as_info, user_id, installation_id):
    log = logger.getChild('post_results')
    log.info('posting results')
    json_data = prepare_results_for_api(results, as_info)
    url = prepare_url(user_id, installation_id)
    response = requests.post(url=url, json=json_data)
    if response.status_code not in (200, 204):
        log.error('Error while trying to post to API, got status code {status_code} for url {url}'
                  .format(status_code=response.status_code,
                          url=url))
        return False
    return True
