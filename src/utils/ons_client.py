"""UK ONS API client with rate limiting."""

from ratelimit import limits, sleep_and_retry
from subsets_utils import get

BASE_URL = "https://api.beta.ons.gov.uk/v1"

# ONS requires User-Agent header
HEADERS = {
    'User-Agent': 'DataIntegrations/1.0 (data-integrations-project)',
    'Accept': 'application/json',
}


# ONS rate limits: 120 requests per 10 seconds, 200 per minute
# We'll be conservative with 100 requests per minute
@sleep_and_retry
@limits(calls=100, period=60)
def rate_limited_get(endpoint, params=None):
    """Make a rate-limited GET request to ONS API."""
    url = f"{BASE_URL}{endpoint}"
    response = get(url, params=params, headers=HEADERS, timeout=60.0)
    return response


def get_datasets(limit=500, offset=0):
    """
    Get list of available datasets.

    Returns:
        List of dataset metadata dicts
    """
    params = {'limit': limit, 'offset': offset}
    response = rate_limited_get('/datasets', params=params)
    response.raise_for_status()
    data = response.json()
    return data.get('items', [])


def get_dataset_editions(dataset_id):
    """
    Get available editions for a dataset.

    Returns:
        List of edition metadata
    """
    response = rate_limited_get(f'/datasets/{dataset_id}/editions')
    if response.status_code == 404:
        return []
    response.raise_for_status()
    data = response.json()
    return data.get('items', [])


def get_edition_versions(dataset_id, edition):
    """
    Get available versions for a dataset edition.

    Returns:
        List of version metadata
    """
    response = rate_limited_get(f'/datasets/{dataset_id}/editions/{edition}/versions')
    if response.status_code == 404:
        return []
    response.raise_for_status()
    data = response.json()
    return data.get('items', [])


def get_observations(dataset_id, edition, version, dimensions=None):
    """
    Get observations from a dataset.

    Args:
        dataset_id: Dataset identifier
        edition: Edition name (e.g., 'time-series')
        version: Version number
        dimensions: Dict of dimension filters

    Returns:
        List of observation dicts
    """
    endpoint = f'/datasets/{dataset_id}/editions/{edition}/versions/{version}/observations'

    params = {}
    if dimensions:
        for dim, value in dimensions.items():
            params[dim] = value

    response = rate_limited_get(endpoint, params=params)
    if response.status_code == 404:
        return []
    response.raise_for_status()
    data = response.json()
    return data.get('observations', [])
