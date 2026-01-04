"""Fetch ONS key economic indicator observations."""

from utils import get_dataset_editions, get_edition_versions, rate_limited_get, KEY_DATASETS
from subsets_utils import get, save_raw_file


def run():
    """Fetch key economic datasets from ONS."""
    print("  Fetching key economic datasets...")

    for dataset_id in KEY_DATASETS:
        print(f"    Processing {dataset_id}...")

        editions = get_dataset_editions(dataset_id)
        if not editions:
            print(f"      No editions found for {dataset_id}")
            continue

        edition = editions[0].get('edition', 'time-series')

        versions = get_edition_versions(dataset_id, edition)
        if not versions:
            print(f"      No versions found for {dataset_id}/{edition}")
            continue

        latest_version = versions[0]
        downloads = latest_version.get('downloads', {})
        csv_url = downloads.get('csv', {}).get('href')

        if csv_url:
            response = rate_limited_get(csv_url.replace('https://api.beta.ons.gov.uk/v1', ''))
            if response.status_code != 200:
                response = get(csv_url, timeout=120.0)

            if response.status_code == 200:
                save_raw_file(response.text, f"observations_{dataset_id}", extension="csv")
                print(f"      Fetched {len(response.text):,} bytes")
            else:
                print(f"      Failed to fetch CSV for {dataset_id}")
        else:
            print(f"      No CSV download available for {dataset_id}")

    print("  Saved observations data")
