
"""Fetch ONS dataset catalogue"""
from ons_client import get_datasets
from subsets_utils import save_raw_json


def run():
    print("Fetching ONS dataset catalogue...")

    all_datasets = []
    offset = 0
    limit = 500

    while True:
        datasets = get_datasets(limit=limit, offset=offset)
        if not datasets:
            break

        all_datasets.extend(datasets)
        print(f"  Fetched {len(all_datasets)} datasets...")

        if len(datasets) < limit:
            break
        offset += limit

    save_raw_json(all_datasets, "datasets")
    print(f"Saved {len(all_datasets)} datasets")
