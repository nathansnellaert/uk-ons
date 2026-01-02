#!/usr/bin/env python3
import os

"""UK ONS Statistics Connector"""
import os
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

import argparse
from subsets_utils import validate_environment
from ingest import datasets as ingest_datasets
from ingest import observations as ingest_observations
from transforms import datasets as transform_datasets
from transforms import observations as transform_observations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_datasets.run()
        ingest_observations.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        transform_datasets.run()
        transform_observations.run()


if __name__ == "__main__":
    main()
