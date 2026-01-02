"""Transform ONS key economic indicator observations."""

import csv
from io import StringIO
import pyarrow as pa
from subsets_utils import load_raw_file, upload_data

KEY_DATASETS = [
    'cpih01',
    'mm23',
    'lms',
    'gdp-low-level-aggregates',
    'retail-sales-index',
    'uk-spending-on-cards',
]


def run():
    """Transform ONS observations to PyArrow table."""
    print("  Processing economic observations...")

    all_observations = []

    for dataset_id in KEY_DATASETS:
        try:
            csv_text = load_raw_file(f"observations_{dataset_id}", extension="csv")
            observations = parse_csv_observations(dataset_id, csv_text)
            all_observations.extend(observations)
            print(f"    {dataset_id}: {len(observations)} observations")
        except FileNotFoundError:
            print(f"    {dataset_id}: No data found")

    if not all_observations:
        print("No observations parsed")
        return

    schema = pa.schema([
        ('dataset_id', pa.string()),
        ('time', pa.string()),
        ('geography', pa.string()),
        ('measure_type', pa.string()),
        ('observation', pa.float64()),
        ('unit', pa.string()),
        ('data_marking', pa.string()),
    ])

    table = pa.Table.from_pylist(all_observations, schema=schema)
    upload_data(table, "ons_economic_indicators", mode="overwrite")
    print(f"Total observations: {len(all_observations)}")


def parse_csv_observations(dataset_id, csv_text, max_rows=50000):
    """Parse CSV text into observation dicts."""
    reader = csv.DictReader(StringIO(csv_text))

    observations = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break

        obs = {
            'dataset_id': dataset_id,
            'time': row.get('Time') or row.get('time') or row.get('DATE') or row.get('Period'),
            'geography': row.get('Geography') or row.get('geography') or row.get('GEOGRAPHY'),
            'measure_type': row.get('Measure Type') or row.get('measure_type') or row.get('MEASURE'),
            'observation': None,
            'unit': row.get('Unit') or row.get('unit') or row.get('UNIT'),
            'data_marking': row.get('Data Marking') or row.get('data_marking'),
        }

        obs_val = row.get('Observation') or row.get('observation') or row.get('OBS_VALUE') or row.get('v4_0')
        if obs_val:
            try:
                obs['observation'] = float(obs_val)
            except (ValueError, TypeError):
                pass

        observations.append(obs)

    return observations
