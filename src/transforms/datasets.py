
"""Transform ONS dataset catalogue"""
import pyarrow as pa
from subsets_utils import load_raw_json, upload_data


def run():
    print("Processing dataset catalogue...")

    all_datasets = load_raw_json("datasets")

    if not all_datasets:
        print("No datasets found")
        return

    flattened = []
    for ds in all_datasets:
        links = ds.get('links', {})
        flattened.append({
            'id': ds.get('id'),
            'title': ds.get('title'),
            'description': ds.get('description', '')[:2000] if ds.get('description') else None,
            'state': ds.get('state'),
            'type': ds.get('type'),
            'release_frequency': ds.get('release_frequency'),
            'next_release': ds.get('next_release'),
            'contacts_name': ds.get('contacts', [{}])[0].get('name') if ds.get('contacts') else None,
            'contacts_email': ds.get('contacts', [{}])[0].get('email') if ds.get('contacts') else None,
            'latest_version_url': links.get('latest_version', {}).get('href'),
            'editions_url': links.get('editions', {}).get('href'),
            'self_url': links.get('self', {}).get('href'),
            'keywords': '|'.join(ds.get('keywords', [])) if ds.get('keywords') else None,
            'national_statistic': ds.get('national_statistic'),
            'license': ds.get('license'),
            'uri': ds.get('uri'),
        })

    schema = pa.schema([
        ('id', pa.string()),
        ('title', pa.string()),
        ('description', pa.string()),
        ('state', pa.string()),
        ('type', pa.string()),
        ('release_frequency', pa.string()),
        ('next_release', pa.string()),
        ('contacts_name', pa.string()),
        ('contacts_email', pa.string()),
        ('latest_version_url', pa.string()),
        ('editions_url', pa.string()),
        ('self_url', pa.string()),
        ('keywords', pa.string()),
        ('national_statistic', pa.bool_()),
        ('license', pa.string()),
        ('uri', pa.string()),
    ])

    table = pa.Table.from_pylist(flattened, schema=schema)
    upload_data(table, "ons_datasets", mode="overwrite")
    print(f"Processed {len(flattened)} datasets")
