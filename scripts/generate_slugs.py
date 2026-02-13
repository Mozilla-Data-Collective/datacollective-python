import sys
from pathlib import Path
import json
import requests
import xml.etree.ElementTree as ET

# Add src to path to allow imports from datacollective
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datacollective.datasets import get_dataset_details

SITEMAP_URL = "https://datacollective.mozillafoundation.org/sitemap.xml"
DATASET_URL_PREFIX = "https://datacollective.mozillafoundation.org/datasets/"

def generate_slugs():
    """
    Fetches dataset IDs from the sitemap, gets their details, and generates
    a JSON file mapping slugs to dataset IDs.
    """
    print("Generating dataset slugs from sitemap.xml (may take a while :p)...")
    slugs = {}

    try:
        response = requests.get(SITEMAP_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch sitemap: {e}")
        return

    root = ET.fromstring(response.content)
    # The sitemap has a namespace, which we need to handle
    namespace = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    dataset_ids = []
    for loc in root.findall('sitemap:url/sitemap:loc', namespace):
        url = loc.text
        if url and url.startswith(DATASET_URL_PREFIX):
            dataset_id = url.replace(DATASET_URL_PREFIX, '')
            if dataset_id:
                dataset_ids.append(dataset_id)
    total_datasets = len(dataset_ids)
    print(f"Found {total_datasets} dataset IDs in sitemap.")

    for i, dataset_id in enumerate(dataset_ids, start=1):
        try:
            print(f"({i}/{total_datasets}) Fetching details for {dataset_id}... ", end="")
            details = get_dataset_details(dataset_id)
            
            org = details.get("organization")
            if org and isinstance(org, dict):
                org_slug = org.get("slug")
            else:
                org_slug = None
            
            dataset_slug = details.get("slug")

            if org_slug and dataset_slug:
                full_slug = f"{org_slug}/{dataset_slug}"
                slugs[full_slug] = dataset_id
                print("OK")
            else:
                print(f"Skipping dataset {dataset_id} due to missing slug or organization info.")
        except Exception as e:
            print(f"Could not fetch details for dataset {dataset_id}. Error: {e}")
            # This could be a private dataset not accessible without auth, or another issue.
            continue

    if not slugs:
        print("No slugs were generated. This could be an issue with the sitemap or the API.")
        return

    output_path = Path(__file__).parent.parent / "src" / "datacollective" / "data" / "slugs.json"
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump(slugs, f, indent=2, sort_keys=True)
    
    print(f"Successfully generated {len(slugs)} slugs at {output_path}")

if __name__ == "__main__":
    generate_slugs()
