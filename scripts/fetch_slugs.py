import re
import sys
from pathlib import Path
import json

# Add src to path to allow imports from datacollective
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datacollective.datasets import get_dataset_details
from datacollective.api_utils import send_api_request

SITEMAP_URL = "https://datacollective.mozillafoundation.org/sitemap.xml"
DATASET_URL_PREFIX = "https://datacollective.mozillafoundation.org/datasets/"

def _build_slugs_dataset_ids(dataset_ids: list[str]) -> dict[str, str]:
    """Build slugs and dataset IDs mapping from a list of dataset IDs.

    Args:
       dataset_ids (list[str]): List of dataset IDs.

    Returns:
       dict[str, str]: Dictionary mapping slugs to dataset IDs.
    """
    slugs = {}
    for i, dataset_id in enumerate(dataset_ids, start=1):
        try:
            details = get_dataset_details(dataset_id)
            
            dataset_slug = details.get("slug")

            if dataset_slug:
                slugs[dataset_slug] = dataset_id
        except Exception:
            continue
    return slugs

def _write_slugs_to_file(slugs: dict[str, str]) -> None:
    """Writes the given dictionary of slugs to a JSON file."""
    output_path = Path(__file__).parent.parent / "src" / "datacollective" / "data" / "slugs.json"
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump(slugs, f, indent=2, sort_keys=True)

def fetch_slugs():
    """
    Fetches dataset IDs from the sitemap, gets their details, and generates
    a JSON file mapping slugs to dataset IDs.
    """
    response = send_api_request("GET", SITEMAP_URL)

    dataset_urls = re.findall(f'<loc>{DATASET_URL_PREFIX}([^<]+)</loc>', response.text)
    dataset_ids = [id for id in dataset_urls if id]

    slugs = _build_slugs_dataset_ids(dataset_ids)
    
    if not slugs:
        return

    _write_slugs_to_file(slugs)
    

if __name__ == "__main__":
    fetch_slugs()
