import sys
from pathlib import Path
import json
import re
import unicodedata
from bs4 import BeautifulSoup

# Add src to path to allow imports from datacollective
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def slugify(value: str) -> str:
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces and other separators to hyphens.
    """
    value = str(value)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = value.lower().strip()
    value = re.sub(r'[\s._-]+', '-', value)
    value = re.sub(r'[^\w-]+', '', value)
    return value


def generate_slugs():
    """
    Parses the datasets.html file to generate a JSON file mapping slugs to dataset IDs.
    This script requires `beautifulsoup4` and `lxml` to be installed.
    """
    print("Generating dataset slugs from HTML...")
    slugs = {}

    # Path to the source HTML file
    html_path = Path(__file__).parent.parent / "src" / "datacollective" / "data" / "datasets.html"
    if not html_path.exists():
        print(f"HTML file not found at {html_path}")
        print("Aborting slug generation.")
        return

    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, 'lxml')

    # Find all dataset cards, which are <a> tags with hrefs starting with /datasets/
    dataset_cards = soup.find_all('a', href=re.compile(r'^/datasets/'))

    for card in dataset_cards:
        href = card.get('href')
        if not href:
            continue
        dataset_id = href.split('/')[-1]

        dataset_name_tag = card.find('h3')
        if not dataset_name_tag:
            print(f"Skipping dataset with missing name for href: {href}")
            continue
        dataset_slug = slugify(dataset_name_tag.get_text(strip=True))

        card_title_div = card.find('div', attrs={'data-slot': 'card-title'})
        org_div = card_title_div.find_previous_sibling('div') if card_title_div else None
        if not org_div:
            print(f"Skipping dataset with missing organization div for href: {href}")
            continue
        org_slug = slugify(org_div.get_text(strip=True))

        if org_slug and dataset_slug and dataset_id:
            full_slug = f"{org_slug}/{dataset_slug}"
            slugs[full_slug] = dataset_id
        else:
            print(f"Skipping dataset with missing info for href: {href}")

    if not slugs:
        print("No slugs were generated. Check the HTML file or parsing logic.")
        return

    output_path = Path(__file__).parent.parent / "src" / "datacollective" / "data" / "slugs.json"
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump(slugs, f, indent=2, sort_keys=True)
    
    print(f"Successfully generated {len(slugs)} slugs at {output_path}")

if __name__ == "__main__":
    generate_slugs()
