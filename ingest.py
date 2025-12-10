import re
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

BASE_URL = (
    "https://www.dmv.ca.gov/portal/vehicle-industry-services/autonomous-vehicles/autonomous-vehicle-collision-reports/"
)

RAW_DIR = Path(__file__).parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_collision_reports()->list[Any]:
    page = requests.get(BASE_URL)
    soup = BeautifulSoup(page.content, "html.parser")

    accordion_blocks = soup.find_all(id=[re.compile("acc-.*")])

    reports = []
    for accordion_block in accordion_blocks:
        print(accordion_block["id"])
        for link in accordion_block.find_all("a"):
            print(link["href"])
    return reports


def download_report(url: str, base_dir: Path = Path("data/raw")) -> Path:
    page = requests.get(url)



if __name__ == "__main__":
    reports_found = fetch_collision_reports()
    print(f"Found {len(reports_found)} reports")