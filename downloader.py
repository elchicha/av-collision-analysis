import logging
import re
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

import sys
from pathlib import Path

# Setup logging to write to log directory
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "downloader.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)
class CollisionReportDownloader:
    REPORTS_URL = (
        "https://www.dmv.ca.gov/portal/vehicle-industry-services/autonomous-vehicles/autonomous-vehicle-collision-reports/"
    )
    SITE_URL = (
        "https://www.dmv.ca.gov"
    )

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch_report_urls(self) -> list[str]:
        """Fetches a list of PDF URLs from the main page."""
        try:
            page = requests.get(self.REPORTS_URL)
            page.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to load page: {e}")
            return []

        soup = BeautifulSoup(page.content, "html.parser")
        
        # Find all accordion blocks
        accordion_blocks = soup.find_all(id=re.compile("acc-.*"))

        reports = []
        for accordion_block in accordion_blocks:
            # Find all links within the block
            for link in accordion_block.find_all("a", href=True):
                href = link["href"]
                if not href.startswith(("https://",)):
                    href = self.SITE_URL + href
                # Basic validation to ensure we are grabbing PDFs
                if "pdf" in href.strip().lower():
                    reports.append(href)

        return reports

    def download_report(self, url: str) -> Path | None:
        """Downloads a single PDF report."""
        try:
            # Extract filename from URL
            parsed_url = urlparse(url)
            filename_url = Path(parsed_url.path).name

            response = requests.get(url, stream=True)
            response.raise_for_status()
            d = response.headers['content-disposition']
            filename: str = re.findall("filename=(.+)", d)[0]

            destination = self.output_dir / filename_url / filename

            if destination.exists():
                logger.info(f"Skipping {filename}, already exists.")
                return destination

            logger.info(f"Downloading: {filename}")

            destination.parent.mkdir(parents=True, exist_ok=True)
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=32764):
                    f.write(chunk)

            logger.info(f"Successfully downloaded: {filename}")
            return destination
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

    def run(self):
        """Orchestrates the finding and downloading of reports."""
        logger.info(f"Fetching reports from {self.REPORTS_URL}...")
        report_urls = self.fetch_report_urls()
        logger.info(f"Found {len(report_urls)} reports.")

        for url in report_urls:
            self.download_report(url)