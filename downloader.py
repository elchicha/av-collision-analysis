import logging
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "av-collision-analysis/1.0"
        })
        retries = Retry(
            total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD")
        )
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.timeout = (5, 30)

    def _dest_path_for(self, url:str) -> bool:
        """" Check if filename has already been downloaded. """
        filename_url = Path(urlparse(url).path).name
        dest_path = self.output_dir / filename_url
        if dest_path.exists() and dest_path.is_dir():
            pdf_files = list(dest_path.glob("*.pdf"))
            if len(pdf_files) == 1:
                return True
        return False

    def fetch_report_urls(self) -> list[str]:
        """Fetches a list of PDF URLs from the main page."""
        try:
            page = self.session.get(self.REPORTS_URL, timeout=self.timeout)
            page.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to load page: {e}")
            return []

        soup = BeautifulSoup(page.content, "lxml")
        
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
                    if not self._dest_path_for(href):
                        reports.append(href)

        return reports

    def download_report(self, url: str) -> Path | None:
        """Downloads a single PDF report."""
        try:
            # Extract filename from URL
            parsed_url = urlparse(url)
            filename_url = Path(parsed_url.path).name

            response = self.session.get(url, stream=True, timeout=self.timeout)
            response.raise_for_status()

            cont_disp = response.headers['content-disposition']
            filename: str = re.findall("filename=(.+)", cont_disp)[0]
            destination = self.output_dir / filename_url / filename

            if destination.exists():
                logger.warning(f"Skipping {filename}, already exists.")
                return destination

            logger.info(f"Downloading: {filename}")
            destination.parent.mkdir(parents=True, exist_ok=True)

            response.raw.decode_content = True
            with open(destination, "wb") as f:
                shutil.copyfileobj(response.raw, f, length=1024 * 1024)

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

        max_workers = min(16, max(4, (len(report_urls) // 10) or 4))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(self.download_report, url): url for url in report_urls}
            for fut in as_completed(futures):
                url = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    logger.error(f"Failed {url}: {e}")