from pathlib import Path

from report_parser import ReportParser


file = "Zoox-OL-316-112525-Redadcted.pdf"
file_path = Path(__file__).parent / "data" / "raw" / file

if __name__ == "__main__":
    rp = ReportParser(file_path)
    rp.extract()
    print(rp.result)

