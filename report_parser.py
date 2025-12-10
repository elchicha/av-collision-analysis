import pathlib
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple, Optional

import pdfplumber
from pypdf import PdfReader


class ReportParser:

    def __init__(self, pdf_path: Path) -> None:
        self.pdf_path = pathlib.Path(pdf_path)
        if not self.pdf_path.is_file():
            raise FileNotFoundError(f"File not found: {self.pdf_path}")
        self.result: Dict[str, str] = {}


    # PRIVATE: load the PDF text (once per instance)
    def _load_text(self):
        """Read the PDF with pdfplumber and concatenate page text."""
        with pdfplumber.open(self.pdf_path) as pdf:
            self._full_text = "\n".join(
                page.extract_text() or "" for page in pdf.pages
            )

        return self._full_text

    def _extract_form_values(self) -> Dict[str, Any]:
        """
        Extract form fields using pypdf (best for AcroForms)
        Returns: Dictionary of field names and values for those fields with a corresponding value.
        """
        reader = PdfReader(self.pdf_path)

        if reader.is_encrypted:
            reader.decrypt('')

        fields = {}

        # Get form fields from the document
        if "/AcroForm" in reader.trailer["/Root"]:
            form = reader.trailer["/Root"]["/AcroForm"]
            if "/Fields" in form:
                for field in form["/Fields"]:
                    field_obj = field.get_object()
                    field_name = field_obj.get("/T", "").lower()
                    field_value = field_obj.get("/V", "")

                    # Handle different value types
                    if hasattr(field_value, 'get_object'):
                        field_value = field_value.get_object()

                    if field_value:
                        fields[field_name] = field_value

        return fields

    # PUBLIC: perform the extraction
    def extract(self) -> None:
        self.result = self._extract_form_values()

    # PUBLIC: export the current result into CSV-ready structures
    def to_csv_ready(
        self,
        field_order: Optional[Iterable[str]] = None,
    ) -> Tuple[List[str], List[str]]:
        """Return (headers, row_values) for the current parsed result.
        Use after calling extract() or by setting self.result.
        """
        from csv_formatter import to_csv_ready_row

        return to_csv_ready_row(self.result, field_order)

    def to_csv_line(
        self,
        field_order: Optional[Iterable[str]] = None,
        dialect: str = "excel",
    ) -> Tuple[List[str], List[str], str]:
        """Return (headers, row_values, csv_line_string) for the current result."""
        from csv_formatter import to_csv_line

        return to_csv_line(self.result, field_order, dialect)