from __future__ import annotations

import io
import re
import csv
import shutil
import tempfile
import zipfile
from pathlib import Path
from collections import Counter, defaultdict
from typing import Any

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from pypdf import PdfReader
from openpyxl import load_workbook
from docx import Document


app = FastAPI(title="Tender Pack Summary")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="templates")


# ---------------- ZIP safety ----------------
def safe_extract_zip(zip_path: Path, extract_to: Path, max_files: int = 5000) -> list[Path]:
    extracted: list[Path] = []
    base = extract_to.resolve()

    with zipfile.ZipFile(zip_path, "r") as z:
        members = z.infolist()
        if len(members) > max_files:
            raise ValueError(f"Too many files in ZIP ({len(members)}). Limit is {max_files}.")

        for m in members:
            if m.is_dir():
                continue

            out_path = (extract_to / m.filename).resolve()
            if not str(out_path).startswith(str(base)):
                raise ValueError("Unsafe ZIP: path traversal detected.")

            out_path.parent.mkdir(parents=True, exist_ok=True)
            with z.open(m, "r") as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted.append(out_path)

    return extracted


# ---------------- classification ----------------
KEYWORDS = {
    "boq": ["boq", "bill", "bq", "quantities", "pricing schedule", "schedule of rates", "sor", "price"],
    "register": ["register", "drawing register", "document register", "issue register", "transmittal"],
    "addenda": ["addendum", "addenda", "clarification", "rfi response", "tender query", "tq"],
    "prelims": ["prelim", "prelims"],
    "specs": ["spec", "specification", "employer", "requirements", "works information"],
    "forms": ["form", "tender form", "declaration", "questionnaire", "pqq", "sq", "itt", "appendix"],
    "programme": ["programme", "program", "schedule", "gantt"],
    "h&s": ["rams", "h&s", "health and safety", "cdm", "cpp", "construction phase plan"],
}

DRAWING_HINTS = ["drg", "dwg", "drawing", "ga", "plan", "elev", "section", "demo", "demolition", "sketch", "sk"]


def _has_any(name: str, words: list[str]) -> bool:
    n = name.lower()
    return any(w in n for w in words)


def classify_file(p: Path) -> str:
    name = p.name.lower()
    ext = p.suffix.lower()

    # CAD drawings
    if ext in [".dwg", ".dxf"]:
        return "drawings"

    # Images
    if ext in [".jpg", ".jpeg", ".png", ".webp"]:
        return "photos"

    # Spreadsheets/CSVs
    if ext in [".xlsx", ".xls", ".csv"]:
        if _has_any(name, KEYWORDS["register"]):
            return "registers"
        if _has_any(name, KEYWORDS["boq"]):
            return "boq"
        return "spreadsheets"

    # Word docs
    if ext in [".docx", ".doc"]:
        if _has_any(name, KEYWORDS["forms"]):
            return "forms"
        if _has_any(name, KEYWORDS["prelims"]):
            return "prelims"
        if _has_any(name, KEYWORDS["specs"]):
            return "specs"
        return "documents"

    # PDFs
    if ext == ".pdf":
        if _has_any(name, KEYWORDS["addenda"]):
            return "addenda"
        if _has_any(name, KEYWORDS["boq"]):
            return "boq"
        if _has_any(name, KEYWORDS["register"]):
            return "registers"
        if _has_any(name, KEYWORDS["prelims"]):
            return "prelims"
        if _has_any(name, KEYWORDS["specs"]):
            return "specs"
        if any(h in name for h in DRAWING_HINTS):
            return "drawings"
        return "pdfs"

    return "other"


def guess_revision(filename: str) -> str | None:
    s = filename.upper()
    m = re.search(r"(?:REV[\s_\-]*)?([PC]\d{2,3})\b", s)
    return m.group(1) if m else None


def guess_drawing_number(filename: str) -> str | None:
    # common: A101, D-102, SK-03, DR-A-100, etc (best-effort)
    s = Path(filename).stem.upper()
    m = re.search(r"\b([A-Z]{1,4}[-_ ]?\d{2,5})\b", s)
    if m:
        return m.group(1).replace(" ", "-").replace("_", "-")
    return None


# ---------------- extraction helpers ----------------
ESTIMATOR_KEYWORDS = [
    "asbestos", "soft strip", "strip out", "demolition", "temporary works", "propping",
    "party wall", "working hours", "out of hours", "noise", "dust", "vibration",
    "traffic management", "tm", "permits", "waste", "recycling", "segregation",
    "water", "electric", "gas", "services", "live", "isolation",
    "liquidated damages", "ld", "lad", "damages", "penalty",
    "site access", "hoarding", "scaffold", "crushing", "arising", "arisings",
]

DATE_PATTERNS = [
    r"\b(\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{2,4})\b",
    r"\b(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\b",
]


def extract_pdf_info(path: Path, max_pages: int = 8) -> dict[str, Any]:
    info: dict[str, Any] = {"pages": None, "keyword_hits": {}, "date_candidates": [], "snippet": ""}

    try:
        reader = PdfReader(str(path))
        info["pages"] = len(reader.pages)

        text_parts: list[str] = []
        for i in range(min(max_pages, len(reader.pages))):
            t = reader.pages[i].extract_text() or ""
            if t.strip():
                text_parts.append(t)

        text = "\n".join(text_parts)
        text_lc = text.lower()

        hits = {}
        for kw in ESTIMATOR_KEYWORDS:
            if kw in text_lc:
                hits[kw] = text_lc.count(kw)
        info["keyword_hits"] = dict(sorted(hits.items(), key=lambda x: x[1], reverse=True)[:25])

        dates: set[str] = set()
        for pat in DATE_PATTERNS:
            for m in re.finditer(pat, text, flags=re.IGNORECASE):
                dates.add(m.group(1))
        info["date_candidates"] = sorted(dates)[:25]

        # small snippet for quick skim (first ~1200 chars of cleaned text)
        cleaned = re.sub(r"\s+", " ", text).strip()
        info["snippet"] = cleaned[:1200]

    except Exception as e:
        info["error"] = f"PDF read failed: {e}"

    return info


def extract_docx_info(path: Path, max_paras: int = 200) -> dict[str, Any]:
    info: dict[str, Any] = {"headings": [], "keyword_hits": {}, "snippet": ""}

    try:
        doc = Document(str(path))
        paras = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]
        paras = paras[:max_paras]
        text = "\n".join(paras)
        text_lc = text.lower()

        # headings: best-effort (Word styles often "Heading 1/2/3")
        headings = []
        for p in doc.paragraphs:
            if p.style and p.style.name and "Heading" in p.style.name and p.text.strip():
                headings.append(p.text.strip())
            if len(headings) >= 30:
                break
        info["headings"] = headings

        hits = {}
        for kw in ESTIMATOR_KEYWORDS:
            if kw in text_lc:
                hits[kw] = text_lc.count(kw)
        info["keyword_hits"] = dict(sorted(hits.items(), key=lambda x: x[1], reverse=True)[:25])

        cleaned = re.sub(r"\s+", " ", text).strip()
        info["snippet"] = cleaned[:1200]

    except Exception as e:
        info["error"] = f"DOCX read failed: {e}"

    return info


def detect_boq_columns(header_row: list[str]) -> dict[str, int]:
    """
    Map common BOQ columns to indices using fuzzy-ish rules.
    """
    cols = [c.lower().strip() for c in header_row]
    def find_any(keys: list[str]) -> int:
        for i, c in enumerate(cols):
            if any(k in c for k in keys):
                return i
        return -1

    mapping = {
        "item": find_any(["item", "no", "line", "ref"]),
        "description": find_any(["description", "desc", "details", "work", "item description"]),
        "qty": find_any(["qty", "quantity", "quant"]),
        "unit": find_any(["unit", "uom"]),
        "rate": find_any(["rate", "price"]),
        "amount": find_any(["amount", "total", "value"]),
    }
    return {k: v for k, v in mapping.items() if v != -1}


def extract_xlsx_info(path: Path) -> dict[str, Any]:
    info: dict[str, Any] = {"sheets": []}

    try:
        wb = load_workbook(filename=str(path), data_only=True, read_only=True)
        for name in wb.sheetnames[:30]:
            ws = wb[name]
            # Get a small window of cells to detect structure
            rows = []
            for r in ws.iter_rows(min_row=1, max_row=25, values_only=True):
                rows.append([("" if v is None else str(v)).strip() for v in r][:30])

            # find first non-empty row as header candidate
            header = None
            header_idx = None
            for i, r in enumerate(rows):
                non_empty = sum(1 for x in r if x)
                if non_empty >= 3:
                    header = r
                    header_idx = i
                    break

            colmap = detect_boq_columns(header) if header else {}

            # count “lines” roughly by scanning first column for non-empty after header
            approx_lines = 0
            if header_idx is not None:
                for r in rows[header_idx + 1:]:
                    if any(x for x in r):
                        approx_lines += 1

            info["sheets"].append({
                "name": name,
                "header_guess": header[:12] if header else [],
                "boq_column_map": colmap,   # if it finds desc/qty/unit etc this becomes useful
                "approx_lines_in_preview": approx_lines,
                "preview_rows": rows[header_idx:header_idx+6] if header_idx is not None else rows[:6],
            })

    except Exception as e:
        info["error"] = f"XLSX read failed: {e}"

    return info


def extract_csv_info(path: Path) -> dict[str, Any]:
    info: dict[str, Any] = {"header_guess": [], "boq_column_map": {}, "preview_rows": []}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            rows = []
            for i, r in enumerate(reader):
                rows.append([c.strip() for c in r][:40])
                if i >= 20:
                    break

        header = rows[0] if rows else []
        info["header_guess"] = header[:20]
        info["boq_column_map"] = detect_boq_columns(header) if header else {}
        info["preview_rows"] = rows[:8]

    except Exception as e:
        info["error"] = f"CSV read failed: {e}"
    return info


def extract_by_type(path: Path, category: str) -> dict[str, Any]:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return extract_pdf_info(path)
    if ext in [".docx"]:
        return extract_docx_info(path)
    if ext in [".xlsx", ".xls"]:
        return extract_xlsx_info(path)
    if ext == ".csv":
        return extract_csv_info(path)

    # drawings/photos/other: keep it simple in v1
    if category == "drawings":
        return {
            "drawing_number_guess": guess_drawing_number(path.name),
            "revision_guess": guess_revision(path.name),
        }

    return {}


# ---------------- routes ----------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/analyse")
async def analyse(zip_file: list[UploadFile] = File(...)):
    if not zip_file:
        return JSONResponse({"error": "No files uploaded."}, status_code=400)

    max_bytes = 300 * 1024 * 1024

    # We build a structured “sections” report
    sections: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ext_counter: Counter[str] = Counter()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        extract_dir = tmp_path / "unzipped"
        extract_dir.mkdir(parents=True, exist_ok=True)

        uploaded_names: list[str] = []
        scanned_paths: list[tuple[Path, str]] = []  # (path, display_name)

        # 1) ingest: save uploads, unzip zips
        for uf in zip_file:
            if not uf.filename:
                continue
            uploaded_names.append(uf.filename)

            content = await uf.read()
            if len(content) > max_bytes:
                return JSONResponse({"error": f"File too large (limit 300MB): {uf.filename}"}, status_code=400)

            if uf.filename.lower().endswith(".zip"):
                zp = tmp_path / f"upload_{len(uploaded_names)}.zip"
                zp.write_bytes(content)
                try:
                    extracted = safe_extract_zip(zp, extract_dir, max_files=5000)
                except Exception as e:
                    return JSONResponse({"error": f"Could not extract ZIP {uf.filename}: {e}"}, status_code=400)

                for p in extracted:
                    scanned_paths.append((p, str(p.relative_to(extract_dir))))
            else:
                safe_name = Path(uf.filename).name
                op = tmp_path / safe_name
                op.write_bytes(content)
                scanned_paths.append((op, safe_name))

        # 2) classify + extract
        total_files = 0
        by_category_count: dict[str, int] = defaultdict(int)

        for p, display in scanned_paths:
            total_files += 1
            ext = p.suffix.lower() if p.suffix else "(no_ext)"
            ext_counter[ext] += 1

            category = classify_file(p)
            by_category_count[category] += 1

            extracted = extract_by_type(p, category)
            sections[category].append({
                "file": display,
                "ext": ext,
                "category": category,
                "extracted": extracted,
            })

        # 3) high-level flags estimators care about
        def has_cat(cat: str) -> bool:
            return len(sections.get(cat, [])) > 0

        report = {
            "summary": {
                "uploaded_items": len(uploaded_names),
                "uploaded_names": uploaded_names[:200],
                "total_files_scanned": total_files,
                "by_extension": dict(ext_counter.most_common()),
                "by_category": dict(sorted(by_category_count.items(), key=lambda x: x[1], reverse=True)),
                "boq_found": has_cat("boq"),
                "register_found": has_cat("registers"),
                "drawings_found": has_cat("drawings"),
                "forms_found": has_cat("forms"),
                "prelims_found": has_cat("prelims"),
                "specs_found": has_cat("specs"),
                "addenda_found": has_cat("addenda"),
            },
            "sections": sections,  # this is the big win: separated + extracted
        }

        return JSONResponse(report)
