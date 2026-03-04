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

    if ext in [".dwg", ".dxf"]:
        return "drawings"

    if ext in [".jpg", ".jpeg", ".png", ".webp"]:
        return "photos"

    if ext in [".xlsx", ".xls", ".csv"]:
        if _has_any(name, KEYWORDS["register"]):
            return "registers"
        if _has_any(name, KEYWORDS["boq"]):
            return "boq"
        return "spreadsheets"

    if ext in [".docx", ".doc"]:
        if _has_any(name, KEYWORDS["forms"]):
            return "forms"
        if _has_any(name, KEYWORDS["prelims"]):
            return "prelims"
        if _has_any(name, KEYWORDS["specs"]):
            return "specs"
        return "documents"

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

RISK_BUCKETS: dict[str, list[str]] = {
    "Asbestos / hazardous materials": ["asbestos", "acm", "hazardous", "lead paint", "silica"],
    "Temporary works / propping": ["temporary works", "propping", "needling", "backpropping", "sequencing"],
    "Party wall / adjacent structures": ["party wall", "adjoining", "adjacent", "third party", "neighbour"],
    "Traffic management / access": ["traffic management", "tm", "delivery", "access", "logistics", "road closure"],
    "Noise / dust / vibration": ["noise", "dust", "vibration", "monitoring", "suppression"],
    "Services / isolations": ["live", "services", "isolation", "electric", "gas", "water", "drainage"],
    "Waste / crushing / segregation": ["waste", "segregation", "recycling", "crushing", "arisings", "haulage"],
    "Permits / licences": ["permit", "licence", "license", "section 61", "consent"],
    "Working hours / constraints": ["working hours", "out of hours", "weekend", "night works"],
    "Liquidated damages / penalties": ["liquidated damages", "ld", "lad", "damages", "penalty"],
}

DATE_PATTERNS = [
    r"\b(\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{2,4})\b",
    r"\b(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\b",
]

TENDER_RETURN_PATTERNS = [
    r"(tender|return|submit|submission)\s+(date|deadline|by)\s*[:\-]?\s*([^\n]{0,140})",
    r"\b(deadline)\b\s*[:\-]?\s*([^\n]{0,140})",
]
SUBMISSION_PATTERNS = [
    r"\b(submit|submission|return)\b.{0,120}\b(email|e-mail|portal|upload|address)\b.{0,120}",
    r"\b(email)\b\s*[:\-]?\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
]
LD_PATTERNS = [
    r"(liquidated damages|LDs?|LADs?)\s*[:\-]?\s*(£\s?\d[\d,]*\.?\d*)",
    r"(liquidated damages|LDs?|LADs?).{0,80}(£\s?\d[\d,]*\.?\d*)",
]
RETENTION_PATTERNS = [
    r"(retention)\s*[:\-]?\s*(\d{1,2}(\.\d+)?\s*%)",
    r"(\d{1,2}(\.\d+)?\s*%)\s*(retention)",
]
PROGRAMME_PATTERNS = [
    r"(programme|program|duration|contract period)\s*[:\-]?\s*(\d{1,3})\s*(weeks?|months?)",
    r"(\d{1,3})\s*(weeks?|months?)\s*(programme|duration|contract period)",
]
WORKING_HOURS_PATTERNS = [
    r"(working hours|site hours|hours of work)\s*[:\-]?\s*([^\n]{0,160})",
    r"(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?).{0,50}(\d{1,2}[:.]\d{2}).{0,20}(\d{1,2}[:.]\d{2})",
]
INSURANCE_PATTERNS = [
    r"\b(public liability|employers liability|EL|PL)\b.{0,80}(£\s?\d[\d,]*\.?\d*)",
    r"\b(insurance)\b.{0,120}(£\s?\d[\d,]*\.?\d*)",
]
ACCREDITATION_PATTERNS = [
    r"\b(CHAS|SMAS|SafeContractor|Constructionline|ISO\s?9001|ISO\s?14001|ISO\s?45001)\b.{0,120}",
]

REQ_STRICT_RE = re.compile(r"\b(must|shall|required|mandatory|as a minimum|minimum of|no later than)\b", re.I)
REQ_LOOSE_RE = re.compile(r"\b(should|please|requested|provide|submit|include|confirm)\b", re.I)


def _clean_text(s: str) -> str:
    s = s.replace("\x00", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _snip_context(text: str, idx: int, window: int = 140) -> str:
    start = max(0, idx - window)
    end = min(len(text), idx + window)
    snippet = text[start:end]
    snippet = re.sub(r"\s+", " ", snippet).strip()
    return snippet[:300]


def _find_evidence(text: str, patterns: list[str], max_items: int = 6) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    if not text:
        return found
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            raw = m.group(0).strip()
            found.append({"match": raw[:220], "evidence": _snip_context(text, m.start())})
            if len(found) >= max_items:
                return found
    return found


def _extract_requirements(text: str, max_lines: int = 40) -> dict[str, list[str]]:
    strict: list[str] = []
    loose: list[str] = []

    lines = [l.strip() for l in re.split(r"[\r\n]+", text) if l and l.strip()]
    for l in lines:
        if len(l) < 6:
            continue
        ll = l[:420]
        if REQ_STRICT_RE.search(ll):
            strict.append(ll)
        elif REQ_LOOSE_RE.search(ll):
            loose.append(ll)
        if len(strict) >= max_lines and len(loose) >= max_lines:
            break

    def dedup(items: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for x in items:
            k = x.lower()
            if k not in seen:
                seen.add(k)
                out.append(x)
        return out

    return {"strict": dedup(strict)[:max_lines], "loose": dedup(loose)[:max_lines]}


# HARD caps to prevent Render/timeouts (most tender value is in early pages anyway)
PDF_MAX_PAGES = 12
DOCX_MAX_PARAS = 250
DOCX_MAX_TABLES = 30
DOCX_MAX_TABLE_ROWS = 140


def extract_pdf_info(path: Path, max_pages: int = PDF_MAX_PAGES) -> dict[str, Any]:
    info: dict[str, Any] = {
        "pages": None,
        "keyword_hits": {},
        "date_candidates": [],
        "snippet": "",
        "text_len": 0,
    }

    try:
        reader = PdfReader(str(path))
        info["pages"] = len(reader.pages)

        text_parts: list[str] = []
        for i in range(min(max_pages, len(reader.pages))):
            t = reader.pages[i].extract_text() or ""
            if t.strip():
                text_parts.append(t)

        text = _clean_text("\n".join(text_parts))
        info["text_len"] = len(text)

        text_lc = text.lower()
        hits: dict[str, int] = {}
        for kw in ESTIMATOR_KEYWORDS:
            if kw in text_lc:
                hits[kw] = text_lc.count(kw)
        info["keyword_hits"] = dict(sorted(hits.items(), key=lambda x: x[1], reverse=True)[:25])

        dates: set[str] = set()
        for pat in DATE_PATTERNS:
            for m in re.finditer(pat, text, flags=re.IGNORECASE):
                dates.add(m.group(1))
        info["date_candidates"] = sorted(dates)[:30]

        info["snippet"] = text[:1200]
        info["text"] = text[:20000]  # internal use only
        info["requirements"] = _extract_requirements(text)

    except Exception as e:
        info["error"] = f"PDF read failed: {e}"

    return info


def extract_docx_info(path: Path, max_paras: int = DOCX_MAX_PARAS) -> dict[str, Any]:
    info: dict[str, Any] = {"headings": [], "keyword_hits": {}, "snippet": "", "text_len": 0}

    try:
        doc = Document(str(path))

        paras = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]
        paras = paras[:max_paras]

        table_lines: list[str] = []
        for table in doc.tables[:DOCX_MAX_TABLES]:
            for row in table.rows[:DOCX_MAX_TABLE_ROWS]:
                cells = [c.text.strip() for c in row.cells if c.text and c.text.strip()]
                if cells:
                    table_lines.append(" | ".join(cells)[:420])

        text = _clean_text("\n".join(paras + table_lines))
        info["text_len"] = len(text)

        headings: list[str] = []
        for p in doc.paragraphs:
            if p.style and p.style.name and "Heading" in p.style.name and p.text.strip():
                headings.append(p.text.strip())
            if len(headings) >= 30:
                break
        info["headings"] = headings

        text_lc = text.lower()
        hits: dict[str, int] = {}
        for kw in ESTIMATOR_KEYWORDS:
            if kw in text_lc:
                hits[kw] = text_lc.count(kw)
        info["keyword_hits"] = dict(sorted(hits.items(), key=lambda x: x[1], reverse=True)[:25])

        info["snippet"] = text[:1200]
        info["text"] = text[:20000]  # internal use only
        info["requirements"] = _extract_requirements(text)

    except Exception as e:
        info["error"] = f"DOCX read failed: {e}"

    return info


def detect_boq_columns(header_row: list[str]) -> dict[str, int]:
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
        for name in wb.sheetnames[:20]:
            ws = wb[name]
            rows: list[list[str]] = []
            for r in ws.iter_rows(min_row=1, max_row=40, values_only=True):
                rows.append([("" if v is None else str(v)).strip() for v in r][:30])

            header = rows[0] if rows else []
            colmap = detect_boq_columns(header) if header else {}

            info["sheets"].append({
                "name": name,
                "header_guess": header[:20],
                "boq_column_map": colmap,
                "preview_rows": rows[:8],
            })

    except Exception as e:
        info["error"] = f"XLSX read failed: {e}"

    return info


def extract_csv_info(path: Path) -> dict[str, Any]:
    info: dict[str, Any] = {"header_guess": [], "boq_column_map": {}, "preview_rows": []}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            rows: list[list[str]] = []
            for i, r in enumerate(reader):
                rows.append([c.strip() for c in r][:30])
                if i >= 30:
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
    if ext == ".docx":
        return extract_docx_info(path)
    if ext in [".xlsx", ".xls"]:
        return extract_xlsx_info(path)
    if ext == ".csv":
        return extract_csv_info(path)

    if category == "drawings":
        return {
            "drawing_number_guess": guess_drawing_number(path.name),
            "revision_guess": guess_revision(path.name),
        }

    return {}


def extract_pack_briefing(sections: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    text_blobs: list[str] = []
    strict_reqs: list[str] = []
    loose_reqs: list[str] = []

    for cat in ["prelims", "specs", "forms", "addenda", "documents", "pdfs"]:
        for item in sections.get(cat, []):
            ex = item.get("extracted") or {}
            t = ex.get("text")
            if t and isinstance(t, str) and t.strip():
                text_blobs.append(t)

            req = ex.get("requirements") or {}
            for s in req.get("strict", []) or []:
                strict_reqs.append(s)
            for s in req.get("loose", []) or []:
                loose_reqs.append(s)

    merged = "\n\n".join(text_blobs)[:160000]
    merged_lc = merged.lower()

    tender_return = _find_evidence(merged, TENDER_RETURN_PATTERNS, max_items=8)
    submission = _find_evidence(merged, SUBMISSION_PATTERNS, max_items=8)
    ld = _find_evidence(merged, LD_PATTERNS, max_items=8)
    retention = _find_evidence(merged, RETENTION_PATTERNS, max_items=8)
    programme = _find_evidence(merged, PROGRAMME_PATTERNS, max_items=8)
    working_hours = _find_evidence(merged, WORKING_HOURS_PATTERNS, max_items=8)
    insurance = _find_evidence(merged, INSURANCE_PATTERNS, max_items=8)
    accreditations = _find_evidence(merged, ACCREDITATION_PATTERNS, max_items=10)

    risks: dict[str, Any] = {}
    for bucket, needles in RISK_BUCKETS.items():
        count = 0
        evidence: list[str] = []
        for needle in needles:
            if needle in merged_lc:
                count += merged_lc.count(needle)
                start = 0
                for _ in range(2):
                    idx = merged_lc.find(needle, start)
                    if idx == -1:
                        break
                    evidence.append(_snip_context(merged, idx))
                    start = idx + len(needle)
        if count > 0:
            dedup: list[str] = []
            seen: set[str] = set()
            for e in evidence:
                k = e.lower()
                if k not in seen:
                    seen.add(k)
                    dedup.append(e)
            risks[bucket] = {"mentions": count, "evidence": dedup[:6]}

    date_candidates: set[str] = set()
    for _, items in sections.items():
        for it in items:
            ex = it.get("extracted") or {}
            for d in ex.get("date_candidates", []) or []:
                date_candidates.add(str(d))

    def dedup_list(items: list[str], limit: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for x in items:
            k = x.strip().lower()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(x.strip())
            if len(out) >= limit:
                break
        return out

    return {
        "date_candidates": sorted(date_candidates)[:40],
        "tender_return_candidates": tender_return,
        "submission_candidates": submission,
        "liquidated_damages_candidates": ld,
        "retention_candidates": retention,
        "programme_candidates": programme,
        "working_hours_candidates": working_hours,
        "insurance_candidates": insurance,
        "accreditations_candidates": accreditations,
        "risk_buckets": risks,
        "requirements_strict": dedup_list(strict_reqs, 60),
        "requirements_loose": dedup_list(loose_reqs, 60),
        "sources_scanned": len(text_blobs),
    }


def _strip_internal_fields(sections: dict[str, list[dict[str, Any]]]) -> None:
    for items in sections.values():
        for it in items:
            ex = it.get("extracted")
            if isinstance(ex, dict):
                ex.pop("text", None)
                if isinstance(ex.get("sheets"), list):
                    for sh in ex["sheets"]:
                        if isinstance(sh, dict):
                            sh.pop("preview_rows", None)


# ---------------- routes ----------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/analyse")
async def analyse(zip_file: list[UploadFile] = File(...)):
    # IMPORTANT: top-level try so we never drop the connection without JSON
    try:
        if not zip_file:
            return JSONResponse({"error": "No files uploaded."}, status_code=400)

        max_bytes = 300 * 1024 * 1024

        sections: dict[str, list[dict[str, Any]]] = defaultdict(list)
        ext_counter: Counter[str] = Counter()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            extract_dir = tmp_path / "unzipped"
            extract_dir.mkdir(parents=True, exist_ok=True)

            uploaded_names: list[str] = []
            scanned_paths: list[tuple[Path, str]] = []

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

            # 2) classify + extract (guard: cap total processed files)
            total_files = 0
            by_category_count: dict[str, int] = defaultdict(int)

            MAX_FILES_TO_PROCESS = 1200
            for p, display in scanned_paths[:MAX_FILES_TO_PROCESS]:
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

            def has_cat(cat: str) -> bool:
                return len(sections.get(cat, [])) > 0

            # 3) estimator-grade briefing (uses internal 'text' blobs)
            briefing = extract_pack_briefing(sections)

            # 4) strip internal heavy fields BEFORE responding
            _strip_internal_fields(sections)

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
                "briefing": briefing,
                "sections": dict(sections),
            }

            return JSONResponse(report)

    except Exception as e:
        # If ANYTHING blows up, return JSON instead of dropping the connection
        return JSONResponse({"error": f"Analyse failed: {e}"}, status_code=500)
