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

# CHANGED (minimises false positives vs generic specs/BREEAM guidance)
RISK_BUCKETS: dict[str, list[str]] = {
    "Asbestos / hazardous materials": ["asbestos", "acm", "hazardous", "lead paint", "silica"],
    "Temporary works / propping": ["temporary works", "propping", "needling", "backpropping", "sequencing"],
    "Party wall / adjacent structures": ["party wall", "adjoining", "adjacent", "third party", "neighbour", "neighbor"],
    "Traffic management / access": ["traffic management", "road closure", "lane closure", "delivery", "access", "logistics", "banksman"],
    "Noise / dust / vibration": ["noise", "dust", "vibration", "monitoring", "suppression", "section 61"],
    "Services / isolations": ["isolation", "isolations", "live", "disconnect", "disconnection", "divert", "diversion", "electric", "gas", "water", "drainage"],
    "Waste / crushing / segregation": ["waste", "segregation", "recycling", "crushing", "arisings", "haulage", "skip", "muck away"],
    "Permits / licences": ["permit", "licence", "license", "consent", "section 61"],
    "Working hours / constraints": ["working hours", "site hours", "out of hours", "weekend", "night works"],
    "Liquidated damages / penalties": ["liquidated damages", "lds", "lads", "lad", "penalty"],
}

DATE_PATTERNS = [
    r"\b(\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{2,4})\b",
    r"\b(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\b",
]

TENDER_RETURN_PATTERNS = [
    r"(tender|return|submit|submission)\s+(date|deadline|by)\s*[:\-]?\s*([^\n]{0,160})",
    r"\b(deadline)\b\s*[:\-]?\s*([^\n]{0,160})",
]
SUBMISSION_PATTERNS = [
    r"\b(submit|submission|return)\b.{0,140}\b(email|e-mail|portal|upload|address)\b.{0,140}",
    r"\b(email)\b\s*[:\-]?\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
]
LD_PATTERNS = [
    r"(liquidated damages|LDs?|LADs?)\s*[:\-]?\s*(£\s?\d[\d,]*\.?\d*)",
    r"(liquidated damages|LDs?|LADs?).{0,100}(£\s?\d[\d,]*\.?\d*)",
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
    r"(working hours|site hours|hours of work)\s*[:\-]?\s*([^\n]{0,200})",
    r"(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?).{0,60}(\d{1,2}[:.]\d{2}).{0,30}(\d{1,2}[:.]\d{2})",
]
INSURANCE_PATTERNS = [
    r"\b(public liability|employers liability|EL|PL)\b.{0,120}(£\s?\d[\d,]*\.?\d*)",
    r"\b(insurance)\b.{0,160}(£\s?\d[\d,]*\.?\d*)",
]
ACCREDITATION_PATTERNS = [
    r"\b(CHAS|SMAS|SafeContractor|Constructionline|ISO\s?9001|ISO\s?14001|ISO\s?45001)\b.{0,180}",
]

REQ_STRICT_RE = re.compile(r"\b(must|shall|required|mandatory|as a minimum|minimum of|no later than)\b", re.I)
REQ_LOOSE_RE = re.compile(r"\b(should|please|requested|provide|submit|include|confirm)\b", re.I)

SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?])\s+|\n+")

# NEW: filters to stop “random spec guidance” polluting the briefing
TENDER_CONTEXT_WORDS = [
    "tender", "return", "submit", "submission", "deadline", "closing date",
    "contract", "conditions", "jct", "nec", "scope", "works", "employer",
    "pricing", "boq", "rates", "programme", "duration", "working hours",
    "insurance", "retention", "liquidated damages", "lad", "ld",
    "site", "access", "logistics", "permit", "licence", "hoarding",
    "demolition", "strip", "soft strip", "asbestos", "temporary works",
]
COMMERCIAL_SIGNAL_RE = re.compile(r"(£\s?\d|%\b|\bweeks?\b|\bmonths?\b|\b\d{1,2}[:.]\d{2}\b)", re.I)
IRRELEVANT_DOC_HINTS = [
    "breeam", "credit", "wat 01", "assessor", "calculator", "guidance",
    "performance levels", "this document represents guidance",
]


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
            found.append({"match": raw[:240], "evidence": _snip_context(text, m.start())})
            if len(found) >= max_items:
                return found
    return found


def _normalize_line(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _looks_irrelevant(text: str) -> bool:
    tl = (text or "").lower()
    return any(h in tl for h in IRRELEVANT_DOC_HINTS)


def _is_tabley(s: str) -> bool:
    return (s or "").count("|") >= 3


def _is_gibberish_line(s: str) -> bool:
    s2 = _normalize_line(s)
    if len(s2) < 10:
        return True
    if _is_tabley(s2):
        return True
    code_tokens = re.findall(r"\b[A-Z]{1,4}[-_ ]?\d{1,4}[A-Z]?\b", s2)
    if len(code_tokens) >= 6:
        return True
    letters = [c for c in s2 if c.isalpha()]
    if letters:
        upper_ratio = sum(1 for c in letters if c.isupper()) / max(1, len(letters))
        if upper_ratio > 0.75 and len(s2) > 40:
            return True
    return False


def _is_tender_relevant_sentence(s: str) -> bool:
    s2 = _normalize_line(s)
    if not s2 or _is_gibberish_line(s2):
        return False
    if _looks_irrelevant(s2):
        return False
    tl = s2.lower()
    if COMMERCIAL_SIGNAL_RE.search(s2):
        return True
    if any(w in tl for w in TENDER_CONTEXT_WORDS):
        return True
    return False


def _split_sentences(text: str) -> list[str]:
    if not text:
        return []
    parts = [p.strip() for p in SENT_SPLIT_RE.split(text) if p and p.strip()]
    return parts


# CHANGED: only keep requirements that are actually tender/submission relevant
def _extract_requirements(text: str, max_lines: int = 40) -> dict[str, list[str]]:
    strict: list[str] = []
    loose: list[str] = []

    raw_lines = [l.strip() for l in re.split(r"[\r\n]+", text or "") if l and l.strip()]

    def accept_line(l: str) -> bool:
        l2 = _normalize_line(l)[:320]
        if not l2:
            return False
        if _looks_irrelevant(l2):
            return False
        if _is_gibberish_line(l2):
            return False
        return _is_tender_relevant_sentence(l2)

    for l in raw_lines:
        ll = l[:420]
        if not accept_line(ll):
            continue
        if REQ_STRICT_RE.search(ll):
            strict.append(_normalize_line(ll)[:260])
        elif REQ_LOOSE_RE.search(ll):
            loose.append(_normalize_line(ll)[:260])
        if len(strict) >= max_lines and len(loose) >= max_lines:
            break

    # sentence fallback if PDFs don’t preserve line breaks well
    if len(strict) < 6:
        for s in _split_sentences(text):
            ss = s[:420]
            if not accept_line(ss):
                continue
            if REQ_STRICT_RE.search(ss):
                strict.append(_normalize_line(ss)[:260])
            if len(strict) >= max_lines:
                break

    def dedup(items: list[str], limit: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for x in items:
            k = x.lower().strip()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(x.strip())
            if len(out) >= limit:
                break
        return out

    return {"strict": dedup(strict, max_lines), "loose": dedup(loose, max_lines)}


def _best_line_from_evidence(items: list[dict[str, str]] | None) -> str | None:
    if not items:
        return None
    # prefer tender-relevant matches
    for it in items:
        m = _normalize_line(it.get("match") or "")
        if m and _is_tender_relevant_sentence(m):
            return m[:240]
    # fallback to non-gibberish
    for it in items:
        m = _normalize_line(it.get("match") or "")
        if m and not _is_gibberish_line(m):
            return m[:240]
    return None


def _sentences_around(text: str, idx: int, max_sentences: int = 2) -> str:
    if not text:
        return ""
    parts = SENT_SPLIT_RE.split(text)
    offsets: list[tuple[int, int, str]] = []
    pos = 0
    for p in parts:
        p2 = p.strip()
        if not p2:
            pos += len(p) + 1
            continue
        start = text.find(p2, pos)
        if start == -1:
            start = pos
        end = start + len(p2)
        offsets.append((start, end, p2))
        pos = end

    s_i = 0
    for i, (a, b, _) in enumerate(offsets):
        if a <= idx <= b:
            s_i = i
            break

    chosen: list[str] = []
    for j in range(max(0, s_i), min(len(offsets), s_i + max_sentences)):
        sent = offsets[j][2]
        if sent and not _is_gibberish_line(sent):
            chosen.append(sent[:240])

    return " ".join(chosen).strip()


def _find_bucket_evidence(merged: str, needle: str, bucket: str, max_items: int = 1) -> list[str]:
    """
    Finds a short, readable tender-relevant sentence for a bucket.
    Extra guard: Services bucket must include isolation/live/disconnect-ish words.
    """
    merged_lc = merged.lower()
    out: list[str] = []
    start = 0
    while len(out) < max_items:
        idx = merged_lc.find(needle, start)
        if idx == -1:
            break
        s = _sentences_around(merged, idx, max_sentences=2)
        if s and _is_tender_relevant_sentence(s):
            sl = s.lower()
            if bucket == "Services / isolations":
                if not any(x in sl for x in ["isolation", "isolations", "live", "disconnect", "disconnection", "divert", "diversion"]):
                    start = idx + len(needle)
                    continue
            if s not in out:
                out.append(s)
        start = idx + len(needle)
    return out


# HARD caps to prevent Render/timeouts
PDF_MAX_PAGES = 14
DOCX_MAX_PARAS = 280
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
        info["text"] = text[:22000]  # internal use only
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
        info["text"] = text[:22000]  # internal use only
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


# CHANGED: builds an actual usable summary (headline facts + clean constraints + missing list)
def extract_pack_briefing(sections: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    text_blobs: list[str] = []
    strict_reqs: list[str] = []
    loose_reqs: list[str] = []

    # prefer where tender rules usually are; still include pdfs but filter later
    for cat in ["forms", "prelims", "addenda", "specs", "documents", "pdfs"]:
        for item in sections.get(cat, []):
            ex = item.get("extracted") or {}
            t = ex.get("text")
            if t and isinstance(t, str) and t.strip():
                # drop obviously irrelevant blobs early (stops BREEAM guidance etc)
                if _looks_irrelevant(t[:900]):
                    continue
                text_blobs.append(t)

            req = ex.get("requirements") or {}
            strict_reqs.extend(req.get("strict", []) or [])
            loose_reqs.extend(req.get("loose", []) or [])

    merged = "\n\n".join(text_blobs)[:160000]
    merged_lc = merged.lower()

    tender_return = _find_evidence(merged, TENDER_RETURN_PATTERNS, max_items=10)
    submission = _find_evidence(merged, SUBMISSION_PATTERNS, max_items=10)
    ld = _find_evidence(merged, LD_PATTERNS, max_items=10)
    retention = _find_evidence(merged, RETENTION_PATTERNS, max_items=10)
    programme = _find_evidence(merged, PROGRAMME_PATTERNS, max_items=10)
    working_hours = _find_evidence(merged, WORKING_HOURS_PATTERNS, max_items=10)
    insurance = _find_evidence(merged, INSURANCE_PATTERNS, max_items=10)
    accreditations = _find_evidence(merged, ACCREDITATION_PATTERNS, max_items=12)

    # flatten dates from per-doc extraction
    date_candidates: set[str] = set()
    for _, items in sections.items():
        for it in items:
            ex = it.get("extracted") or {}
            for d in ex.get("date_candidates", []) or []:
                date_candidates.add(str(d))

    # requirements: keep only tender-relevant lines (already filtered in extractor)
    def dedup_clean(items: list[str], limit: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for x in items:
            x2 = _normalize_line(x)
            if not x2:
                continue
            if not _is_tender_relevant_sentence(x2):
                continue
            k = x2.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(x2[:260])
            if len(out) >= limit:
                break
        return out

    strict_clean = dedup_clean(strict_reqs, 18)
    loose_clean = dedup_clean(loose_reqs, 18)

    # constraints: one good sentence per bucket (no massive blobs)
    constraints: list[str] = []
    for bucket, needles in RISK_BUCKETS.items():
        if not any(n in merged_lc for n in needles):
            continue

        best_ev = ""
        for needle in needles:
            if needle in merged_lc:
                evs = _find_bucket_evidence(merged, needle, bucket=bucket, max_items=1)
                if evs:
                    best_ev = evs[0]
                    break

        if best_ev:
            constraints.append(f"{bucket}: {best_ev}")

        if len(constraints) >= 10:
            break

    headline_deadline = _best_line_from_evidence(tender_return) or (sorted(date_candidates)[0] if date_candidates else None)
    headline_submission = _best_line_from_evidence(submission)
    headline_ld = _best_line_from_evidence(ld)
    headline_ret = _best_line_from_evidence(retention)
    headline_prog = _best_line_from_evidence(programme)
    headline_hours = _best_line_from_evidence(working_hours)
    headline_ins = _best_line_from_evidence(insurance)

    missing: list[str] = []
    if not tender_return and not date_candidates:
        missing.append("Tender return date / deadline not found")
    if not submission:
        missing.append("Submission method (email/portal/address) not found")
    if not programme:
        missing.append("Programme / duration not found")
    if not working_hours:
        missing.append("Working hours not found")
    if not retention:
        missing.append("Retention not found")
    if not ld:
        missing.append("Liquidated damages / LADs not found")
    if not insurance:
        missing.append("Insurance levels not found")
    if not accreditations:
        missing.append("Accreditations (CHAS/SMAS/etc) not found")

    lines: list[str] = []
    lines.append("EXECUTIVE SUMMARY")
    if headline_deadline:
        lines.append(f"• Deadline / key date: {headline_deadline}")
    if headline_submission:
        lines.append(f"• Submission route: {headline_submission}")
    if headline_prog:
        lines.append(f"• Programme / duration: {headline_prog}")
    if headline_hours:
        lines.append(f"• Working hours: {headline_hours}")
    if headline_ret:
        lines.append(f"• Retention: {headline_ret}")
    if headline_ld:
        lines.append(f"• LD / LADs: {headline_ld}")
    if headline_ins:
        lines.append(f"• Insurance: {headline_ins}")

    if len(lines) == 1:
        lines.append("• No commercial headline terms confidently detected in the first-pass scan.")

    executive_summary = "\n".join(lines)

    acc_short = []
    for x in (accreditations or [])[:12]:
        m = _normalize_line(x.get("match") or "")
        if m and _is_tender_relevant_sentence(m):
            acc_short.append(m[:160])
        if len(acc_short) >= 8:
            break

    return {
        "executive_summary": executive_summary,
        "key_facts": {
            "deadline_or_key_date": headline_deadline,
            "submission_route": headline_submission,
            "programme_duration": headline_prog,
            "working_hours": headline_hours,
            "retention": headline_ret,
            "liquidated_damages": headline_ld,
            "insurance_levels": headline_ins,
            "accreditations": acc_short,
        },
        "dates_found": sorted(date_candidates)[:40],
        "constraints": constraints,
        "requirements_strict": strict_clean,
        "requirements_loose": loose_clean,
        "missing": missing,
        "sources_scanned": len(text_blobs),
        # keep evidence for debugging / future UI toggles
        "evidence": {
            "tender_return_candidates": tender_return[:6],
            "submission_candidates": submission[:6],
            "programme_candidates": programme[:6],
            "working_hours_candidates": working_hours[:6],
            "retention_candidates": retention[:6],
            "liquidated_damages_candidates": ld[:6],
            "insurance_candidates": insurance[:6],
            "accreditations_candidates": accreditations[:8],
        },
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
                    
                    # Preserve folder structure if the browser provides it (folder upload)
# UploadFile.filename will be the relative path when using webkitdirectory
rel = (uf.filename or "").replace("\\", "/").lstrip("/")

# prevent traversal + remove empty/. /.. segments
rel_path = Path(rel)
safe_rel = Path(*[p for p in rel_path.parts if p not in ("", ".", "..")])

op = tmp_path / safe_rel
op.parent.mkdir(parents=True, exist_ok=True)
op.write_bytes(content)

scanned_paths.append((op, str(safe_rel)))

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

            # 3) briefing (returns executive_summary/constraints/missing)
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
        return JSONResponse({"error": f"Analyse failed: {e}"}, status_code=500)
