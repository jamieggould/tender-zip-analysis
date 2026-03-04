from __future__ import annotations

import re
import csv
import shutil
import tempfile
import zipfile
from pathlib import Path
from collections import Counter, defaultdict
from typing import Any, Optional

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
    s = (s or "").replace("\x00", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _snip_context(text: str, idx: int, window: int = 170) -> str:
    start = max(0, idx - window)
    end = min(len(text), idx + window)
    snippet = text[start:end]
    snippet = re.sub(r"\s+", " ", snippet).strip()
    return snippet[:380]


def _normalize_line(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _looks_irrelevant(text: str) -> bool:
    tl = (text or "").lower()
    return any(h in tl for h in IRRELEVANT_DOC_HINTS)


def _is_tabley(s: str) -> bool:
    return (s or "").count("|") >= 3


_SCHEDULE_GLUE_RE = re.compile(r"[A-Z]{2,}\d+[A-Z]{2,}|\d+[A-Z]{2,}", re.I)


def _looks_like_schedule_row(s: str) -> bool:
    s2 = _normalize_line(s)
    if not s2:
        return True
    if _is_tabley(s2):
        return True

    toks = [t for t in re.split(r"\s+", s2) if t]
    longest = max((len(t) for t in toks), default=0)
    if longest >= 26:
        return True

    words = re.findall(r"[A-Za-z]+", s2)
    if len(words) >= 10:
        caps = sum(1 for w in words if w.isupper() and len(w) >= 3)
        if caps / max(1, len(words)) > 0.55:
            return True

    if len(s2) > 90 and _SCHEDULE_GLUE_RE.search(s2):
        return True

    return False


def _is_gibberish_line(s: str) -> bool:
    s2 = _normalize_line(s)
    if len(s2) < 12:
        return True
    if _looks_like_schedule_row(s2):
        return True
    code_tokens = re.findall(r"\b[A-Z]{1,4}[-_ ]?\d{1,4}[A-Z]?\b", s2)
    if len(code_tokens) >= 6:
        return True
    letters = [c for c in s2 if c.isalpha()]
    if letters:
        upper_ratio = sum(1 for c in letters if c.isupper()) / max(1, len(letters))
        if upper_ratio > 0.75 and len(s2) > 60:
            return True
    return False


def _is_tender_relevant_sentence(s: str) -> bool:
    s2 = _normalize_line(s)
    if not s2 or _is_gibberish_line(s2):
        return False
    if _looks_irrelevant(s2):
        return False
    if s2.count(" ") < 6:
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
    return [p.strip() for p in SENT_SPLIT_RE.split(text) if p and p.strip()]


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


def _find_evidence_in_docs(
    docs: list[tuple[str, str]],
    patterns: list[str],
    max_items: int = 10,
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for fname, text in docs:
        if not text:
            continue
        hits = _find_evidence(text, patterns, max_items=6)
        for h in hits:
            out.append({"file": fname, "match": h.get("match", ""), "evidence": h.get("evidence", "")})
            if len(out) >= max_items:
                return out
    return out


def _best_line_from_evidence(items: list[dict[str, str]] | None) -> str | None:
    """
    Human output: NO references.
    Traceability: kept in briefing.evidence (file + evidence).
    """
    if not items:
        return None
    for it in items:
        m = _normalize_line(it.get("match") or "")
        if m and _is_tender_relevant_sentence(m):
            return m[:220]
    for it in items:
        ev = _normalize_line(it.get("evidence") or "")
        if ev and not _is_gibberish_line(ev):
            return ev[:240]
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
            chosen.append(sent[:260])

    return " ".join(chosen).strip()


def _find_bucket_evidence_in_docs(
    docs: list[tuple[str, str]],
    needles: list[str],
    bucket: str,
) -> str | None:
    """
    Human output: NO references.
    Traceability: if you need it later, use briefing.evidence.bucket_evidence (we include it).
    """
    for _, text in docs:
        if not text:
            continue
        tl = text.lower()
        for needle in needles:
            if needle not in tl:
                continue
            idx = tl.find(needle)
            s = _sentences_around(text, idx, max_sentences=2)
            if not s:
                continue
            if _looks_like_schedule_row(s) or _is_gibberish_line(s):
                continue
            if not _is_tender_relevant_sentence(s):
                continue

            if bucket == "Services / isolations":
                sl = s.lower()
                if not any(x in sl for x in ["isolation", "isolations", "live", "disconnect", "disconnection", "divert", "diversion"]):
                    continue

            return s[:240]
    return None


def _extract_requirements(text: str, max_lines: int = 40) -> dict[str, list[str]]:
    """
    Human output: clean bullets only, NO (file) prefixes.
    We keep the sources in briefing.evidence instead.
    """
    strict: list[str] = []
    loose: list[str] = []

    raw_lines = [l.strip() for l in re.split(r"[\r\n]+", text or "") if l and l.strip()]

    def accept_line(l: str) -> bool:
        l2 = _normalize_line(l)[:360]
        if not l2:
            return False
        if _looks_irrelevant(l2):
            return False
        if _looks_like_schedule_row(l2):
            return False
        if _is_gibberish_line(l2):
            return False
        return _is_tender_relevant_sentence(l2)

    for l in raw_lines:
        ll = l[:420]
        if not accept_line(ll):
            continue
        if ll.count(" ") < 6:
            continue
        if REQ_STRICT_RE.search(ll):
            strict.append(_normalize_line(ll)[:260])
        elif REQ_LOOSE_RE.search(ll):
            loose.append(_normalize_line(ll)[:260])
        if len(strict) >= max_lines and len(loose) >= max_lines:
            break

    if len(strict) < 6:
        for s in _split_sentences(text):
            ss = s[:420]
            if not accept_line(ss):
                continue
            if ss.count(" ") < 6:
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


# ---- NEW: signals so “FOUND/NOT FOUND” is accurate even with bad filenames ----
def _infer_signals(filename: str, text: str) -> set[str]:
    name = (filename or "").lower()
    t = (text or "").lower()

    signals: set[str] = set()

    # direct filename hints
    if any(h in name for h in DRAWING_HINTS):
        signals.add("drawings")

    if _has_any(name, KEYWORDS["boq"]):
        signals.add("boq")
    if _has_any(name, KEYWORDS["register"]):
        signals.add("registers")
    if _has_any(name, KEYWORDS["specs"]):
        signals.add("specs")
    if _has_any(name, KEYWORDS["prelims"]):
        signals.add("prelims")
    if _has_any(name, KEYWORDS["forms"]):
        signals.add("forms")
    if _has_any(name, KEYWORDS["addenda"]):
        signals.add("addenda")

    # text hints (lightweight and safe)
    # Only scan the first chunk to keep it fast.
    head = t[:20000]
    if any(k in head for k in ["bill of quantities", "schedule of rates", "pricing schedule", "rate", "sor"]):
        signals.add("boq")
    if any(k in head for k in ["document register", "drawing register", "issue register", "transmittal"]):
        signals.add("registers")
    if any(k in head for k in ["employer's requirements", "works information", "specification"]):
        signals.add("specs")
    if any(k in head for k in ["preliminaries", "prelims"]):
        signals.add("prelims")
    if any(k in head for k in ["form of tender", "tender form", "declaration", "questionnaire"]):
        signals.add("forms")
    if any(k in head for k in ["addendum", "clarification", "tender query", "rfi response"]):
        signals.add("addenda")
    if any(k in head for k in ["drawing", "general arrangement", "elevation", "section", "plan"]):
        signals.add("drawings")

    return signals


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
        "signals": [],
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
        info["signals"] = sorted(_infer_signals(path.name, text))

    except Exception as e:
        info["error"] = f"PDF read failed: {e}"

    return info


def extract_docx_info(path: Path, max_paras: int = DOCX_MAX_PARAS) -> dict[str, Any]:
    info: dict[str, Any] = {"headings": [], "keyword_hits": {}, "snippet": "", "text_len": 0, "signals": []}

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
        info["signals"] = sorted(_infer_signals(path.name, text))

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

        # Spreadsheet signals (filename + first sheet header text)
        head_text = " ".join(info["sheets"][0].get("header_guess", []) if info["sheets"] else [])
        info["signals"] = sorted(_infer_signals(path.name, head_text))

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
        info["signals"] = sorted(_infer_signals(path.name, " ".join(header)))

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
            "signals": ["drawings"],
        }

    return {"signals": sorted(_infer_signals(path.name, ""))}


def extract_pack_briefing(sections: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    docs: list[tuple[str, str]] = []
    strict_reqs: list[str] = []
    loose_reqs: list[str] = []

    all_signals: set[str] = set()

    for cat in ["forms", "prelims", "addenda", "specs", "documents", "pdfs", "spreadsheets", "registers", "boq", "drawings"]:
        for item in sections.get(cat, []):
            ex = item.get("extracted") or {}
            all_signals.update(ex.get("signals") or [])

            t = ex.get("text")
            if t and isinstance(t, str) and t.strip():
                if _looks_irrelevant(t[:900]):
                    continue
                fname = Path(item.get("file") or "").name or "unknown"
                docs.append((fname, t))

            req = ex.get("requirements") or {}
            strict_reqs.extend(req.get("strict", []) or [])
            loose_reqs.extend(req.get("loose", []) or [])

    tender_return = _find_evidence_in_docs(docs, TENDER_RETURN_PATTERNS, max_items=10)
    submission = _find_evidence_in_docs(docs, SUBMISSION_PATTERNS, max_items=10)
    ld = _find_evidence_in_docs(docs, LD_PATTERNS, max_items=10)
    retention = _find_evidence_in_docs(docs, RETENTION_PATTERNS, max_items=10)
    programme = _find_evidence_in_docs(docs, PROGRAMME_PATTERNS, max_items=10)
    working_hours = _find_evidence_in_docs(docs, WORKING_HOURS_PATTERNS, max_items=10)
    insurance = _find_evidence_in_docs(docs, INSURANCE_PATTERNS, max_items=10)
    accreditations = _find_evidence_in_docs(docs, ACCREDITATION_PATTERNS, max_items=12)

    date_candidates: set[str] = set()
    for _, items in sections.items():
        for it in items:
            ex = it.get("extracted") or {}
            for d in ex.get("date_candidates", []) or []:
                date_candidates.add(str(d))

    def dedup_clean(items: list[str], limit: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for x in items:
            x2 = _normalize_line(x)
            if not x2:
                continue
            if _looks_like_schedule_row(x2) or _is_gibberish_line(x2):
                continue
            core = x2.lower()
            if core in seen:
                continue
            seen.add(core)
            out.append(x2[:260])
            if len(out) >= limit:
                break
        return out

    strict_clean = dedup_clean(strict_reqs, 18)
    loose_clean = dedup_clean(loose_reqs, 18)

    constraints: list[str] = []
    bucket_evidence: dict[str, str] = {}
    for bucket, needles in RISK_BUCKETS.items():
        ev = _find_bucket_evidence_in_docs(docs, needles, bucket=bucket)
        if ev:
            constraints.append(f"{bucket}: {ev}")
            bucket_evidence[bucket] = ev
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

    # ---- NEW: estimator-ready notes (more detail + why it matters + action) ----
    def note(title: str, value: str | None, why: str, action: str, fallback: str) -> dict[str, str]:
        return {
            "title": title,
            "value": (value or "").strip() or fallback,
            "why_it_matters": why,
            "estimator_action": action,
        }

    estimator_notes: list[dict[str, str]] = []
    estimator_notes.append(note(
        "Tender deadline / key date",
        headline_deadline,
        "Locks programme + pricing sign-off window; missing it creates rework and risk.",
        "Confirm the ACTUAL return deadline and timezone; set internal bid freeze at least 24h before.",
        "Not clearly detected — check ITT / email for deadline.",
    ))
    estimator_notes.append(note(
        "Submission route",
        headline_submission,
        "Dictates packaging (portal naming rules vs email limits); easy to get disqualified.",
        "Confirm portal link / email address, file format rules, max attachment size, and naming convention.",
        "Not clearly detected — check ITT / cover email for submission instructions.",
    ))
    estimator_notes.append(note(
        "Programme / duration",
        headline_prog,
        "Affects prelims, labour curve, access strategy and sequencing.",
        "Confirm duration + constraints; price overtime/OOH if required; ensure temporary works allow programme.",
        "Not clearly detected — look for programme section or ERs constraints.",
    ))
    estimator_notes.append(note(
        "Working hours / OOH",
        headline_hours,
        "OOH restrictions drive labour cost, welfare, logistics, noise permits and sequencing.",
        "Check if noisy works restricted; allow for night/weekend premiums, permits, deliveries, lift bookings.",
        "Not clearly detected — check prelims / site rules.",
    ))
    estimator_notes.append(note(
        "Retention",
        headline_ret,
        "Impacts cashflow and final account strategy.",
        "Confirm % and release stages (PC / end of defects).",
        "Not clearly detected — check contract particulars / conditions.",
    ))
    estimator_notes.append(note(
        "LD / LADs",
        headline_ld,
        "Major commercial risk; may require float, acceleration, or exclusions.",
        "Confirm daily/weekly rate; consider acceleration allowance and programme risk commentary.",
        "Not clearly detected — check contract particulars / schedule.",
    ))
    estimator_notes.append(note(
        "Insurance levels",
        headline_ins,
        "Impacts compliance and subcontractor requirements.",
        "Confirm EL/PL values and any CAR/PI requirements; ensure subs match levels.",
        "Not clearly detected — check contract / ERs.",
    ))

    lines: list[str] = []
    lines.append("EXECUTIVE SUMMARY (Estimator-ready)")
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

    acc_short: list[str] = []
    for x in (accreditations or [])[:12]:
        m = _normalize_line(x.get("match") or "")
        if not m:
            continue
        if _looks_like_schedule_row(m) or _is_gibberish_line(m):
            continue
        acc_short.append(m[:150])
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
        "sources_scanned": len(docs),
        "signals_found": sorted(all_signals),
        "estimator_notes": estimator_notes,
        "evidence": {
            # evidence keeps file references so JSON is still audit-able,
            # but your human summary is now clean.
            "tender_return_candidates": tender_return[:6],
            "submission_candidates": submission[:6],
            "programme_candidates": programme[:6],
            "working_hours_candidates": working_hours[:6],
            "retention_candidates": retention[:6],
            "liquidated_damages_candidates": ld[:6],
            "insurance_candidates": insurance[:6],
            "accreditations_candidates": accreditations[:8],
            "bucket_evidence": bucket_evidence,
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


async def _save_upload_to_path(uf: UploadFile, dest: Path, max_bytes: int) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    with open(dest, "wb") as f:
        while True:
            chunk = await uf.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                raise ValueError("File too large")
            f.write(chunk)
    return size


@app.post("/api/analyse")
async def analyse(
    zip_file: Optional[list[UploadFile]] = File(None),
    files: Optional[list[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None),
):
    """
    Accepts:
      - one or more .zip files (will be extracted)
      - OR many individual files (including folder uploads where uf.filename contains subfolders)

    Key fix:
      - accept multiple common multipart field names (zip_file / files / file) so the frontend can't mismatch.
      - stream uploads to disk.
    """
    try:
        uploads: list[UploadFile] = []
        if zip_file:
            uploads.extend(zip_file)
        if files:
            uploads.extend(files)
        if file:
            uploads.append(file)

        if not uploads:
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

            for uf in uploads:
                if not uf.filename:
                    continue

                uploaded_names.append(uf.filename)

                if uf.filename.lower().endswith(".zip"):
                    zp = tmp_path / f"upload_{len(uploaded_names)}.zip"
                    try:
                        await _save_upload_to_path(uf, zp, max_bytes=max_bytes)
                    except ValueError:
                        return JSONResponse({"error": f"File too large (limit 300MB): {uf.filename}"}, status_code=400)

                    try:
                        extracted = safe_extract_zip(zp, extract_dir, max_files=5000)
                    except Exception as e:
                        return JSONResponse({"error": f"Could not extract ZIP {uf.filename}: {e}"}, status_code=400)

                    for p in extracted:
                        scanned_paths.append((p, str(p.relative_to(extract_dir))))
                else:
                    rel = (uf.filename or "").replace("\\", "/").lstrip("/")
                    rel_path = Path(rel)
                    safe_rel = Path(*[p for p in rel_path.parts if p not in ("", ".", "..")])
                    if str(safe_rel) in ("", "."):
                        safe_rel = Path(Path(uf.filename).name)

                    op = tmp_path / safe_rel
                    try:
                        await _save_upload_to_path(uf, op, max_bytes=max_bytes)
                    except ValueError:
                        return JSONResponse({"error": f"File too large (limit 300MB): {uf.filename}"}, status_code=400)

                    scanned_paths.append((op, str(safe_rel)))

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

            briefing = extract_pack_briefing(sections)
            _strip_internal_fields(sections)

            signals = set(briefing.get("signals_found") or [])
            # Use signals for “found” (more robust than category-only)
            def has_signal(sig: str) -> bool:
                return sig in signals

            report = {
                "summary": {
                    "uploaded_items": len(uploaded_names),
                    "uploaded_names": uploaded_names[:200],
                    "total_files_scanned": total_files,
                    "by_extension": dict(ext_counter.most_common()),
                    "by_category": dict(sorted(by_category_count.items(), key=lambda x: x[1], reverse=True)),
                    "boq_found": has_signal("boq"),
                    "register_found": has_signal("registers"),
                    "drawings_found": has_signal("drawings"),
                    "forms_found": has_signal("forms"),
                    "prelims_found": has_signal("prelims"),
                    "specs_found": has_signal("specs"),
                    "addenda_found": has_signal("addenda"),
                },
                "briefing": briefing,
                "sections": dict(sections),
            }

            return JSONResponse(report)

    except Exception as e:
        return JSONResponse({"error": f"Analyse failed: {e}"}, status_code=500)
