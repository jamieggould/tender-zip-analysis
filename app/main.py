from __future__ import annotations

import csv
import io
import json
import os
import re
import shutil
import tempfile
import traceback
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from docx import Document
from fastapi import Body, FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openai import OpenAI
from openpyxl import load_workbook
from pypdf import PdfReader
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer
from starlette.requests import Request


app = FastAPI(title="Tender Pack Summary")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="templates")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_TIMEOUT_SEC = float(os.getenv("OPENAI_TIMEOUT_SEC", "14"))
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# performance tuning
PDF_MAX_PAGES = 8
DOCX_MAX_PARAS = 180
DOCX_MAX_TABLES = 16
DOCX_MAX_TABLE_ROWS = 80
MAX_FILES_TO_PROCESS = 12000
MAX_FILE_BYTES = 300 * 1024 * 1024
EXTRACT_WORKERS = 4

# deep-scan only a useful subset so big packs don't 504
BASE_DEEP_SCAN_LIMITS = {
    "boq": 4,
    "registers": 4,
    "drawings": 40,
    "forms": 6,
    "prelims": 6,
    "specs": 8,
    "addenda": 6,
    "documents": 6,
    "pdfs": 8,
    "spreadsheets": 4,
    "photos": 0,
    "other": 0,
}

KEYWORDS = {
    "boq": ["boq", "bill", "bq", "quantities", "pricing schedule", "schedule of rates", "sor", "price", "pricing"],
    "register": ["register", "drawing register", "document register", "issue register", "transmittal"],
    "addenda": ["addendum", "addenda", "clarification", "rfi response", "tender query", "tq", "query response"],
    "prelims": ["prelim", "prelims", "preliminary", "preliminary information"],
    "specs": ["spec", "specification", "employer", "requirements", "works information", "er", "scope"],
    "forms": ["form", "tender form", "declaration", "questionnaire", "pqq", "sq", "itt", "appendix", "submission"],
    "programme": ["programme", "program", "schedule", "gantt"],
}

DRAWING_HINTS = ["drg", "dwg", "drawing", "ga", "plan", "elev", "section", "sketch", "sk"]

ESTIMATOR_KEYWORDS = [
    "asbestos", "acm", "soft strip", "strip out", "demolition", "temporary works", "propping",
    "party wall", "working hours", "out of hours", "noise", "dust", "vibration",
    "traffic management", "tm", "permit", "permits", "consent", "licence", "license",
    "waste", "recycling", "segregation", "muck away", "skip", "haulage", "crushing", "arisings",
    "water", "electric", "gas", "services", "live", "isolation", "disconnect", "diversion",
    "section 61", "access", "logistics", "hoarding", "scaffold", "crane", "lift",
    "phasing", "sequence", "sequencing", "retention", "bond", "warranty", "insurance",
    "liquidated damages", "ld", "lad", "penalty",
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
    r"\b(closing date|deadline)\b\s*[:\-]?\s*([^\n]{0,160})",
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
    r"(working hours|site hours|hours of work)\s*[:\-]?\s*([^\n]{0,220})",
    r"(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?).{0,80}(\d{1,2}[:.]\d{2}).{0,40}(\d{1,2}[:.]\d{2})",
]
INSURANCE_PATTERNS = [
    r"\b(public liability|employers liability|EL|PL)\b.{0,140}(£\s?\d[\d,]*\.?\d*)",
    r"\b(insurance)\b.{0,180}(£\s?\d[\d,]*\.?\d*)",
]
ACCREDITATION_PATTERNS = [
    r"\b(CHAS|SMAS|SafeContractor|Constructionline|ISO\s?9001|ISO\s?14001|ISO\s?45001)\b.{0,180}",
]

REQ_STRICT_RE = re.compile(r"\b(must|shall|required|mandatory|as a minimum|minimum of|no later than)\b", re.I)
REQ_LOOSE_RE = re.compile(r"\b(should|please|requested|provide|submit|include|confirm)\b", re.I)
COMMERCIAL_SIGNAL_RE = re.compile(r"(£\s?\d|%\b|\bweeks?\b|\bmonths?\b|\b\d{1,2}[:.]\d{2}\b)", re.I)
SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?])\s+|\n+")
IRRELEVANT_DOC_HINTS = [
    "breeam", "credit", "wat 01", "assessor", "calculator", "guidance",
    "performance levels", "this document represents guidance",
]
REQUIREMENT_FLOOD_HINTS = [
    "designer", "architect", "design intent", "building regulations",
    "confidential", "not be disclosed", "treated as confidential",
    "acceptance shall not", "cdp", "supplementary drawings",
]
IGNORED_FILENAMES = {
    ".ds_store", "thumbs.db", "desktop.ini"
}
IGNORED_SUFFIXES = {
    ".tmp", ".bak", ".lock"
}


def _has_any(haystack: str, words: list[str]) -> bool:
    h = (haystack or "").lower()
    return any(w in h for w in words)


def _safe_json_loads(s: str) -> dict[str, Any] | None:
    try:
        data = json.loads(s)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _trim_list(items: Any, limit: int = 10, width: int = 320) -> list[str]:
    if not isinstance(items, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        if not isinstance(x, str):
            continue
        x2 = _normalize_line(x)[:width]
        if not x2:
            continue
        k = x2.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x2)
        if len(out) >= limit:
            break
    return out


def _clean_text(s: str) -> str:
    s = (s or "").replace("\x00", "")
    s = re.sub(r"(\w)-\n(\w)", r"\1\2", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = re.sub(r"(?<!\n)\n(?!\n)", " ", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _normalize_line(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _looks_irrelevant(text: str) -> bool:
    tl = (text or "").lower()
    return any(h in tl for h in IRRELEVANT_DOC_HINTS)


def _looks_like_schedule_row(s: str) -> bool:
    s2 = _normalize_line(s)
    if not s2:
        return True
    if s2.count("|") >= 2:
        return True
    tokens = re.findall(r"\b[A-Z]{1,4}[-_ ]?\d{1,6}[A-Z]?\b", s2)
    if len(tokens) >= 5:
        return True
    digits = sum(1 for c in s2 if c.isdigit())
    if digits >= 18 and len(s2) < 140:
        return True
    return False


def _is_gibberish_line(s: str) -> bool:
    s2 = _normalize_line(s)
    if len(s2) < 12:
        return True
    if _looks_like_schedule_row(s2):
        return True
    letters = [c for c in s2 if c.isalpha()]
    if letters:
        upper_ratio = sum(1 for c in letters if c.isupper()) / max(1, len(letters))
        if upper_ratio > 0.78 and len(s2) > 50:
            return True
    return False


def _split_sentences(text: str) -> list[str]:
    if not text:
        return []
    return [p.strip() for p in SENT_SPLIT_RE.split(text) if p and p.strip()]


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
        if sent and not _is_gibberish_line(sent) and not _looks_like_schedule_row(sent):
            chosen.append(sent[:360])

    return " ".join(chosen).strip()


def _sentence_score(s: str) -> int:
    s_l = s.lower()
    score = 0

    if COMMERCIAL_SIGNAL_RE.search(s):
        score += 6

    for kw in ESTIMATOR_KEYWORDS:
        if kw in s_l:
            score += 3

    if REQ_STRICT_RE.search(s):
        score += 2
    if REQ_LOOSE_RE.search(s):
        score += 1

    if any(h in s_l for h in REQUIREMENT_FLOOD_HINTS):
        score -= 5

    if len(s) < 90:
        score -= 2
    if s.count(" ") < 12:
        score -= 3

    return score


def _is_supported_upload(filename: str) -> bool:
    base = Path(filename).name.lower()
    if base in IGNORED_FILENAMES:
        return False
    if base.startswith("~$"):
        return False
    if Path(filename).suffix.lower() in IGNORED_SUFFIXES:
        return False
    return True


def _deep_scan_limit(category: str, total_files: int) -> int:
    limit = BASE_DEEP_SCAN_LIMITS.get(category, 0)
    if total_files > 120:
        limit = max(1, limit // 2) if limit > 0 else 0
    if total_files > 250:
        limit = max(1, limit // 2) if limit > 0 else 0
    return limit


def _build_ai_payload(briefing: dict[str, Any]) -> dict[str, Any]:
    evidence = briefing.get("evidence") or {}
    compact_evidence = {
        "tender_return_candidates": _trim_list(evidence.get("tender_return_candidates"), 3, 260),
        "submission_candidates": _trim_list(evidence.get("submission_candidates"), 3, 260),
        "programme_candidates": _trim_list(evidence.get("programme_candidates"), 3, 260),
        "working_hours_candidates": _trim_list(evidence.get("working_hours_candidates"), 3, 260),
        "retention_candidates": _trim_list(evidence.get("retention_candidates"), 3, 260),
        "liquidated_damages_candidates": _trim_list(evidence.get("liquidated_damages_candidates"), 3, 260),
        "insurance_candidates": _trim_list(evidence.get("insurance_candidates"), 3, 260),
        "accreditations_candidates": _trim_list(evidence.get("accreditations_candidates"), 3, 220),
    }

    return {
        "overview": briefing.get("overview", ""),
        "commercial_summary": briefing.get("commercial_summary", ""),
        "programme_access_summary": briefing.get("programme_access_summary", ""),
        "risk_summary": briefing.get("risk_summary", ""),
        "submission_summary": briefing.get("submission_summary", ""),
        "key_facts": briefing.get("key_facts", {}),
        "dates_found": briefing.get("dates_found", [])[:10],
        "constraints": briefing.get("constraints", [])[:8],
        "requirements_strict": briefing.get("requirements_strict", [])[:10],
        "requirements_loose": briefing.get("requirements_loose", [])[:8],
        "pricing_watchouts": briefing.get("pricing_watchouts", [])[:8],
        "clarifications": briefing.get("clarifications", [])[:8],
        "missing": briefing.get("missing", [])[:8],
        "evidence": compact_evidence,
    }


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

            zname = (m.filename or "").replace("\\", "/").lstrip("/")
            parts = [p for p in zname.split("/") if p not in ("", ".", "..")]
            safe_name = "/".join(parts) if parts else Path(m.filename).name
            if not _is_supported_upload(safe_name):
                continue

            out_path = (extract_to / safe_name).resolve()
            if not str(out_path).startswith(str(base)):
                raise ValueError("Unsafe ZIP: path traversal detected.")

            out_path.parent.mkdir(parents=True, exist_ok=True)
            with z.open(m, "r") as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted.append(out_path)

    return extracted


def classify_file(p: Path, display: str | None = None) -> str:
    ext = p.suffix.lower()
    name = p.name.lower()
    full = (display or p.as_posix()).lower()

    if ext in [".dwg", ".dxf"]:
        return "drawings"

    if ext in [".jpg", ".jpeg", ".png", ".webp"]:
        return "photos"

    if ext in [".xlsx", ".xls", ".csv"]:
        if _has_any(full, KEYWORDS["register"]) or _has_any(name, KEYWORDS["register"]):
            return "registers"
        if _has_any(full, KEYWORDS["boq"]) or _has_any(name, KEYWORDS["boq"]):
            return "boq"
        return "spreadsheets"

    if ext in [".docx", ".doc"]:
        if _has_any(full, KEYWORDS["forms"]) or _has_any(name, KEYWORDS["forms"]):
            return "forms"
        if _has_any(full, KEYWORDS["prelims"]) or _has_any(name, KEYWORDS["prelims"]):
            return "prelims"
        if _has_any(full, KEYWORDS["specs"]) or _has_any(name, KEYWORDS["specs"]):
            return "specs"
        return "documents"

    if ext == ".pdf":
        if _has_any(full, KEYWORDS["addenda"]) or _has_any(name, KEYWORDS["addenda"]):
            return "addenda"
        if _has_any(full, KEYWORDS["boq"]) or _has_any(name, KEYWORDS["boq"]):
            return "boq"
        if _has_any(full, KEYWORDS["register"]) or _has_any(name, KEYWORDS["register"]):
            return "registers"
        if _has_any(full, KEYWORDS["prelims"]) or _has_any(name, KEYWORDS["prelims"]):
            return "prelims"
        if _has_any(full, KEYWORDS["specs"]) or _has_any(name, KEYWORDS["specs"]):
            return "specs"
        if any(h in full for h in DRAWING_HINTS) or any(h in name for h in DRAWING_HINTS):
            return "drawings"
        return "pdfs"

    return "other"


def guess_revision(filename: str) -> str | None:
    s = (filename or "").upper()
    m = re.search(r"(?:REV[\s_\-]*)?([PC]\d{2,3})\b", s)
    return m.group(1) if m else None


def guess_drawing_number(filename: str) -> str | None:
    s = Path(filename).stem.upper()
    m = re.search(r"\b([A-Z]{1,4}[-_ ]?\d{2,6})\b", s)
    if m:
        return m.group(1).replace(" ", "-").replace("_", "-")
    return None


def _extract_requirements(text: str, max_lines: int = 20) -> dict[str, list[str]]:
    if not text:
        return {"strict": [], "loose": []}

    sentences = _split_sentences(text)
    candidates: list[tuple[int, str, str]] = []

    def clean(s: str) -> str | None:
        s2 = _normalize_line(s)
        if not s2:
            return None
        if _looks_irrelevant(s2):
            return None
        if _looks_like_schedule_row(s2) or _is_gibberish_line(s2):
            return None
        if s2[:1].islower() and not re.match(r"^(i|we)\b", s2.lower()):
            return None
        s2 = s2[:380]
        if _sentence_score(s2) <= 0:
            return None
        return s2

    for s in sentences:
        s2 = clean(s)
        if not s2:
            continue
        if REQ_STRICT_RE.search(s2):
            candidates.append((_sentence_score(s2), "strict", s2))
        elif REQ_LOOSE_RE.search(s2):
            candidates.append((_sentence_score(s2), "loose", s2))

    if sum(1 for _, b, _ in candidates if b == "strict") < 6:
        for m in REQ_STRICT_RE.finditer(text):
            chunk = _sentences_around(text, m.start(), max_sentences=2)
            chunk = clean(chunk)
            if not chunk:
                continue
            candidates.append((_sentence_score(chunk), "strict", chunk))

    candidates.sort(key=lambda x: x[0], reverse=True)

    def dedup(bucket: str, limit: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for _, b, s in candidates:
            if b != bucket:
                continue
            k = s.lower().strip()
            if k in seen:
                continue
            seen.add(k)
            out.append(s)
            if len(out) >= limit:
                break
        return out

    return {"strict": dedup("strict", max_lines), "loose": dedup("loose", max_lines)}


def _find_evidence(text: str, patterns: list[str], max_items: int = 6) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    if not text:
        return found

    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            ctx = _sentences_around(text, m.start(), max_sentences=2)
            ctx = _normalize_line(ctx)[:440]
            if not ctx or _looks_irrelevant(ctx) or _looks_like_schedule_row(ctx) or _is_gibberish_line(ctx):
                continue
            found.append({"match": ctx})
            if len(found) >= max_items:
                return found

    return found


def _best_line_from_evidence(items: list[dict[str, str]] | None) -> str | None:
    if not items:
        return None
    best: tuple[int, str] | None = None
    for it in items:
        s = _normalize_line(it.get("match") or "")
        if not s:
            continue
        sc = _sentence_score(s)
        if best is None or sc > best[0]:
            best = (sc, s)
    return best[1][:320] if best else None


def _find_bucket_evidence(merged: str, needle: str, bucket: str, max_items: int = 1) -> list[str]:
    merged_lc = merged.lower()
    out: list[str] = []
    start = 0
    while len(out) < max_items:
        idx = merged_lc.find(needle, start)
        if idx == -1:
            break
        s = _sentences_around(merged, idx, max_sentences=2)
        s = _normalize_line(s)[:440]
        if s and not _looks_irrelevant(s) and not _looks_like_schedule_row(s) and not _is_gibberish_line(s):
            sl = s.lower()
            if bucket == "Services / isolations":
                if not any(x in sl for x in ["isolation", "isolations", "live", "disconnect", "disconnection", "divert", "diversion"]):
                    start = idx + len(needle)
                    continue
            if _sentence_score(s) > 0 and s not in out:
                out.append(s)
        start = idx + len(needle)
    return out


def extract_pdf_info(path: Path, max_pages: int = PDF_MAX_PAGES) -> dict[str, Any]:
    info: dict[str, Any] = {
        "pages": None,
        "keyword_hits": {},
        "date_candidates": [],
        "snippet": "",
        "text_len": 0,
        "requirements": {"strict": [], "loose": []},
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

        info["snippet"] = text[:1000]
        info["text"] = text[:18000]
        info["requirements"] = _extract_requirements(text)

    except Exception as e:
        info["error"] = f"PDF read failed: {e}"

    return info


def extract_docx_info(path: Path, max_paras: int = DOCX_MAX_PARAS) -> dict[str, Any]:
    info: dict[str, Any] = {"headings": [], "keyword_hits": {}, "snippet": "", "text_len": 0, "requirements": {"strict": [], "loose": []}}

    try:
        doc = Document(str(path))

        paras = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]
        paras = paras[:max_paras]

        table_lines: list[str] = []
        for table in doc.tables[:DOCX_MAX_TABLES]:
            for row in table.rows[:DOCX_MAX_TABLE_ROWS]:
                cells = [c.text.strip() for c in row.cells if c.text and c.text.strip()]
                if cells:
                    table_lines.append(" ".join(cells)[:320])

        text = _clean_text("\n".join(paras + table_lines))
        info["text_len"] = len(text)

        headings: list[str] = []
        for p in doc.paragraphs:
            if p.style and p.style.name and "Heading" in p.style.name and p.text.strip():
                headings.append(p.text.strip())
            if len(headings) >= 24:
                break
        info["headings"] = headings

        text_lc = text.lower()
        hits: dict[str, int] = {}
        for kw in ESTIMATOR_KEYWORDS:
            if kw in text_lc:
                hits[kw] = text_lc.count(kw)
        info["keyword_hits"] = dict(sorted(hits.items(), key=lambda x: x[1], reverse=True)[:25])

        info["snippet"] = text[:1000]
        info["text"] = text[:18000]
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
        for name in wb.sheetnames[:12]:
            ws = wb[name]
            rows: list[list[str]] = []
            for r in ws.iter_rows(min_row=1, max_row=24, values_only=True):
                rows.append([("" if v is None else str(v)).strip() for v in r][:20])

            header = rows[0] if rows else []
            colmap = detect_boq_columns(header) if header else {}

            info["sheets"].append({
                "name": name,
                "header_guess": header[:16],
                "boq_column_map": colmap,
                "preview_rows": rows[:6],
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
                rows.append([c.strip() for c in r][:20])
                if i >= 20:
                    break

        header = rows[0] if rows else []
        info["header_guess"] = header[:16]
        info["boq_column_map"] = detect_boq_columns(header) if header else {}
        info["preview_rows"] = rows[:6]

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

    for cat in ["forms", "prelims", "addenda", "specs", "documents", "pdfs"]:
        for item in sections.get(cat, []):
            ex = item.get("extracted") or {}
            t = ex.get("text")
            if t and isinstance(t, str) and t.strip():
                if _looks_irrelevant(t[:900]):
                    continue
                text_blobs.append(t)

            req = ex.get("requirements") or {}
            strict_reqs.extend(req.get("strict", []) or [])
            loose_reqs.extend(req.get("loose", []) or [])

    merged = "\n\n".join(text_blobs)[:120000]
    merged_lc = merged.lower()

    tender_return = _find_evidence(merged, TENDER_RETURN_PATTERNS, max_items=8)
    submission = _find_evidence(merged, SUBMISSION_PATTERNS, max_items=8)
    ld = _find_evidence(merged, LD_PATTERNS, max_items=8)
    retention = _find_evidence(merged, RETENTION_PATTERNS, max_items=8)
    programme = _find_evidence(merged, PROGRAMME_PATTERNS, max_items=8)
    working_hours = _find_evidence(merged, WORKING_HOURS_PATTERNS, max_items=8)
    insurance = _find_evidence(merged, INSURANCE_PATTERNS, max_items=8)
    accreditations = _find_evidence(merged, ACCREDITATION_PATTERNS, max_items=8)

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
            if _looks_irrelevant(x2) or _looks_like_schedule_row(x2) or _is_gibberish_line(x2):
                continue
            if _sentence_score(x2) <= 0:
                continue
            k = x2.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(x2[:340])
            if len(out) >= limit:
                break
        return out

    strict_clean = dedup_clean(strict_reqs, 14)
    loose_clean = dedup_clean(loose_reqs, 10)

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

        if len(constraints) >= 8:
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

    executive_lines: list[str] = []
    if headline_deadline:
        executive_lines.append(f"Deadline / key date: {headline_deadline}")
    if headline_submission:
        executive_lines.append(f"Submission route: {headline_submission}")
    if headline_prog:
        executive_lines.append(f"Programme / duration: {headline_prog}")
    if headline_hours:
        executive_lines.append(f"Working hours: {headline_hours}")
    if headline_ret:
        executive_lines.append(f"Retention: {headline_ret}")
    if headline_ld:
        executive_lines.append(f"LD / LADs: {headline_ld}")
    if headline_ins:
        executive_lines.append(f"Insurance: {headline_ins}")

    if not executive_lines:
        executive_lines.append("No clear commercial headline terms detected from the first-pass scan.")

    overview_parts: list[str] = []
    if headline_deadline:
        overview_parts.append(f"The tender appears to close on {headline_deadline}.")
    if headline_submission:
        overview_parts.append(f"Submission instructions indicate {headline_submission.lower()}.")
    if headline_prog:
        overview_parts.append(f"The programme information suggests {headline_prog.lower()}.")
    if headline_hours:
        overview_parts.append(f"Access or working hour constraints indicate {headline_hours.lower()}.")
    if not overview_parts:
        overview_parts.append("The pack has been scanned and the strongest commercial and logistical indicators are summarised below.")

    commercial_summary_parts: list[str] = []
    if headline_ld:
        commercial_summary_parts.append(f"Liquidated damages appear to be referenced: {headline_ld}.")
    if headline_ret:
        commercial_summary_parts.append(f"Retention provisions appear to be referenced: {headline_ret}.")
    if headline_ins:
        commercial_summary_parts.append(f"Insurance requirements appear to be referenced: {headline_ins}.")
    if not commercial_summary_parts:
        commercial_summary_parts.append("No strong commercial headline terms were confidently extracted beyond the core tender instructions.")

    pricing_watchouts = strict_clean[:8] if strict_clean else constraints[:8]

    clarifications = missing[:]
    if not clarifications and not submission:
        clarifications.append("Confirm tender submission route and any file size or naming constraints.")
    if not clarifications and not headline_prog:
        clarifications.append("Confirm programme duration, milestones, and any phased handover requirements.")

    acc_short: list[str] = []
    for x in (accreditations or [])[:8]:
        m = _normalize_line(x.get("match") or "")
        if not m:
            continue
        if _looks_irrelevant(m) or _looks_like_schedule_row(m) or _is_gibberish_line(m):
            continue
        if m not in acc_short:
            acc_short.append(m[:220])
        if len(acc_short) >= 6:
            break

    return {
        "title": "Tender Pack Summary",
        "executive_summary": "EXECUTIVE SUMMARY\n" + "\n".join([f"• {x}" for x in executive_lines]),
        "overview": " ".join(overview_parts),
        "commercial_summary": " ".join(commercial_summary_parts),
        "programme_access_summary": " ".join(constraints[:4]) if constraints else "No strong programme or access constraints were confidently extracted.",
        "risk_summary": " ".join(constraints[:5]) if constraints else "No major delivery risks were confidently extracted from the scanned text.",
        "submission_summary": headline_submission or "Submission method was not confidently extracted.",
        "pricing_watchouts": pricing_watchouts,
        "clarifications": clarifications[:10],
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
        "evidence": {
            "tender_return_candidates": [x.get("match") for x in (tender_return or [])[:4] if x.get("match")],
            "submission_candidates": [x.get("match") for x in (submission or [])[:4] if x.get("match")],
            "programme_candidates": [x.get("match") for x in (programme or [])[:4] if x.get("match")],
            "working_hours_candidates": [x.get("match") for x in (working_hours or [])[:4] if x.get("match")],
            "retention_candidates": [x.get("match") for x in (retention or [])[:4] if x.get("match")],
            "liquidated_damages_candidates": [x.get("match") for x in (ld or [])[:4] if x.get("match")],
            "insurance_candidates": [x.get("match") for x in (insurance or [])[:4] if x.get("match")],
            "accreditations_candidates": acc_short[:6],
        },
    }


def ai_enhance_briefing(briefing: dict[str, Any]) -> dict[str, Any]:
    if not openai_client:
        return briefing

    payload = _build_ai_payload(briefing)

    system_prompt = """
You are a senior UK construction estimator reviewing a tender pack.

You will receive extracted evidence from tender documents.
Turn it into a clean, practical tender summary page that reads like a report, not like notes.

Rules:
- Use ONLY the provided evidence.
- Ignore broken fragments or anything unclear.
- Do NOT mention file names, sources, or "the evidence says".
- Write clearly and commercially.
- Expand each section into useful plain English, but stay concise.
- Prioritise deadline, submission route, programme, access, working hours, LDs, retention, insurance, permits, asbestos, isolations, waste, temporary works.
- Only keep requirements that matter to pricing, delivery, risk, logistics, compliance, or tender submission.
- If a date looks suspicious or unsupported, do not overstate it.
- Do not combine unrelated fragments into one sentence.
- Prefer short, readable sentences over dense wording.

Return STRICT JSON with this exact structure:
{
  "title": "string",
  "executive_summary": "string",
  "overview": "string",
  "commercial_summary": "string",
  "programme_access_summary": "string",
  "risk_summary": "string",
  "submission_summary": "string",
  "pricing_watchouts": ["..."],
  "clarifications": ["..."],
  "key_facts": {
    "deadline_or_key_date": "string or null",
    "submission_route": "string or null",
    "programme_duration": "string or null",
    "working_hours": "string or null",
    "retention": "string or null",
    "liquidated_damages": "string or null",
    "insurance_levels": "string or null",
    "accreditations": ["..."]
  },
  "constraints": ["..."],
  "requirements_strict": ["..."],
  "requirements_loose": ["..."],
  "missing": ["..."]
}
"""

    try:
        resp = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.1,
            timeout=OPENAI_TIMEOUT_SEC,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
        )

        content = resp.choices[0].message.content or ""
        parsed = _safe_json_loads(content)
        if not parsed:
            return briefing

        enhanced = {
            "title": parsed.get("title") or briefing.get("title") or "Tender Pack Summary",
            "executive_summary": parsed.get("executive_summary") or briefing.get("executive_summary"),
            "overview": parsed.get("overview") or briefing.get("overview"),
            "commercial_summary": parsed.get("commercial_summary") or briefing.get("commercial_summary"),
            "programme_access_summary": parsed.get("programme_access_summary") or briefing.get("programme_access_summary"),
            "risk_summary": parsed.get("risk_summary") or briefing.get("risk_summary"),
            "submission_summary": parsed.get("submission_summary") or briefing.get("submission_summary"),
            "pricing_watchouts": _trim_list(parsed.get("pricing_watchouts"), 10, 340) or briefing.get("pricing_watchouts", []),
            "clarifications": _trim_list(parsed.get("clarifications"), 10, 340) or briefing.get("clarifications", []),
            "key_facts": {
                "deadline_or_key_date": (parsed.get("key_facts") or {}).get("deadline_or_key_date") or (briefing.get("key_facts") or {}).get("deadline_or_key_date"),
                "submission_route": (parsed.get("key_facts") or {}).get("submission_route") or (briefing.get("key_facts") or {}).get("submission_route"),
                "programme_duration": (parsed.get("key_facts") or {}).get("programme_duration") or (briefing.get("key_facts") or {}).get("programme_duration"),
                "working_hours": (parsed.get("key_facts") or {}).get("working_hours") or (briefing.get("key_facts") or {}).get("working_hours"),
                "retention": (parsed.get("key_facts") or {}).get("retention") or (briefing.get("key_facts") or {}).get("retention"),
                "liquidated_damages": (parsed.get("key_facts") or {}).get("liquidated_damages") or (briefing.get("key_facts") or {}).get("liquidated_damages"),
                "insurance_levels": (parsed.get("key_facts") or {}).get("insurance_levels") or (briefing.get("key_facts") or {}).get("insurance_levels"),
                "accreditations": _trim_list((parsed.get("key_facts") or {}).get("accreditations"), 6, 220) or (briefing.get("key_facts") or {}).get("accreditations", []),
            },
            "dates_found": briefing.get("dates_found", []),
            "constraints": _trim_list(parsed.get("constraints"), 10, 340) or briefing.get("constraints", []),
            "requirements_strict": _trim_list(parsed.get("requirements_strict"), 14, 340) or briefing.get("requirements_strict", []),
            "requirements_loose": _trim_list(parsed.get("requirements_loose"), 12, 340) or briefing.get("requirements_loose", []),
            "missing": _trim_list(parsed.get("missing"), 10, 220) or briefing.get("missing", []),
            "sources_scanned": briefing.get("sources_scanned", 0),
            "evidence": briefing.get("evidence", {}),
            "ai_used": True,
        }
        return enhanced

    except Exception:
        return briefing


def build_pdf_report(report: dict[str, Any]) -> bytes:
    briefing = report.get("briefing") or {}
    summary = report.get("summary") or {}
    key_facts = briefing.get("key_facts") or {}

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        rightMargin=16 * mm,
        leftMargin=16 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title=(briefing.get("title") or "Tender Pack Summary"),
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TitleSmall", parent=styles["Title"], fontSize=20, leading=24, textColor=colors.HexColor("#111827")))
    styles.add(ParagraphStyle(name="SectionHead", parent=styles["Heading2"], fontSize=12, leading=15, textColor=colors.HexColor("#374151"), spaceAfter=6))
    styles.add(ParagraphStyle(name="BodySmall", parent=styles["BodyText"], fontSize=9.5, leading=13, textColor=colors.HexColor("#111827")))
    styles.add(ParagraphStyle(name="Meta", parent=styles["BodyText"], fontSize=8.5, leading=11, textColor=colors.HexColor("#6B7280")))

    story: list[Any] = []

    def esc(s: Any) -> str:
        return (
            str(s or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def add_paragraph(text: str, style: str = "BodySmall") -> None:
        if text:
            story.append(Paragraph(esc(text), styles[style]))
            story.append(Spacer(1, 3 * mm))

    def add_bullets(items: list[str]) -> None:
        cleaned = [i for i in items if isinstance(i, str) and i.strip()]
        if not cleaned:
            return
        flow = ListFlowable(
            [ListItem(Paragraph(esc(i), styles["BodySmall"])) for i in cleaned],
            bulletType="bullet",
            leftPadding=14,
        )
        story.append(flow)
        story.append(Spacer(1, 3 * mm))

    title = briefing.get("title") or "Tender Pack Summary"
    story.append(Paragraph(esc(title), styles["TitleSmall"]))
    story.append(
        Paragraph(
            esc(f"Generated {datetime.utcnow().strftime('%d %b %Y %H:%M UTC')} • Files scanned: {summary.get('total_files_scanned', 0)}"),
            styles["Meta"],
        )
    )
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Executive Summary", styles["SectionHead"]))
    add_paragraph(briefing.get("overview") or briefing.get("executive_summary") or "")

    story.append(Paragraph("Commercial Summary", styles["SectionHead"]))
    add_paragraph(briefing.get("commercial_summary") or "")

    story.append(Paragraph("Programme and Access", styles["SectionHead"]))
    add_paragraph(briefing.get("programme_access_summary") or "")

    story.append(Paragraph("Risk Summary", styles["SectionHead"]))
    add_paragraph(briefing.get("risk_summary") or "")

    story.append(Paragraph("Submission Summary", styles["SectionHead"]))
    add_paragraph(briefing.get("submission_summary") or "")

    story.append(Paragraph("Key Facts", styles["SectionHead"]))
    facts: list[str] = []
    fact_map = [
        ("Deadline / key date", key_facts.get("deadline_or_key_date")),
        ("Submission route", key_facts.get("submission_route")),
        ("Programme / duration", key_facts.get("programme_duration")),
        ("Working hours", key_facts.get("working_hours")),
        ("Retention", key_facts.get("retention")),
        ("LD / LADs", key_facts.get("liquidated_damages")),
        ("Insurance", key_facts.get("insurance_levels")),
    ]
    for label, value in fact_map:
        if value:
            facts.append(f"{label}: {value}")
    if key_facts.get("accreditations"):
        facts.append("Accreditations: " + ", ".join(key_facts["accreditations"][:6]))
    add_bullets(facts or ["No key facts were confidently extracted."])

    story.append(Paragraph("Pricing Watchouts", styles["SectionHead"]))
    add_bullets(briefing.get("pricing_watchouts") or ["No pricing watchouts were confidently extracted."])

    story.append(Paragraph("Key Constraints", styles["SectionHead"]))
    add_bullets(briefing.get("constraints") or ["No strong constraints were confidently extracted."])

    story.append(Paragraph("Mandatory Requirements", styles["SectionHead"]))
    add_bullets((briefing.get("requirements_strict") or [])[:12] or ["No strong mandatory requirements were confidently extracted."])

    story.append(Paragraph("Other Submission Requests", styles["SectionHead"]))
    add_bullets((briefing.get("requirements_loose") or [])[:10] or ["No other clear submission requests were confidently extracted."])

    story.append(Paragraph("Clarifications / Queries to Raise", styles["SectionHead"]))
    add_bullets(briefing.get("clarifications") or briefing.get("missing") or ["No additional clarifications suggested."])

    doc.build(story)
    return buf.getvalue()


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


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


async def _save_upload_to_path(uf: UploadFile, dest: Path, max_bytes: int) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    with open(dest, "wb") as f:
        while True:
            chunk = await uf.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                raise ValueError("File too large")
            f.write(chunk)
    return size


@app.post("/api/report/pdf")
async def report_pdf(payload: dict[str, Any] = Body(...)):
    pdf_bytes = build_pdf_report(payload)
    filename = "tender-summary.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/analyse")
async def analyse(
    zip_file: Optional[list[UploadFile]] = File(None),
    files: Optional[list[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None),
):
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

        sections: dict[str, list[dict[str, Any]]] = defaultdict(list)
        ext_counter: Counter[str] = Counter()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            extract_dir = tmp_path / "unzipped"
            extract_dir.mkdir(parents=True, exist_ok=True)

            uploaded_names: list[str] = []
            scanned_paths: list[tuple[Path, str]] = []

            for uf in uploads:
                if not uf.filename or not _is_supported_upload(uf.filename):
                    continue

                uploaded_names.append(uf.filename)

                if uf.filename.lower().endswith(".zip"):
                    zp = tmp_path / f"upload_{len(uploaded_names)}.zip"
                    try:
                        await _save_upload_to_path(uf, zp, max_bytes=MAX_FILE_BYTES)
                    except ValueError:
                        return JSONResponse({"error": f"File too large (limit 300MB): {uf.filename}"}, status_code=400)

                    try:
                        extracted = safe_extract_zip(zp, extract_dir, max_files=15000)
                    except Exception as e:
                        return JSONResponse({"error": f"Could not extract ZIP {uf.filename}: {e}"}, status_code=400)

                    for p in extracted:
                        scanned_paths.append((p, str(p.relative_to(extract_dir)).replace("\\", "/")))
                else:
                    rel = (uf.filename or "").replace("\\", "/").lstrip("/")
                    rel_path = Path(rel)
                    safe_rel = Path(*[p for p in rel_path.parts if p not in ("", ".", "..")])
                    if str(safe_rel) in ("", "."):
                        safe_rel = Path(Path(uf.filename).name)

                    if not _is_supported_upload(str(safe_rel)):
                        continue

                    op = tmp_path / safe_rel
                    try:
                        await _save_upload_to_path(uf, op, max_bytes=MAX_FILE_BYTES)
                    except ValueError:
                        return JSONResponse({"error": f"File too large (limit 300MB): {uf.filename}"}, status_code=400)

                    scanned_paths.append((op, str(safe_rel).replace("\\", "/")))

            total_available_files = len(scanned_paths)
            if total_available_files == 0:
                return JSONResponse({"error": "No supported files found in upload."}, status_code=400)

            total_files = 0
            by_category_count: dict[str, int] = defaultdict(int)
            deep_scan_count: dict[str, int] = defaultdict(int)
            extraction_jobs: list[tuple[dict[str, Any], Path, str]] = []

            # classify/count everything first, but only deep-scan a subset
            for p, display in scanned_paths[:MAX_FILES_TO_PROCESS]:
                total_files += 1
                ext = p.suffix.lower() if p.suffix else "(no_ext)"
                ext_counter[ext] += 1

                category = classify_file(p, display=display)
                by_category_count[category] += 1

                item = {
                    "file": display,
                    "ext": ext,
                    "category": category,
                    "extracted": {},
                }
                sections[category].append(item)

                limit = _deep_scan_limit(category, total_available_files)
                if deep_scan_count[category] < limit:
                    deep_scan_count[category] += 1
                    extraction_jobs.append((item, p, category))

            # deep extract in parallel
            def _run_extract(job: tuple[dict[str, Any], Path, str]) -> tuple[dict[str, Any], dict[str, Any]]:
                item, p, category = job
                return item, extract_by_type(p, category)

            if extraction_jobs:
                with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as executor:
                    futures = [executor.submit(_run_extract, job) for job in extraction_jobs]
                    for future in as_completed(futures):
                        try:
                            item, extracted = future.result()
                            item["extracted"] = extracted
                        except Exception as e:
                            # keep going even if one file fails
                            print(f"EXTRACT ERROR: {e}")

            def has_cat(cat: str) -> bool:
                return len(sections.get(cat, [])) > 0

            raw_briefing = extract_pack_briefing(sections)
            briefing = ai_enhance_briefing(raw_briefing)

            _strip_internal_fields(sections)

            report = {
                "summary": {
                    "uploaded_items": len(uploaded_names),
                    "uploaded_names": uploaded_names[:200],
                    "total_files_scanned": total_files,
                    "total_files_available": total_available_files,
                    "deep_scanned_files": sum(deep_scan_count.values()),
                    "by_extension": dict(ext_counter.most_common()),
                    "by_category": dict(sorted(by_category_count.items(), key=lambda x: x[1], reverse=True)),
                    "boq_found": has_cat("boq"),
                    "register_found": has_cat("registers"),
                    "drawings_found": has_cat("drawings"),
                    "forms_found": has_cat("forms"),
                    "prelims_found": has_cat("prelims"),
                    "specs_found": has_cat("specs"),
                    "addenda_found": has_cat("addenda"),
                    "ai_enabled": bool(openai_client),
                    "ai_model": OPENAI_MODEL if openai_client else None,
                    "ai_used": bool(briefing.get("ai_used")),
                },
                "briefing": briefing,
                "sections": dict(sections),
            }

            return JSONResponse(report)

    except Exception as e:
        print("ANALYSE ERROR:")
        traceback.print_exc()
        return JSONResponse({"error": f"Analyse failed: {e}"}, status_code=500)