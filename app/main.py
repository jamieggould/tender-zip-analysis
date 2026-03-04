from __future__ import annotations

import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from collections import Counter, defaultdict

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

app = FastAPI(title="Tender ZIP Analyser")

# Serve frontend files
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="templates")


# -------- ZIP safety --------
def safe_extract_zip(zip_path: Path, extract_to: Path, max_files: int = 5000) -> list[Path]:
    """
    Safely extract ZIP contents:
    - blocks path traversal ("zip slip")
    - limits file count
    """
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


# -------- simple v1 classification --------
def classify_file(p: Path) -> str:
    name = p.name.lower()
    ext = p.suffix.lower()

    if ext in [".dwg", ".dxf"]:
        return "drawings"
    if ext in [".xlsx", ".xls", ".csv"]:
        if "register" in name:
            return "registers"
        if any(k in name for k in ["boq", "bill", "pricing", "schedule"]):
            return "boq"
        return "spreadsheets"
    if ext in [".docx", ".doc"]:
        if any(k in name for k in ["form", "tender", "declaration", "questionnaire", "pqq", "sq", "itt"]):
            return "forms"
        return "documents"
    if ext == ".pdf":
        if any(k in name for k in ["addendum", "addenda", "clarification"]):
            return "addenda"
        if any(k in name for k in ["prelim", "spec", "requirement", "employer"]):
            return "specs"
        return "pdfs"
    if ext in [".jpg", ".jpeg", ".png"]:
        return "images"
    return "other"


def guess_revision(filename: str) -> str | None:
    """
    Guess revision from filename, e.g.:
    - REV_P03 / _P03 / -P03
    - REV C01
    """
    s = filename.upper()
    m = re.search(r"(?:REV[\s_\-]*)?([PC]\d{2,3})\b", s)
    return m.group(1) if m else None


# -------- routes --------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/analyse")
async def analyse(zip_file: UploadFile = File(...)):
    if not zip_file.filename.lower().endswith(".zip"):
        return JSONResponse({"error": "Please upload a .zip file."}, status_code=400)

    content = await zip_file.read()
    if len(content) > 300 * 1024 * 1024:
        return JSONResponse({"error": "ZIP too large for v1 (limit 300MB)."}, status_code=400)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        zip_path = tmp_path / "upload.zip"
        zip_path.write_bytes(content)

        extract_dir = tmp_path / "unzipped"
        extract_dir.mkdir(parents=True, exist_ok=True)

        try:
            files = safe_extract_zip(zip_path, extract_dir, max_files=5000)
        except Exception as e:
            return JSONResponse({"error": f"Could not extract ZIP: {str(e)}"}, status_code=400)

        by_category: dict[str, list[str]] = defaultdict(list)
        ext_counter: Counter[str] = Counter()

        drawings_by_number: dict[str, list[dict]] = defaultdict(list)

        for f in files:
            rel = str(f.relative_to(extract_dir))
            ext = f.suffix.lower() if f.suffix else "(no_ext)"
            ext_counter[ext] += 1

            cat = classify_file(f)
            by_category[cat].append(rel)

            # crude drawing number guess from filename start, e.g. A101, D-102, SK123
            stem = f.stem.upper()
            m = re.match(r"([A-Z]{1,3}[-_]?\d{2,4})", stem)
            if m:
                drg = m.group(1).replace("_", "-")
                drawings_by_number[drg].append({"file": rel, "rev": guess_revision(f.name)})

        report = {
            "summary": {
                "total_files": len(files),
                "by_extension": dict(ext_counter.most_common()),
                "by_category": {k: len(v) for k, v in by_category.items()},
                "boq_found": len(by_category.get("boq", [])) > 0,
                "register_found": len(by_category.get("registers", [])) > 0,
                "addenda_found": len(by_category.get("addenda", [])) > 0,
            },
            "top_hits": {
                "boq_files": by_category.get("boq", [])[:50],
                "register_files": by_category.get("registers", [])[:50],
                "addenda_files": by_category.get("addenda", [])[:50],
                "forms": by_category.get("forms", [])[:50],
                "specs": by_category.get("specs", [])[:50],
                "drawings_files": by_category.get("drawings", [])[:50],
            },
            "drawings": {
                "count_guess": len(drawings_by_number),
                "items": drawings_by_number,
            },
            "category_samples": {k: v[:30] for k, v in by_category.items()},
        }

        return JSONResponse(report)
