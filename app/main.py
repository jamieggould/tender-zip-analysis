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
    s = filename.upper()
    m = re.search(r"(?:REV[\s_\-]*)?([PC]\d{2,3})\b", s)
    return m.group(1) if m else None


# -------- routes --------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/analyse")
async def analyse(zip_file: list[UploadFile] = File(...)):
    """
    IMPORTANT: keep the form field name as `zip_file` so the existing frontend
    (/static/app.js) keeps working without changes.
    Now supports uploading multiple files and/or ZIPs.
    """
    if not zip_file:
        return JSONResponse({"error": "No files uploaded."}, status_code=400)

    max_bytes = 300 * 1024 * 1024

    by_category: dict[str, list[str]] = defaultdict(list)
    ext_counter: Counter[str] = Counter()
    drawings_by_number: dict[str, list[dict]] = defaultdict(list)

    total_scanned = 0

    def scan_path(fpath: Path, rel_name: str) -> None:
        nonlocal total_scanned

        ext = fpath.suffix.lower() if fpath.suffix else "(no_ext)"
        ext_counter[ext] += 1

        cat = classify_file(fpath)
        by_category[cat].append(rel_name)
        total_scanned += 1

        stem = fpath.stem.upper()
        m = re.match(r"([A-Z]{1,3}[-_]?\d{2,4})", stem)
        if m:
            drg = m.group(1).replace("_", "-")
            drawings_by_number[drg].append({"file": rel_name, "rev": guess_revision(fpath.name)})

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        extract_dir = tmp_path / "unzipped"
        extract_dir.mkdir(parents=True, exist_ok=True)

        uploaded_names: list[str] = []

        for uf in zip_file:
            if not uf.filename:
                continue

            uploaded_names.append(uf.filename)

            content = await uf.read()
            if len(content) > max_bytes:
                return JSONResponse(
                    {"error": f"File too large (limit 300MB): {uf.filename}"},
                    status_code=400,
                )

            # ZIP → extract and scan contents
            if uf.filename.lower().endswith(".zip"):
                zip_path = tmp_path / f"upload_{len(uploaded_names)}.zip"
                zip_path.write_bytes(content)

                try:
                    extracted = safe_extract_zip(zip_path, extract_dir, max_files=5000)
                except Exception as e:
                    return JSONResponse(
                        {"error": f"Could not extract ZIP {uf.filename}: {str(e)}"},
                        status_code=400
                    )

                for p in extracted:
                    rel = str(p.relative_to(extract_dir))
                    scan_path(p, rel)

            else:
                # Non-ZIP: save file and scan it directly (v1 only looks at name/ext)
                safe_name = Path(uf.filename).name
                out_path = tmp_path / safe_name
                out_path.write_bytes(content)
                scan_path(out_path, safe_name)

        report = {
            "summary": {
                "total_files": total_scanned,
                "uploaded_items": len(uploaded_names),
                "uploaded_names": uploaded_names[:200],
                "by_extension": dict(ext_counter.most_common()),
                "by_category": {k: len(v) for k, v in by_category.items()},
                "boq_found": len(by_category.get("boq", [])) > 0,
                "register_found": len(by_category.get("registers", [])) > 0,
                "addenda_found": len(by_category.get("addenda", [])) > 0,
                "zips_found": sum(1 for n in uploaded_names if n.lower().endswith(".zip")),
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
