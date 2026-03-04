from __future__ import annotations

import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from collections import Counter, defaultdict

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()


# ---------- helpers ----------
def safe_extract_zip(zip_path: Path, extract_to: Path, max_files: int = 5000) -> list[Path]:
    """
    Safely extract a zip to extract_to.
    - Prevents zip-slip (path traversal)
    - Limits total file count
    Returns list of extracted file paths.
    """
    extracted_files: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as z:
        members = z.infolist()
        if len(members) > max_files:
            raise ValueError(f"Zip has too many files ({len(members)}). Limit is {max_files}.")

        for member in members:
            # skip directories
            if member.is_dir():
                continue

            # Resolve the final path and block zip-slip
            target_path = (extract_to / member.filename).resolve()
            if not str(target_path).startswith(str(extract_to.resolve())):
                raise ValueError("Unsafe zip (path traversal detected).")

            target_path.parent.mkdir(parents=True, exist_ok=True)
            with z.open(member, "r") as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted_files.append(target_path)

    return extracted_files


def classify_file(p: Path) -> str:
    name = p.name.lower()
    ext = p.suffix.lower()

    # very simple v1 rules (you can improve later)
    if ext in [".dwg", ".dxf"]:
        return "drawings"
    if ext in [".xlsx", ".xls", ".csv"]:
        if "register" in name:
            return "registers"
        if "boq" in name or "bill" in name or "pricing" in name:
            return "boq"
        return "spreadsheets"
    if ext in [".docx", ".doc"]:
        if "form" in name or "tender" in name or "declaration" in name or "questionnaire" in name:
            return "forms"
        return "documents"
    if ext in [".pdf"]:
        if "prelim" in name or "spec" in name or "requirement" in name:
            return "specs"
        if "addendum" in name or "addenda" in name or "clarification" in name:
            return "addenda"
        return "pdfs"
    if ext in [".jpg", ".jpeg", ".png"]:
        return "images"
    return "other"


def guess_revision(filename: str) -> str | None:
    """
    Very rough revision guess from filename:
    - REV_P03, _P03, -P03, REV C01, etc.
    """
    s = filename.upper()
    m = re.search(r"(?:REV[\s_\-]*)?([PC]\d{2,3})\b", s)
    return m.group(1) if m else None


# ---------- UI ----------
@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Tender ZIP Analyser</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 40px; max-width: 900px; }
    .card { border: 1px solid #ddd; padding: 16px; border-radius: 8px; }
    pre { background: #f6f6f6; padding: 12px; border-radius: 8px; overflow-x: auto; }
    button { padding: 10px 14px; cursor: pointer; }
  </style>
</head>
<body>
  <h1>Tender ZIP Analyser (v1)</h1>
  <div class="card">
    <p>Upload a tender ZIP and get a quick breakdown.</p>
    <input id="zip" type="file" accept=".zip" />
    <button onclick="upload()">Analyse</button>
  </div>

  <h2>Result</h2>
  <pre id="out">No result yet.</pre>

  <script>
    async function upload() {
      const fileInput = document.getElementById('zip');
      if (!fileInput.files.length) {
        alert("Choose a .zip file first");
        return;
      }
      const fd = new FormData();
      fd.append("zip_file", fileInput.files[0]);

      document.getElementById('out').textContent = "Analysing...";

      const res = await fetch("/api/analyse", { method: "POST", body: fd });
      const text = await res.text();

      try {
        const json = JSON.parse(text);
        document.getElementById('out').textContent = JSON.stringify(json, null, 2);
      } catch {
        document.getElementById('out').textContent = text;
      }
    }
  </script>
</body>
</html>
"""


# ---------- API ----------
@app.post("/api/analyse")
async def analyse(zip_file: UploadFile = File(...)):
    if not zip_file.filename.lower().endswith(".zip"):
        return JSONResponse({"error": "Please upload a .zip file"}, status_code=400)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / "upload.zip"

        # save upload
        content = await zip_file.read()
        if len(content) > 300 * 1024 * 1024:
            return JSONResponse({"error": "Zip too large (limit 300MB for v1)."}, status_code=400)

        zip_path.write_bytes(content)

        # extract
        extract_dir = tmpdir_path / "unzipped"
        extract_dir.mkdir()

        try:
            files = safe_extract_zip(zip_path, extract_dir, max_files=5000)
        except Exception as e:
            return JSONResponse({"error": f"Could not extract zip: {str(e)}"}, status_code=400)

        # analyse
        by_type = defaultdict(list)
        ext_counter = Counter()

        drawings_by_number = defaultdict(list)

        for f in files:
            ext_counter[f.suffix.lower() or "(no_ext)"] += 1
            cat = classify_file(f)
            rel = str(f.relative_to(extract_dir))
            by_type[cat].append(rel)

            # basic drawing number + revision guess from filename
            # if it starts with something like A101, D-101 etc
            base = f.stem.upper()
            m = re.match(r"([A-Z]{1,3}[-_]?\d{2,4})", base)
            if m:
                num = m.group(1).replace("_", "-")
                rev = guess_revision(f.name)
                drawings_by_number[num].append({"file": rel, "rev": rev})

        # build response
        report = {
            "summary": {
                "total_files": len(files),
                "by_extension": dict(ext_counter.most_common()),
                "categories": {k: len(v) for k, v in by_type.items()},
                "boq_found": len(by_type.get("boq", [])) > 0,
                "register_found": len(by_type.get("registers", [])) > 0,
                "addenda_found": len(by_type.get("addenda", [])) > 0,
            },
            "top_hits": {
                "boq_files": by_type.get("boq", [])[:20],
                "register_files": by_type.get("registers", [])[:20],
                "addenda_files": by_type.get("addenda", [])[:20],
                "forms": by_type.get("forms", [])[:20],
                "specs": by_type.get("specs", [])[:20],
            },
            "drawings": {
                "count_guess": len(drawings_by_number),
                "items": drawings_by_number,  # dict of drawing number -> list of {file, rev}
            },
            "all_categories_sample": {k: v[:30] for k, v in by_type.items()},
        }

        return JSONResponse(report)from __future__ import annotations

import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from collections import Counter, defaultdict

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()


# ---------- helpers ----------
def safe_extract_zip(zip_path: Path, extract_to: Path, max_files: int = 5000) -> list[Path]:
    """
    Safely extract a zip to extract_to.
    - Prevents zip-slip (path traversal)
    - Limits total file count
    Returns list of extracted file paths.
    """
    extracted_files: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as z:
        members = z.infolist()
        if len(members) > max_files:
            raise ValueError(f"Zip has too many files ({len(members)}). Limit is {max_files}.")

        for member in members:
            # skip directories
            if member.is_dir():
                continue

            # Resolve the final path and block zip-slip
            target_path = (extract_to / member.filename).resolve()
            if not str(target_path).startswith(str(extract_to.resolve())):
                raise ValueError("Unsafe zip (path traversal detected).")

            target_path.parent.mkdir(parents=True, exist_ok=True)
            with z.open(member, "r") as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted_files.append(target_path)

    return extracted_files


def classify_file(p: Path) -> str:
    name = p.name.lower()
    ext = p.suffix.lower()

    # very simple v1 rules (you can improve later)
    if ext in [".dwg", ".dxf"]:
        return "drawings"
    if ext in [".xlsx", ".xls", ".csv"]:
        if "register" in name:
            return "registers"
        if "boq" in name or "bill" in name or "pricing" in name:
            return "boq"
        return "spreadsheets"
    if ext in [".docx", ".doc"]:
        if "form" in name or "tender" in name or "declaration" in name or "questionnaire" in name:
            return "forms"
        return "documents"
    if ext in [".pdf"]:
        if "prelim" in name or "spec" in name or "requirement" in name:
            return "specs"
        if "addendum" in name or "addenda" in name or "clarification" in name:
            return "addenda"
        return "pdfs"
    if ext in [".jpg", ".jpeg", ".png"]:
        return "images"
    return "other"


def guess_revision(filename: str) -> str | None:
    """
    Very rough revision guess from filename:
    - REV_P03, _P03, -P03, REV C01, etc.
    """
    s = filename.upper()
    m = re.search(r"(?:REV[\s_\-]*)?([PC]\d{2,3})\b", s)
    return m.group(1) if m else None


# ---------- UI ----------
@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Tender ZIP Analyser</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 40px; max-width: 900px; }
    .card { border: 1px solid #ddd; padding: 16px; border-radius: 8px; }
    pre { background: #f6f6f6; padding: 12px; border-radius: 8px; overflow-x: auto; }
    button { padding: 10px 14px; cursor: pointer; }
  </style>
</head>
<body>
  <h1>Tender ZIP Analyser (v1)</h1>
  <div class="card">
    <p>Upload a tender ZIP and get a quick breakdown.</p>
    <input id="zip" type="file" accept=".zip" />
    <button onclick="upload()">Analyse</button>
  </div>

  <h2>Result</h2>
  <pre id="out">No result yet.</pre>

  <script>
    async function upload() {
      const fileInput = document.getElementById('zip');
      if (!fileInput.files.length) {
        alert("Choose a .zip file first");
        return;
      }
      const fd = new FormData();
      fd.append("zip_file", fileInput.files[0]);

      document.getElementById('out').textContent = "Analysing...";

      const res = await fetch("/api/analyse", { method: "POST", body: fd });
      const text = await res.text();

      try {
        const json = JSON.parse(text);
        document.getElementById('out').textContent = JSON.stringify(json, null, 2);
      } catch {
        document.getElementById('out').textContent = text;
      }
    }
  </script>
</body>
</html>
"""


# ---------- API ----------
@app.post("/api/analyse")
async def analyse(zip_file: UploadFile = File(...)):
    if not zip_file.filename.lower().endswith(".zip"):
        return JSONResponse({"error": "Please upload a .zip file"}, status_code=400)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        zip_path = tmpdir_path / "upload.zip"

        # save upload
        content = await zip_file.read()
        if len(content) > 300 * 1024 * 1024:
            return JSONResponse({"error": "Zip too large (limit 300MB for v1)."}, status_code=400)

        zip_path.write_bytes(content)

        # extract
        extract_dir = tmpdir_path / "unzipped"
        extract_dir.mkdir()

        try:
            files = safe_extract_zip(zip_path, extract_dir, max_files=5000)
        except Exception as e:
            return JSONResponse({"error": f"Could not extract zip: {str(e)}"}, status_code=400)

        # analyse
        by_type = defaultdict(list)
        ext_counter = Counter()

        drawings_by_number = defaultdict(list)

        for f in files:
            ext_counter[f.suffix.lower() or "(no_ext)"] += 1
            cat = classify_file(f)
            rel = str(f.relative_to(extract_dir))
            by_type[cat].append(rel)

            # basic drawing number + revision guess from filename
            # if it starts with something like A101, D-101 etc
            base = f.stem.upper()
            m = re.match(r"([A-Z]{1,3}[-_]?\d{2,4})", base)
            if m:
                num = m.group(1).replace("_", "-")
                rev = guess_revision(f.name)
                drawings_by_number[num].append({"file": rel, "rev": rev})

        # build response
        report = {
            "summary": {
                "total_files": len(files),
                "by_extension": dict(ext_counter.most_common()),
                "categories": {k: len(v) for k, v in by_type.items()},
                "boq_found": len(by_type.get("boq", [])) > 0,
                "register_found": len(by_type.get("registers", [])) > 0,
                "addenda_found": len(by_type.get("addenda", [])) > 0,
            },
            "top_hits": {
                "boq_files": by_type.get("boq", [])[:20],
                "register_files": by_type.get("registers", [])[:20],
                "addenda_files": by_type.get("addenda", [])[:20],
                "forms": by_type.get("forms", [])[:20],
                "specs": by_type.get("specs", [])[:20],
            },
            "drawings": {
                "count_guess": len(drawings_by_number),
                "items": drawings_by_number,  # dict of drawing number -> list of {file, rev}
            },
            "all_categories_sample": {k: v[:30] for k, v in by_type.items()},
        }

        return JSONResponse(report)
