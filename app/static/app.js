let lastJson = null;

function setStatus(text) {
  document.getElementById("status").textContent = text;
}

async function uploadZip() {
  const fileInput = document.getElementById("zip");
  const out = document.getElementById("out");
  const btn = document.getElementById("btn");
  const downloadBtn = document.getElementById("downloadBtn");

  if (!fileInput.files.length) {
    alert("Choose a .zip file first");
    return;
  }

  const f = fileInput.files[0];
  const fd = new FormData();
  fd.append("zip_file", f);

  btn.disabled = true;
  downloadBtn.disabled = true;
  lastJson = null;

  setStatus("Uploading…");
  out.textContent = "Working…";

  try {
    const res = await fetch("/api/analyse", { method: "POST", body: fd });
    const text = await res.text();

    if (!res.ok) {
      setStatus("Error");
      out.textContent = text;
      return;
    }

    const json = JSON.parse(text);
    lastJson = json;
    out.textContent = JSON.stringify(json, null, 2);
    setStatus("Done");
    downloadBtn.disabled = false;
  } catch (e) {
    setStatus("Error");
    out.textContent = String(e);
  } finally {
    btn.disabled = false;
  }
}

function downloadJson() {
  if (!lastJson) return;
  const blob = new Blob([JSON.stringify(lastJson, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "tender_report.json";
  a.click();
  URL.revokeObjectURL(url);
}
