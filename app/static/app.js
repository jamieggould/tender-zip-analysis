let lastJson = null;

function setStatus(text) {
  document.getElementById("status").textContent = text;
}

function formatReport(data) {
  if (!data.summary) return JSON.stringify(data, null, 2);

  const s = data.summary;
  const sections = data.sections || {};

  let out = "";

  out += "TENDER PACK SUMMARY\n";
  out += "--------------------\n\n";

  out += "FILES ANALYSED\n";
  out += `• Total files: ${s.total_files_scanned || s.total_files || 0}\n\n`;

  if (s.by_category) {
    out += "DOCUMENT TYPES\n";
    for (const [k, v] of Object.entries(s.by_category)) {
      out += `• ${k}: ${v}\n`;
    }
    out += "\n";
  }

  if (sections.boq && sections.boq.length) {
    out += "BOQ FILES\n";
    sections.boq.slice(0,5).forEach(f=>{
      out += `• ${f.file}\n`;
    });
    out += "\n";
  }

  if (sections.registers && sections.registers.length) {
    out += "REGISTERS\n";
    sections.registers.slice(0,5).forEach(f=>{
      out += `• ${f.file}\n`;
    });
    out += "\n";
  }

  if (sections.drawings && sections.drawings.length) {
    out += "DRAWINGS\n";
    sections.drawings.slice(0,10).forEach(d=>{
      const ex = d.extracted || {};
      const num = ex.drawing_number_guess || "";
      const rev = ex.revision_guess || "";
      out += `• ${d.file} ${num ? `(${num}` : ""}${rev ? ` Rev ${rev}` : ""}${num ? ")" : ""}\n`;
    });
    out += "\n";
  }

  if (sections.pdfs) {
    let keywords = {};

    sections.pdfs.forEach(p=>{
      const hits = p.extracted?.keyword_hits || {};
      for (const [k,v] of Object.entries(hits)) {
        keywords[k] = (keywords[k] || 0) + v;
      }
    });

    const sorted = Object.entries(keywords)
      .sort((a,b)=>b[1]-a[1])
      .slice(0,10);

    if (sorted.length) {
      out += "KEY FINDINGS\n";
      sorted.forEach(([k,v])=>{
        out += `• ${k} mentioned (${v} times)\n`;
      });
      out += "\n";
    }
  }

  if (sections.pdfs) {
    let dates = new Set();

    sections.pdfs.forEach(p=>{
      (p.extracted?.date_candidates || []).forEach(d=>{
        dates.add(d);
      });
    });

    if (dates.size) {
      out += "DATES FOUND\n";
      [...dates].slice(0,10).forEach(d=>{
        out += `• ${d}\n`;
      });
      out += "\n";
    }
  }

  return out;
}

async function uploadZip() {
  const fileInput = document.getElementById("zip");
  const out = document.getElementById("out");
  const btn = document.getElementById("btn");
  const downloadBtn = document.getElementById("downloadBtn");

  if (!fileInput.files.length) {
    alert("Choose files first");
    return;
  }

  const fd = new FormData();

  for (const f of fileInput.files) {
    fd.append("zip_file", f);
  }

  btn.disabled = true;
  downloadBtn.disabled = true;
  lastJson = null;

  setStatus("Uploading…");
  out.textContent = "Analysing tender pack…";

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

    // FORMAT RESULT INTO HUMAN TEXT
    out.textContent = formatReport(json);

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

  const blob = new Blob(
    [JSON.stringify(lastJson, null, 2)],
    { type: "application/json" }
  );

  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "tender_report.json";
  a.click();
  URL.revokeObjectURL(url);
}
