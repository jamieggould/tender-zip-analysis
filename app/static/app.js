let lastJson = null;

function setStatus(text) {
  document.getElementById("status").textContent = text;
}

/**
 * Backwards-compatible formatter:
 * - If your new index.html renderer exists (window.renderTenderReport), use it.
 * - Else fall back to the original text summary.
 *
 * NOTE: We keep the old behaviour intact so nothing breaks if you revert the HTML.
 */
function formatReport(data) {
  // Prefer the new “1–2 page briefing” renderer if present
  if (window.renderTenderReport && typeof window.renderTenderReport === "function") {
    // The renderer writes to #out itself, but we return a string too as a safe fallback.
    // (If you want, you can ignore the return value.)
    try {
      window.renderTenderReport(data);
      return ""; // renderer handled output
    } catch (e) {
      // If renderer fails for any reason, fall back to legacy output
      console.warn("renderTenderReport failed, falling back:", e);
    }
  }

  // --- Legacy output (your original formatter), kept with minimal tweaks ---
  if (!data || !data.summary) return JSON.stringify(data, null, 2);

  const s = data.summary;
  const sections = data.sections || {};
  const briefing = data.briefing || {}; // new backend field (safe if missing)

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
    sections.boq.slice(0, 5).forEach(f => {
      out += `• ${f.file}\n`;
    });
    out += "\n";
  }

  if (sections.registers && sections.registers.length) {
    out += "REGISTERS\n";
    sections.registers.slice(0, 5).forEach(f => {
      out += `• ${f.file}\n`;
    });
    out += "\n";
  }

  if (sections.drawings && sections.drawings.length) {
    out += "DRAWINGS\n";
    sections.drawings.slice(0, 10).forEach(d => {
      const ex = d.extracted || {};
      const num = ex.drawing_number_guess || "";
      const rev = ex.revision_guess || "";
      out += `• ${d.file} ${num ? `(${num}` : ""}${rev ? ` Rev ${rev}` : ""}${num ? ")" : ""}\n`;
    });
    out += "\n";
  }

  // NEW (minimal): If briefing exists, show the important bits before keyword spam
  // This keeps your app “useful” even without the HTML renderer.
  if (briefing && Object.keys(briefing).length) {
    const ev = (items, title, max = 4) => {
      if (!items || !items.length) return;
      out += `${title}\n`;
      items.slice(0, max).forEach(it => {
        if (it.match) out += `• ${it.match}\n`;
        if (it.evidence) out += `  - ${it.evidence}\n`;
      });
      out += "\n";
    };

    if (briefing.date_candidates && briefing.date_candidates.length) {
      out += "DATES FOUND (CANDIDATES)\n";
      briefing.date_candidates.slice(0, 12).forEach(d => (out += `• ${d}\n`));
      out += "\n";
    }

    ev(briefing.tender_return_candidates, "TENDER RETURN / DEADLINE (EVIDENCE)");
    ev(briefing.submission_candidates, "SUBMISSION ROUTE (EVIDENCE)");
    ev(briefing.programme_candidates, "PROGRAMME / DURATION (EVIDENCE)");
    ev(briefing.working_hours_candidates, "WORKING HOURS (EVIDENCE)");
    ev(briefing.retention_candidates, "RETENTION (EVIDENCE)");
    ev(briefing.liquidated_damages_candidates, "LIQUIDATED DAMAGES / LADs (EVIDENCE)");
    ev(briefing.insurance_candidates, "INSURANCE LEVELS (EVIDENCE)");
    ev(briefing.accreditations_candidates, "ACCREDITATIONS / COMPLIANCE (EVIDENCE)");

    const rb = briefing.risk_buckets || {};
    const keys = Object.keys(rb);
    if (keys.length) {
      out += "KEY RISKS / CONSTRAINTS\n";
      keys.slice(0, 10).forEach(k => {
        const item = rb[k] || {};
        const mentions = item.mentions || 0;
        out += `• ${k} — ${mentions} mention${mentions === 1 ? "" : "s"}\n`;
        (item.evidence || []).slice(0, 1).forEach(e => (out += `  - ${e}\n`));
      });
      out += "\n";
    }

    const strict = briefing.requirements_strict || [];
    const loose = briefing.requirements_loose || [];
    if (strict.length) {
      out += "MANDATORY REQUIREMENTS (STRICT)\n";
      strict.slice(0, 12).forEach(r => (out += `• ${r}\n`));
      if (strict.length > 12) out += `• … (${strict.length - 12} more)\n`;
      out += "\n";
    }
    if (loose.length) {
      out += "OTHER SUBMISSION REQUESTS (LOOSE)\n";
      loose.slice(0, 12).forEach(r => (out += `• ${r}\n`));
      if (loose.length > 12) out += `• … (${loose.length - 12} more)\n`;
      out += "\n";
    }
  }

  // Keep your old “keywords” section as a fallback (but only if briefing wasn't present)
  if ((!briefing || !Object.keys(briefing).length) && sections.pdfs) {
    let keywords = {};

    sections.pdfs.forEach(p => {
      const hits = p.extracted?.keyword_hits || {};
      for (const [k, v] of Object.entries(hits)) {
        keywords[k] = (keywords[k] || 0) + v;
      }
    });

    const sorted = Object.entries(keywords)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);

    if (sorted.length) {
      out += "KEY FINDINGS\n";
      sorted.forEach(([k, v]) => {
        out += `• ${k} mentioned (${v} times)\n`;
      });
      out += "\n";
    }
  }

  if ((!briefing || !Object.keys(briefing).length) && sections.pdfs) {
    let dates = new Set();

    sections.pdfs.forEach(p => {
      (p.extracted?.date_candidates || []).forEach(d => {
        dates.add(d);
      });
    });

    if (dates.size) {
      out += "DATES FOUND\n";
      [...dates].slice(0, 10).forEach(d => {
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
    const formatted = formatReport(json);

    // If the HTML renderer handled output, formatted will be ""
    if (formatted) out.textContent = formatted;

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
