const state = {
  leads: [],
  runs: [],
  health: {},
  currentTab: "approved",
  filters: {
    search: "",
    source: "all",
    minScore: 0,
    minConfidence: 0,
    status: "all",
    category: "all",
    urgency: "all",
    minReasons: 0,
    enrichedOnly: false,
    hasLink: false,
    freelancerOnly: false,
    hiringOnly: false,
    helpOnly: false,
    sort: "verdict_priority",
  },
};

const ui = {
  runBtn: document.getElementById("runBtn"),
  refreshBtn: document.getElementById("refreshBtn"),
  meta: document.getElementById("meta"),
  countLeads: document.getElementById("countLeads"),
  countRuns: document.getElementById("countRuns"),
  topScore: document.getElementById("topScore"),
  filteredIn: document.getElementById("filteredIn"),
  approvedCount: document.getElementById("approvedCount"),
  rejectedCount: document.getElementById("rejectedCount"),
  skippedExisting: document.getElementById("skippedExisting"),
  autoRunStatus: document.getElementById("autoRunStatus"),
  latestRunTime: document.getElementById("latestRunTime"),
  ingestionCount: document.getElementById("ingestionCount"),
  rankedCount: document.getElementById("rankedCount"),
  enrichedCount: document.getElementById("enrichedCount"),
  rejectionReasons: document.getElementById("rejectionReasons"),
  approvedMeta: document.getElementById("approvedMeta"),
  rejectedMeta: document.getElementById("rejectedMeta"),
  searchInput: document.getElementById("searchInput"),
  sourceSelect: document.getElementById("sourceSelect"),
  minScoreInput: document.getElementById("minScoreInput"),
  minConfidenceInput: document.getElementById("minConfidenceInput"),
  statusSelect: document.getElementById("statusSelect"),
  categorySelect: document.getElementById("categorySelect"),
  urgencySelect: document.getElementById("urgencySelect"),
  minReasonsInput: document.getElementById("minReasonsInput"),
  enrichedOnlyInput: document.getElementById("enrichedOnlyInput"),
  hasLinkInput: document.getElementById("hasLinkInput"),
  freelancerOnlyInput: document.getElementById("freelancerOnlyInput"),
  hiringOnlyInput: document.getElementById("hiringOnlyInput"),
  helpOnlyInput: document.getElementById("helpOnlyInput"),
  sortSelect: document.getElementById("sortSelect"),
  presetFreelanceBtn: document.getElementById("presetFreelanceBtn"),
  presetHelpBtn: document.getElementById("presetHelpBtn"),
  presetHighSignalBtn: document.getElementById("presetHighSignalBtn"),
  filterResultMeta: document.getElementById("filterResultMeta"),
  clearFiltersBtn: document.getElementById("clearFiltersBtn"),
  approvedLeads: document.getElementById("approvedLeads"),
  rejectedLeads: document.getElementById("rejectedLeads"),
  healthList: document.getElementById("healthList"),
};

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} on ${url}`);
  }
  return response.json();
}

function setMeta(message, isError = false) {
  ui.meta.textContent = message;
  ui.meta.classList.toggle("error", isError);
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatCursor(value, maxLen = 28) {
  const text = String(value || "none");
  if (text.length <= maxLen) {
    return { short: text, full: text, truncated: false };
  }
  const head = text.slice(0, 12);
  const tail = text.slice(-8);
  return {
    short: `${head}...${tail}`,
    full: text,
    truncated: true,
  };
}

function getLeadUrl(item) {
  const raw = item?.source_url || item?.url || item?.link || "";
  if (!raw) {
    return "";
  }
  try {
    const parsed = new URL(String(raw));
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.href;
    }
  } catch {
    return "";
  }
  return "";
}

function renderGeminiVerdict(item) {
  const status = String(item.status || "").toLowerCase();
  const enrichment = item.enrichment && typeof item.enrichment === "object" ? item.enrichment : null;
  if (status === "failed") {
    const failureReason = enrichment?.failure_reason ? escapeHtml(String(enrichment.failure_reason)) : "Gemini did not return this lead after retries.";
    return `
      <details class="verdict" open>
        <summary>Gemini verdict: failed</summary>
        <p class="verdict-empty">${failureReason}</p>
      </details>
    `;
  }

  if (!enrichment || !Object.keys(enrichment).length) {
    return `
      <details class="verdict">
        <summary>Gemini verdict: unavailable</summary>
        <p class="verdict-empty">No enrichment payload for this lead.</p>
      </details>
    `;
  }

  const verdict = enrichment.recommend_as_lead;
  const verdictLabel = verdict === true ? "approved" : verdict === false ? "rejected" : "unknown";
  const payload = escapeHtml(JSON.stringify(enrichment, null, 2));

  return `
    <details class="verdict">
      <summary>Gemini verdict: ${verdictLabel}</summary>
      <pre class="verdict-json">${payload}</pre>
    </details>
  `;
}

function renderStats() {
  const leads = state.leads || [];
  const approved = getVisibleLeads(true);
  const rejected = getVisibleLeads(false);
  const runs = state.runs || [];
  const latestRun = runs[0] || null;
  const topScore = leads.length ? Math.max(...leads.map((x) => Number(x.score_total || 0))) : 0;
  const health = state.health || {};

  ui.countLeads.textContent = String(leads.length);
  ui.countRuns.textContent = String(runs.length);
  ui.topScore.textContent = topScore.toFixed(3);
  ui.filteredIn.textContent = latestRun ? String(latestRun.summary.filtered_in || 0) : "0";
  ui.approvedCount.textContent = String(approved.length);
  ui.rejectedCount.textContent = String(rejected.length);
  ui.skippedExisting.textContent = latestRun ? String(latestRun.summary.skipped_existing || 0) : "0";
  ui.filteredIn.title = `Approved: ${approved.length} | Rejected: ${rejected.length}`;
  ui.autoRunStatus.textContent = health.scheduler_enabled
    ? `${Number(health.scheduler_interval_minutes || 0)}m`
    : "off";

  if (ui.approvedMeta) {
    ui.approvedMeta.textContent = `Approved: ${approved.length}`;
  }
  if (ui.rejectedMeta) {
    ui.rejectedMeta.textContent = `Rejected: ${rejected.length}`;
  }
}

function renderRunSnapshot() {
  const latestRun = state.runs[0] || null;
  if (!latestRun) {
    ui.latestRunTime.textContent = "-";
    ui.ingestionCount.textContent = "0";
    ui.rankedCount.textContent = "0";
    ui.enrichedCount.textContent = "0";
    ui.rejectionReasons.textContent = "No runs yet.";
    return;
  }

  const summary = latestRun.summary || {};
  const ingestion = summary.ingestion || {};
  const createdAt = latestRun.created_at ? new Date(latestRun.created_at) : null;
  ui.latestRunTime.textContent = createdAt && !Number.isNaN(createdAt.getTime())
    ? `${createdAt.toLocaleDateString()} ${createdAt.toLocaleTimeString()}`
    : "-";
  ui.ingestionCount.textContent = String(Number(ingestion.leads_count || 0));
  ui.rankedCount.textContent = String(Number(summary.ranked_count || 0));
  ui.enrichedCount.textContent = String(Number(summary.enriched_count || 0));

  const reasons = Array.isArray(summary.top_rejection_reasons) ? summary.top_rejection_reasons : [];
  ui.rejectionReasons.textContent = reasons.length ? reasons.join(" | ") : "No rejections in latest run.";
}

function renderHealth() {
  const latestRun = state.runs[0];
  const perSource = latestRun?.summary?.ingestion?.per_source || [];

  if (!perSource.length) {
    ui.healthList.innerHTML = '<p class="empty">No run data yet. Execute pipeline to see source health.</p>';
    return;
  }

  ui.healthList.innerHTML = perSource
    .map((item) => {
      const hasError = Boolean(item.error);
      const status = hasError ? "error" : item.exhausted ? "ok" : "active";
      const statusLabel = hasError ? "Error" : item.exhausted ? "Complete" : "Partial";
      const cursor = formatCursor(item.next_cursor);
      const cursorHtml = cursor.truncated
        ? `next_cursor=<span title="${escapeHtml(cursor.full)}">${escapeHtml(cursor.short)}</span>`
        : `next_cursor=${escapeHtml(cursor.short)}`;
      return `
        <article class="health-item">
          <div class="health-head">
            <div class="source-name">${escapeHtml(item.source || "unknown")}</div>
            <span class="status-chip ${hasError ? "fail" : ""}">${statusLabel}</span>
          </div>
          <div class="health-sub">
            fetched=${Number(item.fetched_items || 0)} | normalized=${Number(item.normalized_items || 0)}
          </div>
          <div class="health-sub">
            ${hasError ? escapeHtml(item.error) : cursorHtml}
          </div>
        </article>
      `;
    })
    .join("");
}

function updateSourceOptions() {
  const sources = [...new Set(state.leads.map((lead) => lead.source).filter(Boolean))].sort();
  const current = ui.sourceSelect.value || "all";
  ui.sourceSelect.innerHTML = '<option value="all">All sources</option>';
  for (const source of sources) {
    const option = document.createElement("option");
    option.value = source;
    option.textContent = source;
    ui.sourceSelect.appendChild(option);
  }
  ui.sourceSelect.value = sources.includes(current) ? current : "all";
}

function updateFacetOptions() {
  const categories = [...new Set(
    state.leads
      .map((lead) => String(lead?.enrichment?.category || "").trim().toLowerCase())
      .filter(Boolean)
  )].sort();
  const urgencies = [...new Set(
    state.leads
      .map((lead) => String(lead?.enrichment?.urgency || "").trim().toLowerCase())
      .filter(Boolean)
  )].sort();

  const currentCategory = ui.categorySelect.value || "all";
  const currentUrgency = ui.urgencySelect.value || "all";

  ui.categorySelect.innerHTML = '<option value="all">All categories</option>';
  for (const category of categories) {
    const option = document.createElement("option");
    option.value = category;
    option.textContent = category;
    ui.categorySelect.appendChild(option);
  }

  ui.urgencySelect.innerHTML = '<option value="all">All urgency</option>';
  for (const urgency of urgencies) {
    const option = document.createElement("option");
    option.value = urgency;
    option.textContent = urgency;
    ui.urgencySelect.appendChild(option);
  }

  ui.categorySelect.value = categories.includes(currentCategory) ? currentCategory : "all";
  ui.urgencySelect.value = urgencies.includes(currentUrgency) ? currentUrgency : "all";
}

function getVisibleLeads(isApproved = null) {
  const filters = state.filters;
  const query = filters.search.trim().toLowerCase();

  function verdictPriority(lead) {
    const enrichment = lead.enrichment && typeof lead.enrichment === "object" ? lead.enrichment : {};
    const isRejected = enrichment.recommend_as_lead === false;
    if (isRejected) {
      return 2;
    }
    if (enrichment.is_freelancer_request === true) {
      return 0;
    }
    if (enrichment.is_help_request === true) {
      return 1;
    }
    return 1;
  }

  function confidenceValue(lead) {
    const enrichment = lead.enrichment && typeof lead.enrichment === "object" ? lead.enrichment : {};
    return Number(enrichment.confidence || 0);
  }

  return [...state.leads]
    .filter((lead) => {
      const enrichment = lead.enrichment && typeof lead.enrichment === "object" ? lead.enrichment : {};
      
      if (isApproved !== null) {
        const approved = enrichment.recommend_as_lead === true;
        if (approved !== isApproved) {
          return false;
        }
      }
      
      if (filters.source !== "all" && lead.source !== filters.source) {
        return false;
      }

      if (filters.status !== "all" && String(lead.status || "").toLowerCase() !== filters.status) {
        return false;
      }

      const category = String(enrichment.category || "").toLowerCase();
      if (filters.category !== "all" && category !== filters.category) {
        return false;
      }

      const urgency = String(enrichment.urgency || "").toLowerCase();
      if (filters.urgency !== "all" && urgency !== filters.urgency) {
        return false;
      }
      
      if (Number(lead.score_total || 0) < Number(filters.minScore || 0)) {
        return false;
      }
      
      if (Number(confidenceValue(lead) || 0) < Number(filters.minConfidence || 0)) {
        return false;
      }

      if (Number((lead.reasons || []).length) < Number(filters.minReasons || 0)) {
        return false;
      }

      if (filters.enrichedOnly && (!enrichment || !Object.keys(enrichment).length)) {
        return false;
      }

      if (filters.hasLink && !getLeadUrl(lead)) {
        return false;
      }
      
      if (filters.freelancerOnly && enrichment.is_freelancer_request !== true) {
        return false;
      }
      
      if (filters.hiringOnly && enrichment.is_hiring_request !== true) {
        return false;
      }
      
      if (filters.helpOnly && enrichment.is_help_request !== true) {
        return false;
      }
      
      if (!query) {
        return true;
      }
      
      const haystack = [lead.title, lead.summary, lead.source, ...(lead.reasons || [])]
        .join(" ")
        .toLowerCase();
      return haystack.includes(query);
    })
    .sort((a, b) => {
      const sortMode = filters.sort;
      if (sortMode === "verdict_priority") {
        const groupDiff = verdictPriority(a) - verdictPriority(b);
        if (groupDiff !== 0) {
          return groupDiff;
        }
        const confidenceDiff = confidenceValue(b) - confidenceValue(a);
        if (confidenceDiff !== 0) {
          return confidenceDiff;
        }
        return Number(b.score_total || 0) - Number(a.score_total || 0);
      }
      if (sortMode === "confidence_desc") {
        return confidenceValue(b) - confidenceValue(a);
      }
      if (sortMode === "score_asc") {
        return Number(a.score_total || 0) - Number(b.score_total || 0);
      }
      if (sortMode === "recent") {
        return String(b.lead_id || "").localeCompare(String(a.lead_id || ""));
      }
      return Number(b.score_total || 0) - Number(a.score_total || 0);
    });
}

function renderLeads() {
  const approved = getVisibleLeads(true);
  const rejected = getVisibleLeads(false);
  if (ui.filterResultMeta) {
    const total = state.leads.length;
    const shown = approved.length + rejected.length;
    ui.filterResultMeta.textContent = `Showing ${shown} of ${total} leads`;
  }
  
  function renderLeadList(items, emptyMsg = "No leads in this section.") {
    if (!items.length) {
      return `<article class="item"><p class="empty">${emptyMsg}</p></article>`;
    }

    return items
      .map((item) => {
        const leadUrl = getLeadUrl(item);
        const title = escapeHtml(item.title || "Untitled");
        const source = escapeHtml(item.source || "unknown");
        const score = Number(item.score_total || 0).toFixed(3);
        const statusValue = escapeHtml(String(item.status || "new"));
        const statusClass = String(item.status || "").toLowerCase() === "failed" ? " fail" : "";
        const titleHtml = leadUrl
          ? `<a class="lead-link" href="${escapeHtml(leadUrl)}" target="_blank" rel="noopener noreferrer">${title}</a>`
          : title;
        const sourceHtml = leadUrl
          ? `<a class="pill-link" href="${escapeHtml(leadUrl)}" target="_blank" rel="noopener noreferrer">${source} • ${score}</a>`
          : `${source} • ${score}`;
        const reasons = (item.reasons || [])
          .slice(0, 5)
          .map((reason) => `<span class="chip">${escapeHtml(reason)}</span>`)
          .join("");
        const enrichment = item.enrichment && typeof item.enrichment === "object" ? item.enrichment : {};
        const confidence = Number(enrichment.confidence || 0).toFixed(2);
        const category = enrichment.category ? String(enrichment.category) : "unknown";
        const urgency = enrichment.urgency ? String(enrichment.urgency) : "unknown";

        return `
          <article class="item">
            <div class="row">
              <h3>${titleHtml}</h3>
              <div class="row-badges">
                <span class="status-chip${statusClass}">${statusValue}</span>
                <span class="pill">${sourceHtml}</span>
              </div>
            </div>
            <p class="summary">${escapeHtml(item.summary || "")}</p>
            <div class="lead-meta">
              <span class="meta-chip">confidence: ${escapeHtml(confidence)}</span>
              <span class="meta-chip">category: ${escapeHtml(category)}</span>
              <span class="meta-chip">urgency: ${escapeHtml(urgency)}</span>
            </div>
            <div class="reasons">${reasons}</div>
            ${renderGeminiVerdict(item)}
          </article>
        `;
      })
      .join("");
  }

  if (!state.leads.length) {
    const emptyMsg = '<article class="item"><p class="empty">No leads yet. Run pipeline to fetch candidates.</p></article>';
    ui.approvedLeads.innerHTML = emptyMsg;
    ui.rejectedLeads.innerHTML = emptyMsg;
    return;
  }

  ui.approvedLeads.innerHTML = renderLeadList(approved, "No approved leads match the filters.");
  ui.rejectedLeads.innerHTML = renderLeadList(rejected, "No rejected leads match the filters.");
}

async function refreshAll() {
  try {
    setMeta("Loading dashboard state...");
    const [leads, runs, health] = await Promise.all([fetchJson("/api/leads"), fetchJson("/api/runs"), fetchJson("/health")]);
    state.leads = leads.items || [];
    state.runs = runs.items || [];
    state.health = health;
    updateSourceOptions();
    updateFacetOptions();
    renderStats();
    renderRunSnapshot();
    renderHealth();
    renderLeads();
    const latestRun = state.runs[0];
    const rejectionReasons = latestRun?.summary?.top_rejection_reasons || [];
    if (latestRun && Number(latestRun.summary.filtered_in || 0) === 0 && rejectionReasons.length) {
      setMeta(`No leads passed filter. Top reasons: ${rejectionReasons.join(" | ")}`);
    } else {
      setMeta("Loaded.");
    }
  } catch (error) {
    setMeta(`Error: ${error.message}`, true);
  }
}

function syncFiltersFromUi() {
  state.filters.search = ui.searchInput.value || "";
  state.filters.source = ui.sourceSelect.value || "all";
  state.filters.minScore = Number.parseFloat(ui.minScoreInput.value || "0") || 0;
  state.filters.minConfidence = Number.parseFloat(ui.minConfidenceInput.value || "0") || 0;
  state.filters.status = (ui.statusSelect.value || "all").toLowerCase();
  state.filters.category = (ui.categorySelect.value || "all").toLowerCase();
  state.filters.urgency = (ui.urgencySelect.value || "all").toLowerCase();
  state.filters.minReasons = Number.parseInt(ui.minReasonsInput.value || "0", 10) || 0;
  state.filters.enrichedOnly = ui.enrichedOnlyInput.checked || false;
  state.filters.hasLink = ui.hasLinkInput.checked || false;
  state.filters.freelancerOnly = ui.freelancerOnlyInput.checked || false;
  state.filters.hiringOnly = ui.hiringOnlyInput.checked || false;
  state.filters.helpOnly = ui.helpOnlyInput.checked || false;
  state.filters.sort = ui.sortSelect.value || "verdict_priority";
}

function applyFilters() {
  syncFiltersFromUi();
  renderStats();
  renderLeads();
}

function clearFilters() {
  ui.searchInput.value = "";
  ui.sourceSelect.value = "all";
  ui.minScoreInput.value = "0.00";
  ui.minConfidenceInput.value = "0.00";
  ui.statusSelect.value = "all";
  ui.categorySelect.value = "all";
  ui.urgencySelect.value = "all";
  ui.minReasonsInput.value = "0";
  ui.enrichedOnlyInput.checked = false;
  ui.hasLinkInput.checked = false;
  ui.freelancerOnlyInput.checked = false;
  ui.hiringOnlyInput.checked = false;
  ui.helpOnlyInput.checked = false;
  ui.sortSelect.value = "verdict_priority";
  applyFilters();
}

function applyPreset(name) {
  clearFilters();
  if (name === "freelance") {
    ui.freelancerOnlyInput.checked = true;
    ui.minConfidenceInput.value = "0.45";
    ui.sortSelect.value = "confidence_desc";
  }
  if (name === "help") {
    ui.helpOnlyInput.checked = true;
    ui.minConfidenceInput.value = "0.40";
    ui.sortSelect.value = "confidence_desc";
  }
  if (name === "high_signal") {
    ui.enrichedOnlyInput.checked = true;
    ui.minScoreInput.value = "0.35";
    ui.minConfidenceInput.value = "0.70";
    ui.minReasonsInput.value = "2";
    ui.sortSelect.value = "score_desc";
  }
  applyFilters();
}

function setupTabSwitching() {
  const tabBtns = document.querySelectorAll(".tab-btn");
  const tabContents = document.querySelectorAll(".tab-content");
  
  tabBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      const tabId = btn.getAttribute("data-tab");
      state.currentTab = tabId;
      
      tabBtns.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      
      tabContents.forEach((content) => content.classList.remove("active"));
      const activeContent = document.getElementById(`${tabId}Leads`);
      if (activeContent) {
        activeContent.classList.add("active");
      }
    });
  });
}

async function runPipeline() {
  try {
    ui.runBtn.disabled = true;
    ui.runBtn.textContent = "Running...";
    setMeta("Executing pipeline...");
    const result = await fetchJson("/api/run", { method: "POST" });
    await refreshAll();
    const perSource = result?.summary?.ingestion?.per_source || [];
    const sourceErrors = perSource.filter((item) => Boolean(item.error));
    if (sourceErrors.length) {
      const details = sourceErrors
        .slice(0, 3)
        .map((item) => `${item.source}: ${item.error}`)
        .join(" | ");
      setMeta(`Run finished with source errors (${sourceErrors.length}). ${details}`, true);
      return;
    }
    setMeta(`Run completed. Stored leads: ${result.stored_leads}.`);
  } catch (error) {
    setMeta(`Run failed: ${error.message}`, true);
  } finally {
    ui.runBtn.disabled = false;
    ui.runBtn.textContent = "Run Pipeline";
  }
}

ui.runBtn.addEventListener("click", runPipeline);
ui.refreshBtn.addEventListener("click", refreshAll);
ui.searchInput.addEventListener("input", applyFilters);
ui.sourceSelect.addEventListener("change", applyFilters);
ui.minScoreInput.addEventListener("input", applyFilters);
ui.minConfidenceInput.addEventListener("input", applyFilters);
ui.statusSelect.addEventListener("change", applyFilters);
ui.categorySelect.addEventListener("change", applyFilters);
ui.urgencySelect.addEventListener("change", applyFilters);
ui.minReasonsInput.addEventListener("input", applyFilters);
ui.enrichedOnlyInput.addEventListener("change", applyFilters);
ui.hasLinkInput.addEventListener("change", applyFilters);
ui.freelancerOnlyInput.addEventListener("change", applyFilters);
ui.hiringOnlyInput.addEventListener("change", applyFilters);
ui.helpOnlyInput.addEventListener("change", applyFilters);
ui.sortSelect.addEventListener("change", applyFilters);
ui.presetFreelanceBtn.addEventListener("click", () => applyPreset("freelance"));
ui.presetHelpBtn.addEventListener("click", () => applyPreset("help"));
ui.presetHighSignalBtn.addEventListener("click", () => applyPreset("high_signal"));
ui.clearFiltersBtn.addEventListener("click", clearFilters);
setupTabSwitching();
refreshAll();
