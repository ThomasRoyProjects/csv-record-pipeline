const tabNormalize = document.getElementById("tabNormalize");
const tabMatch = document.getElementById("tabMatch");
const tabUtilities = document.getElementById("tabUtilities");
const normalizeTab = document.getElementById("normalizeTab");
const matchTab = document.getElementById("matchTab");

const workflowSelect = document.getElementById("workflowSelect");
const workflowMeta = document.getElementById("workflowMeta");
const openMatchHelp = document.getElementById("openMatchHelp");
const closeMatchHelp = document.getElementById("closeMatchHelp");
const matchHelpModal = document.getElementById("matchHelpModal");
const dangerModal = document.getElementById("dangerModal");
const closeDangerModal = document.getElementById("closeDangerModal");
const cancelDangerModal = document.getElementById("cancelDangerModal");
const confirmDangerModal = document.getElementById("confirmDangerModal");
const dangerModalMessage = document.getElementById("dangerModalMessage");
const customStagePanel = document.getElementById("customStagePanel");
const customStageList = document.getElementById("customStageList");
const customStageDescription = document.getElementById("customStageDescription");
const stageLibrary = document.getElementById("stageLibrary");
const customStageViewport = document.getElementById("customStageViewport");
const customStageCanvas = document.getElementById("customStageCanvas");
const customStageSvg = document.getElementById("customStageSvg");
const builderTemplateSelect = document.getElementById("builderTemplateSelect");
const applyBuilderTemplate = document.getElementById("applyBuilderTemplate");
const customStageZoom = document.getElementById("customStageZoom");
const zoomOutStages = document.getElementById("zoomOutStages");
const zoomInStages = document.getElementById("zoomInStages");
const jobSpec = document.getElementById("jobSpec");
const resultBox = document.getElementById("resultBox");

const primaryFile = document.getElementById("primaryFile");
const primaryFileStatus = document.getElementById("primaryFileStatus");
const referenceFile = document.getElementById("referenceFile");
const referenceFileStatus = document.getElementById("referenceFileStatus");
const outputPath = document.getElementById("outputPath");
const strictMatchMode = document.getElementById("strictMatchMode");
const confidentThreshold = document.getElementById("confidentThreshold");
const possibleThreshold = document.getElementById("possibleThreshold");
const reviewThreshold = document.getElementById("reviewThreshold");
const presetConservative = document.getElementById("presetConservative");
const presetBalanced = document.getElementById("presetBalanced");
const presetAggressive = document.getElementById("presetAggressive");

const primaryHeaders = document.getElementById("primaryHeaders");
const referenceHeaders = document.getElementById("referenceHeaders");
const primaryHeaderGroups = document.getElementById("primaryHeaderGroups");
const referenceHeaderGroups = document.getElementById("referenceHeaderGroups");
const primaryMappingForm = document.getElementById("primaryMappingForm");
const referenceMappingForm = document.getElementById("referenceMappingForm");
const summaryCards = document.getElementById("summaryCards");
const summaryOutputs = document.getElementById("summaryOutputs");
const summaryReasons = document.getElementById("summaryReasons");
const reviewPreview = document.getElementById("reviewPreview");
const unmatchedPreview = document.getElementById("unmatchedPreview");
const presetSelect = document.getElementById("presetSelect");
const presetName = document.getElementById("presetName");
const presetHelp = document.getElementById("presetHelp");
const loadPresetButton = document.getElementById("loadPreset");
const workflowErrors = document.getElementById("workflowErrors");
const workflowDescription = document.getElementById("workflowDescription");
const primaryPathErrors = document.getElementById("primaryPathErrors");
const primaryMappingErrors = document.getElementById("primaryMappingErrors");
const referencePathErrors = document.getElementById("referencePathErrors");
const referenceMappingErrors = document.getElementById("referenceMappingErrors");
const outputErrors = document.getElementById("outputErrors");
const matchContract = document.getElementById("matchContract");
const summaryMatchInputs = document.getElementById("summaryMatchInputs");
const matchProgress = document.getElementById("matchProgress");
const matchProgressBar = document.getElementById("matchProgressBar");
const matchProgressLabel = document.getElementById("matchProgressLabel");
const matchProgressValue = document.getElementById("matchProgressValue");
const matchPreviewPanel = document.getElementById("matchPreviewPanel");
const workflowMetaPanel = document.getElementById("workflowMetaPanel");
const workflowSavedConfigs = document.getElementById("workflowSavedConfigs");
const quickPresetBar = document.getElementById("quickPresetBar");
const thresholdCard = document.getElementById("thresholdCard");
const jobSpecPanel = document.getElementById("jobSpecPanel");

const normalizeInputFile = document.getElementById("normalizeInputFile");
const normalizeInputStatus = document.getElementById("normalizeInputStatus");
const normalizeOutputName = document.getElementById("normalizeOutputName");
const normalizeStrictCleanup = document.getElementById("normalizeStrictCleanup");
const normalizeProfileSelect = document.getElementById("normalizeProfileSelect");
const normalizeProfileMeta = document.getElementById("normalizeProfileMeta");
const normalizeHeaders = document.getElementById("normalizeHeaders");
const normalizeMappingForm = document.getElementById("normalizeMappingForm");
const normalizeResultBox = document.getElementById("normalizeResultBox");
const normalizePathErrors = document.getElementById("normalizePathErrors");
const normalizeMappingErrors = document.getElementById("normalizeMappingErrors");
const normalizeOutputErrors = document.getElementById("normalizeOutputErrors");
const normalizeDownloadArea = document.getElementById("normalizeDownloadArea");
const normalizedPreview = document.getElementById("normalizedPreview");
const normalizeProgress = document.getElementById("normalizeProgress");
const normalizeProgressBar = document.getElementById("normalizeProgressBar");
const normalizeProgressLabel = document.getElementById("normalizeProgressLabel");
const normalizeProgressValue = document.getElementById("normalizeProgressValue");
const inventoryInputs = document.getElementById("inventoryInputs");
const inventoryNormalized = document.getElementById("inventoryNormalized");
const inventoryMatchInputs = document.getElementById("inventoryMatchInputs");
const inventoryOutputs = document.getElementById("inventoryOutputs");
const deleteAllOutputsButton = document.getElementById("deleteAllOutputs");
const customProfileName = document.getElementById("customProfileName");
const customProfileTarget = document.getElementById("customProfileTarget");
const customProfileRows = document.getElementById("customProfileRows");
const customProfileErrors = document.getElementById("customProfileErrors");
const customProfilePreview = document.getElementById("customProfilePreview");

let workflows = [];
let availableStages = [];
let currentWorkflowMeta = null;
let normalizationProfiles = [];
let presets = [];
let normalizeState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
let primaryState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
let referenceState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
let customStagePlan = [];
let customStageZoomLevel = 100;
let draggedStageId = "";
let customProfileRuleCount = 0;
let primaryUploadedPath = "";
let referenceUploadedPath = "";
let normalizeUploadedPath = "";
let demoDefaults = null;
const DEFAULT_OUTPUT_ROOT = "output/webapp_runs";
const ACTIVE_TAB_STORAGE_KEY = "pipeline.activeTab";
let activeWorkflowSection = "match";
let inventoryState = { inputs: [], normalized: [], outputs: [] };
let pendingDangerAction = null;
let normalizeProgressTimer = null;
let matchProgressTimer = null;
let activeMatchJobId = "";
const NORMALIZE_CANONICAL_FIELDS = [
  "person_id",
  "first_name",
  "middle_name",
  "last_name",
  "primary_address1",
  "primary_address2",
  "mail_city",
  "mail_state",
  "mail_zip",
  "email",
  "phone"
];
const NORMALIZE_FIELD_DESCRIPTIONS = {
  person_id: "Stable ID if the source has one.",
  first_name: "Given name used for matching and review.",
  middle_name: "Middle name or initial when present.",
  last_name: "Family name used for matching and review.",
  primary_address1: "Main street address field. Built-in unit splitting reads from this field.",
  primary_address2: "Apartment, suite, or unit. Built-in splitting writes the extracted unit here.",
  mail_city: "Mailing city or locality.",
  mail_state: "Mailing province or state.",
  mail_zip: "Mailing postal or zip code.",
  email: "Preferred email field.",
  phone: "Preferred phone field."
};
const BUILDER_TEMPLATES = {
  match_template: {
    label: "Match Template",
    stages: [
      { name: "normalize_addresses", config: { dataset_role: "primary", mode: "member" }, x: 40, y: 80 },
      { name: "normalize_addresses", config: { dataset_role: "reference", mode: "voter" }, x: 420, y: 80 },
      { name: "match_records", config: { primary_role: "primary", reference_role: "reference" }, x: 800, y: 80 },
      { name: "write_records_bundle", config: { dataset_role: "primary" }, x: 1180, y: 80 }
    ]
  },
  enrich_template: {
    label: "Enrich Template",
    stages: [
      { name: "join_reference_fields", config: { primary_role: "primary", reference_role: "reference", left_on: "person_id", right_on: "person_id", take_columns: "email,phone" }, x: 40, y: 80 },
      { name: "project_records", config: { source_role: "primary", artifact_name: "enriched_records" }, x: 460, y: 80 },
      { name: "write_records_output", config: { source_kind: "artifact", source_name: "enriched_records", output_key: "enriched_records" }, x: 840, y: 80 }
    ]
  }
};
const MAPPING_SLOT_LABELS = ["Primary source", "Fallback 1", "Fallback 2"];
const GROUP_CANONICAL_TARGETS = {
  identity: ["person_id", "first_name", "middle_name", "last_name"],
  email: ["email"],
  phone: ["phone"],
  address: ["primary_address1", "primary_address2", "mail_city", "mail_state", "mail_zip", "primary_city", "primary_state", "primary_zip"],
  dates: ["created_at", "membership_end_date"],
  money: ["amount"],
  other: []
};

function mappingSlots(value) {
  const values = Array.isArray(value) ? value : value ? [value] : [];
  return [...values.slice(0, 3), "", "", ""].slice(0, 3);
}

function updateMappingValue(state, field, values) {
  const chosen = values.filter(Boolean);
  if (!chosen.length) {
    delete state.mapping[field];
  } else if (chosen.length === 1) {
    state.mapping[field] = chosen[0];
  } else {
    state.mapping[field] = chosen;
  }
}

function buildMappingSelect(headerOptions, value, placeholder) {
  const select = document.createElement("select");
  const blank = document.createElement("option");
  blank.value = "";
  blank.textContent = placeholder;
  select.appendChild(blank);
  headerOptions.forEach((option) => {
    const el = document.createElement("option");
    el.value = option.value;
    el.textContent = option.label;
    select.appendChild(el);
  });
  select.value = value || "";
  return select;
}

function pretty(value) {
  return JSON.stringify(value, null, 2);
}

function isBuilderWorkflow(workflow) {
  return workflow === "custom_job";
}

function defaultOutputDirectory() {
  const workflow = workflowSelect?.value || "run";
  return `${DEFAULT_OUTPUT_ROOT}/${workflow}`;
}

function defaultNormalizedOutputName(sourcePath) {
  const filename = (sourcePath || "normalized").split("/").pop().replace(/\.csv$/i, "");
  return `${filename}_normalized.csv`;
}

function currentMatchPreset() {
  const confident = Number(confidentThreshold.value || 0);
  const possible = Number(possibleThreshold.value || 0);
  const review = Number(reviewThreshold.value || 0);
  const strict = strictMatchMode.checked;
  if (confident === 180 && possible === 140 && review === 95 && strict) {
    return "conservative";
  }
  if (confident === 145 && possible === 110 && review === 75 && !strict) {
    return "aggressive";
  }
  if (confident === 160 && possible === 120 && review === 85 && !strict) {
    return "balanced";
  }
  return "";
}

function renderActiveMatchPreset() {
  const active = currentMatchPreset();
  [
    [presetConservative, "conservative"],
    [presetBalanced, "balanced"],
    [presetAggressive, "aggressive"]
  ].forEach(([button, name]) => {
    button.classList.toggle("active-preset", active === name);
    button.setAttribute("aria-pressed", active === name ? "true" : "false");
  });
}

function applyMatchPreset(presetName) {
  if (presetName === "conservative") {
    confidentThreshold.value = 180;
    possibleThreshold.value = 140;
    reviewThreshold.value = 95;
    strictMatchMode.checked = true;
  } else if (presetName === "aggressive") {
    confidentThreshold.value = 145;
    possibleThreshold.value = 110;
    reviewThreshold.value = 75;
    strictMatchMode.checked = false;
  } else {
    confidentThreshold.value = 160;
    possibleThreshold.value = 120;
    reviewThreshold.value = 85;
    strictMatchMode.checked = false;
  }
  renderActiveMatchPreset();
  syncPreview();
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(pretty(data));
  }
  return data;
}

function startProgress(type) {
  const isNormalize = type === "normalize";
  const shell = isNormalize ? normalizeProgress : matchProgress;
  const bar = isNormalize ? normalizeProgressBar : matchProgressBar;
  const label = isNormalize ? normalizeProgressLabel : matchProgressLabel;
  const value = isNormalize ? normalizeProgressValue : matchProgressValue;
  const steps = isNormalize
    ? [
        { percent: 8, label: "Preparing normalization..." },
        { percent: 26, label: "Loading source file..." },
        { percent: 48, label: "Applying normalization rules..." },
        { percent: 74, label: "Writing normalized CSV..." },
        { percent: 92, label: "Refreshing previews..." }
      ]
    : [
        { percent: 8, label: "Preparing match run..." },
        { percent: 24, label: "Loading staged files..." },
        { percent: 46, label: "Normalizing comparison fields..." },
        { percent: 70, label: "Scoring candidate matches..." },
        { percent: 90, label: "Writing outputs and summary..." }
      ];

  shell.classList.remove("hidden");
  bar.style.width = `${steps[0].percent}%`;
  label.textContent = steps[0].label;
  value.textContent = `${steps[0].percent}%`;

  let index = 0;
  const timer = setInterval(() => {
    index = Math.min(index + 1, steps.length - 1);
    const step = steps[index];
    bar.style.width = `${step.percent}%`;
    label.textContent = step.label;
    value.textContent = `${step.percent}%`;
  }, 650);

  if (isNormalize) {
    normalizeProgressTimer = timer;
  } else {
    matchProgressTimer = timer;
  }
}

function finishProgress(type, finalLabel) {
  const isNormalize = type === "normalize";
  const shell = isNormalize ? normalizeProgress : matchProgress;
  const bar = isNormalize ? normalizeProgressBar : matchProgressBar;
  const label = isNormalize ? normalizeProgressLabel : matchProgressLabel;
  const value = isNormalize ? normalizeProgressValue : matchProgressValue;
  const timer = isNormalize ? normalizeProgressTimer : matchProgressTimer;

  if (timer) {
    clearInterval(timer);
    if (isNormalize) {
      normalizeProgressTimer = null;
    } else {
      matchProgressTimer = null;
    }
  }

  bar.style.width = "100%";
  label.textContent = finalLabel;
  value.textContent = "100%";

  setTimeout(() => {
    shell.classList.add("hidden");
  }, 900);
}

function setMatchHelpOpen(isOpen) {
  matchHelpModal.classList.toggle("hidden", !isOpen);
}

function setDangerModalOpen(isOpen) {
  dangerModal.classList.toggle("hidden", !isOpen);
  if (!isOpen) {
    pendingDangerAction = null;
  }
}

function confirmDangerousAction(message, action) {
  dangerModalMessage.textContent = message;
  pendingDangerAction = action;
  setDangerModalOpen(true);
}

async function uploadSelectedFile(file, statusEl) {
  const content = await file.text();
  statusEl.textContent = `Uploading ${file.name}...`;
  const result = await fetchJson("/api/upload-file", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filename: file.name,
      content
    })
  });
  statusEl.textContent = `${result.filename} stored at ${result.stored_path}`;
  await loadInventory();
  return result.stored_path;
}

function setTab(active) {
  const normalizeActive = active === "normalize";
  const matchActive = active === "match";
  const utilitiesActive = active === "utilities";
  tabNormalize.classList.toggle("active", normalizeActive);
  normalizeTab.classList.toggle("active", normalizeActive);
  tabMatch.classList.toggle("active", matchActive);
  tabUtilities.classList.toggle("active", utilitiesActive);
  matchTab.classList.toggle("active", !normalizeActive);
  matchPreviewPanel.classList.toggle("hidden", !matchActive);
  if (matchActive) {
    activeWorkflowSection = "match";
    renderWorkflowOptions(workflowSelect?.value || "");
    void applyWorkflowDemoDefaults(false);
  } else if (utilitiesActive) {
    activeWorkflowSection = "utilities";
    renderWorkflowOptions(workflowSelect?.value || "");
    void applyWorkflowDemoDefaults(false);
  } else if (normalizeActive) {
    void applyPrepDemoDefaults(false);
  }
  try {
    localStorage.setItem(ACTIVE_TAB_STORAGE_KEY, active);
  } catch {
    // Ignore storage failures and keep the UI usable.
  }
}

function workflowsForActiveSection() {
  return workflows.filter((workflow) => {
    const category = workflow.category || "match";
    return category === activeWorkflowSection;
  });
}

function renderWorkflowOptions(preferredWorkflow = "") {
  const available = workflowsForActiveSection();
  workflowSelect.innerHTML = "";
  available.forEach((workflow) => {
    const option = document.createElement("option");
    option.value = workflow.workflow;
    option.textContent = `${workflow.workflow} — ${workflow.label}`;
    workflowSelect.appendChild(option);
  });
  const preferredAllowed = available.some((workflow) => workflow.workflow === preferredWorkflow);
  if (preferredAllowed) {
    workflowSelect.value = preferredWorkflow;
  } else if (available.length) {
    workflowSelect.value = available[0].workflow;
  }
}

function setSelectOptions(selectEl, options, placeholder = "None") {
  selectEl.innerHTML = "";
  const blank = document.createElement("option");
  blank.value = "";
  blank.textContent = placeholder;
  selectEl.appendChild(blank);
  options.forEach((option) => {
    const el = document.createElement("option");
    el.value = option.value;
    el.textContent = option.label;
    selectEl.appendChild(el);
  });
}

function appendInlineError(container, message) {
  const item = document.createElement("div");
  item.className = "inline-error";
  item.textContent = message;
  container.appendChild(item);
}

function clearInlineErrors() {
  [
    workflowErrors,
    primaryPathErrors,
    primaryMappingErrors,
    referencePathErrors,
    referenceMappingErrors,
    outputErrors,
    normalizePathErrors,
    normalizeMappingErrors,
    normalizeOutputErrors,
    customProfileErrors
  ].forEach((container) => {
    container.innerHTML = "";
  });
}

function formatMatchInputValue(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).join(", ") || "Unmapped";
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value);
    if (!entries.length) return "Unmapped";
    return entries.map(([key, item]) => `${key}: ${formatMatchInputValue(item)}`).join(" | ");
  }
  return value || "Unmapped";
}

function describeNormalizationProfile(profileName) {
  if (!profileName) {
    return "No normalization profile selected.";
  }
  const profile = normalizationProfiles.find((item) => item.name === profileName);
  if (!profile) {
    return "Selected normalization profile could not be loaded.";
  }
  const targets = (profile.targets || []).length ? profile.targets.join(", ") : "no derived targets";
  const strategies = Object.entries(profile.strategies || {})
    .map(([field, strategy]) => `${field}:${strategy}`)
    .join(", ");
  return `Targets: ${targets}. Strategies: ${strategies || "none"}.`;
}

function refreshNormalizationMeta() {
  normalizeProfileMeta.textContent = describeNormalizationProfile(normalizeProfileSelect.value);
}

async function loadNormalizationProfiles() {
  normalizationProfiles = await fetchJson("/api/normalization-profiles");
  const options = normalizationProfiles.map((profile) => ({ value: profile.name, label: profile.name }));
  setSelectOptions(normalizeProfileSelect, options, "No normalization profile");
  refreshNormalizationMeta();
}

function setSelectedPath(kind, path) {
  if (kind === "normalize") {
    normalizeUploadedPath = path;
    normalizeInputStatus.textContent = path;
    normalizeState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
    normalizeHeaders.textContent = "";
    renderNormalizeMappingForm();
    if (!normalizeOutputName.value.trim()) {
      normalizeOutputName.value = defaultNormalizedOutputName(path);
    }
    hydrateNormalizeSelection().catch((error) => {
      normalizeResultBox.textContent = String(error.message || error);
    });
    return;
  }
  if (kind === "primary") {
    primaryUploadedPath = path;
    primaryFileStatus.textContent = path;
    syncPreview();
    return;
  }
  if (kind === "reference") {
    referenceUploadedPath = path;
    referenceFileStatus.textContent = path;
    syncPreview();
  }
}

function clearSelectedPathIfDeleted(path) {
  if (normalizeUploadedPath === path) {
    normalizeUploadedPath = "";
    normalizeInputStatus.textContent = "No file selected.";
    normalizeHeaders.textContent = "";
    normalizeState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
    renderNormalizeMappingForm();
  }
  if (primaryUploadedPath === path) {
    primaryUploadedPath = "";
    primaryFileStatus.textContent = "No file selected.";
    primaryHeaders.textContent = "";
    primaryHeaderGroups.innerHTML = "";
  }
  if (referenceUploadedPath === path) {
    referenceUploadedPath = "";
    referenceFileStatus.textContent = "No file selected.";
    referenceHeaders.textContent = "";
    referenceHeaderGroups.innerHTML = "";
  }
  syncPreview();
}

async function deleteProjectFile(path) {
  confirmDangerousAction(`Delete this project file?\n${path}`, async () => {
    await fetchJson("/api/delete-file", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path })
    });
    clearSelectedPathIfDeleted(path);
    await loadInventory();
  });
}

function clearOutputViews() {
  summaryCards.innerHTML = "";
  summaryOutputs.innerHTML = "";
  summaryReasons.innerHTML = "";
  summaryMatchInputs.innerHTML = "";
  reviewPreview.innerHTML = "";
  unmatchedPreview.innerHTML = "";
  resultBox.textContent = "";
}

async function deleteAllOutputs() {
  confirmDangerousAction("Delete all recent project outputs under output/?", async () => {
    await fetchJson("/api/delete-all-outputs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({})
    });
    clearOutputViews();
    await loadInventory();
  });
}

function renderInventoryList(container, files, actions) {
  container.innerHTML = "";
  if (!files.length) {
    container.innerHTML = `<div class="preview-empty">No files yet</div>`;
    return;
  }

  files.forEach((file) => {
    const item = document.createElement("div");
    item.className = "inventory-item";
    const buttons = actions.map((action) =>
      `<button type="button" class="ghost small inventory-action" data-action="${action.id}" data-path="${file.path}">${action.label}</button>`
    ).join("");
    item.innerHTML = `
      <div class="inventory-meta">
        <strong>${file.name}</strong>
        <code>${file.relative_path}</code>
      </div>
      <div class="inventory-actions">
        ${buttons}
        <a class="inventory-link" href="${file.download_path}">Download</a>
        <button type="button" class="ghost small inventory-delete" data-path="${file.path}">Delete</button>
      </div>
    `;
    item.querySelectorAll(".inventory-action").forEach((button) => {
      button.addEventListener("click", () => {
        setSelectedPath(button.dataset.action, button.dataset.path);
      });
    });
    item.querySelector(".inventory-delete").addEventListener("click", async () => {
      try {
        await deleteProjectFile(file.path);
      } catch (error) {
        resultBox.textContent = String(error.message || error);
      }
    });
    container.appendChild(item);
  });
}

function renderInventory() {
  renderInventoryList(inventoryInputs, inventoryState.inputs, [
    { id: "normalize", label: "Use for Normalize" }
  ]);
  renderInventoryList(inventoryNormalized, inventoryState.normalized, [
    { id: "normalize", label: "Use for Normalize" },
    { id: "primary", label: "Use as Primary" }
  ]);
  renderInventoryList(inventoryMatchInputs, [...inventoryState.normalized, ...inventoryState.inputs], [
    { id: "primary", label: "Use as Primary" },
    { id: "reference", label: "Use as Reference" }
  ]);
  renderInventoryList(
    inventoryOutputs,
    inventoryState.outputs.filter((file) => file.name !== "run_summary.json"),
    []
  );
}

async function loadInventory() {
  inventoryState = await fetchJson("/api/file-inventory");
  renderInventory();
}

async function loadPresets() {
  presets = await fetchJson("/api/presets");
  const options = presets.map((preset) => ({
    value: preset.name,
    label: preset.workflow ? `${preset.name} — ${preset.workflow}` : preset.name
  }));
  setSelectOptions(presetSelect, options, "Select preset");
  const hasPresets = presets.length > 0;
  loadPresetButton.disabled = !hasPresets;
  presetSelect.disabled = !hasPresets;
  presetHelp.textContent = hasPresets
    ? "Pick a saved configuration and load it, or save the current one under a new name."
    : "No saved configurations yet. Enter a name and click Save Current Config to create one.";
}

async function loadWorkflows() {
  workflows = await fetchJson("/api/workflows");
  renderWorkflowOptions(workflowSelect.value);
  if (!outputPath.value.trim()) {
    outputPath.value = defaultOutputDirectory();
  }
  await showWorkflow();
}

async function loadDemoDefaults() {
  demoDefaults = await fetchJson("/api/demo-defaults");
}

async function loadStages() {
  availableStages = await fetchJson("/api/stages");
  if (builderTemplateSelect) {
    setSelectOptions(
      builderTemplateSelect,
      Object.entries(BUILDER_TEMPLATES).map(([value, template]) => ({ value, label: template.label })),
      "Select builder template"
    );
  }
  renderStageLibrary();
  showSelectedStageDetail();
}

function collectCanonicalFields(inputName) {
  const required = currentWorkflowMeta?.required_fields?.[inputName] || [];
  const recommended = currentWorkflowMeta?.recommended_fields?.[inputName] || [];
  const state = inputName === "primary" ? primaryState : referenceState;
  const suggested = Object.keys(state.suggestions || {});
  const current = Object.keys(state.mapping || {});
  return Array.from(new Set([...required, ...recommended, ...suggested, ...current])).sort();
}

function collectNormalizeCanonicalFields() {
  const suggested = Object.keys(normalizeState.suggestions || {});
  const current = Object.keys(normalizeState.mapping || {});
  return Array.from(new Set([...NORMALIZE_CANONICAL_FIELDS, ...suggested, ...current]))
    .filter((field) => NORMALIZE_CANONICAL_FIELDS.includes(field))
    .sort((left, right) => NORMALIZE_CANONICAL_FIELDS.indexOf(left) - NORMALIZE_CANONICAL_FIELDS.indexOf(right));
}

function workflowInputsForBuilder() {
  if (!currentWorkflowMeta) return [];
  if (workflowSelect.value === "custom_job") {
    return ["primary", "reference"];
  }
  return currentWorkflowMeta.inputs || [];
}

function defaultBundleOutputs() {
  return {
    matched_records: { match_status: "CONFIDENT", filename: "matched_records.csv" },
    review_records: { match_status: ["POSSIBLE", "REVIEW"], filename: "review_records.csv" },
    unmatched_records: { match_status: "UNMATCHED", filename: "unmatched_records.csv" }
  };
}

function defaultCustomStagePlan() {
  return buildTemplatePlan("match_template");
}

function templateForWorkflow(workflow) {
  if (workflow === "custom_job") return builderTemplateSelect?.value || "match_template";
  if (workflow === "enrich_records_from_reference") return "enrich_template";
  if (["match_records_to_reference", "compare_records_to_reference", "identify_missing_records_from_system", "process_full_records"].includes(workflow)) {
    return "match_template";
  }
  return "";
}

function buildTemplatePlan(templateName) {
  const template = BUILDER_TEMPLATES[templateName] || BUILDER_TEMPLATES.match_template;
  const plan = template.stages.map((stage, index) => ({
    id: crypto.randomUUID(),
    name: stage.name,
    x: stage.x ?? defaultStagePosition(index).x,
    y: stage.y ?? defaultStagePosition(index).y,
    expanded: false,
    config: { ...stage.config }
  }));
  plan.forEach((stage, index) => {
    stage.nextStageId = plan[index + 1]?.id || "";
  });
  return plan;
}

function defaultStageConfig(stageName) {
  switch (stageName) {
    case "normalize_addresses":
      return { dataset_role: "primary", mode: "member" };
    case "normalize_date_columns":
      return { dataset_role: "primary", columns: "created_at,membership_end_date" };
    case "dedupe_records":
    case "aggregate_contacts":
    case "classify_address_status":
    case "score_priority":
    case "write_records_bundle":
      return { dataset_role: "primary" };
    case "match_records":
    case "flag_reference_identity":
      return { primary_role: "primary", reference_role: "reference" };
    case "join_reference_fields":
      return { primary_role: "primary", reference_role: "reference", left_on: "person_id", right_on: "person_id", take_columns: "email,phone" };
    case "project_selected_columns":
      return { source_role: "primary", columns: "first_name:first_name,last_name:last_name,email:email" };
    default:
      return {};
  }
}

function stageLabel(stageName) {
  return availableStages.find((stage) => stage.name === stageName)?.label || stageName;
}

function stageMeta(stageName) {
  return availableStages.find((stage) => stage.name === stageName) || null;
}

function stageContextLabel(stage) {
  if (!stage?.config) return "";
  if (stage.name === "select_match_bucket" && stage.config.source_role && stage.config.target_role) {
    return `${stage.config.source_role} -> ${stage.config.target_role}`;
  }
  if (stage.name === "join_reference_fields" && stage.config.left_on && stage.config.right_on) {
    return `${stage.config.left_on} -> ${stage.config.right_on}`;
  }
  if (stage.config.dataset_role) {
    return stage.config.dataset_role;
  }
  if (stage.config.primary_role && stage.config.reference_role) {
    return `${stage.config.primary_role} -> ${stage.config.reference_role}`;
  }
  if (stage.config.source_role) {
    return stage.config.source_role;
  }
  return "";
}

function stageFamily(stageName) {
  if (["normalize_names", "normalize_addresses", "normalize_date_columns", "dedupe_records"].includes(stageName)) {
    return "Prep";
  }
  if (["match_records", "flag_reference_identity", "select_match_bucket", "join_reference_fields"].includes(stageName)) {
    return "Match";
  }
  if (["project_records", "project_selected_columns", "write_records_output", "write_records_bundle", "extract_projection"].includes(stageName)) {
    return "Output";
  }
  if (["aggregate_contacts", "classify_address_status", "score_priority"].includes(stageName)) {
    return "Refine";
  }
  return "Step";
}

function formatColumnMapForEditor(columnMap) {
  if (!columnMap || typeof columnMap !== "object") return "";
  return Object.entries(columnMap).map(([target, source]) => `${target}:${source}`).join(", ");
}

function parseColumnMapString(value) {
  const result = {};
  String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .forEach((pair) => {
      const [target, ...rest] = pair.split(":");
      const source = rest.join(":").trim();
      if (target?.trim() && source) {
        result[target.trim()] = source;
      }
    });
  return result;
}

function showSelectedStageDetail() {
  if (!customStageDescription) return;
  const selectedName = stageLibrary?.querySelector(".stage-library-card.active")?.dataset.stageName || "";
  const stage = availableStages.find((item) => item.name === selectedName);
  if (!stage) {
    customStageDescription.innerHTML = `
      <strong>Pick a stage card to add it.</strong>
      <span>Use the canvas to arrange steps visually. Remove cards from the workflow itself when you do not need them.</span>
    `;
    return;
  }
  customStageDescription.innerHTML = `
    <strong>${stage.label}</strong>
    <span>${stage.summary || "No stage summary available."}</span>
  `;
}

function setCustomStageZoom(nextValue) {
  customStageZoomLevel = Math.max(70, Math.min(140, Number(nextValue) || 100));
  if (customStageZoom) customStageZoom.value = String(customStageZoomLevel);
  if (customStageCanvas) customStageCanvas.style.transform = `scale(${customStageZoomLevel / 100})`;
}

function defaultStagePosition(index) {
  return { x: 40 + index * 380, y: 40 };
}

function drawCustomStageConnectors() {
  const svg = customStageSvg;
  if (!svg || !customStageCanvas) return;
  const nodes = Array.from(customStageList.querySelectorAll(".custom-stage-step"));
  const width = Math.max(
    ...nodes.map((node) => (Number(node.style.left.replace("px", "")) || 0) + node.offsetWidth + 120),
    customStageViewport?.clientWidth || 1200
  );
  const height = Math.max(
    ...nodes.map((node) => (Number(node.style.top.replace("px", "")) || 0) + node.offsetHeight + 120),
    320
  );
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("width", String(width));
  svg.setAttribute("height", String(height));
  svg.innerHTML = `<defs><marker id="workflowArrow" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#94a3b8"></path></marker></defs>`;
  const nodeMap = new Map(nodes.map((node) => [node.dataset.stageId, node]));
  for (const stage of customStagePlan) {
    const current = nodeMap.get(stage.id);
    const next = nodeMap.get(stage.nextStageId);
    if (!current || !next) continue;
    const x1 = (Number(current.style.left.replace("px", "")) || 0) + current.offsetWidth;
    const y1 = (Number(current.style.top.replace("px", "")) || 0) + current.offsetHeight / 2;
    const x2 = Number(next.style.left.replace("px", "")) || 0;
    const y2 = (Number(next.style.top.replace("px", "")) || 0) + next.offsetHeight / 2;
    const delta = Math.max(60, Math.abs(x2 - x1) / 2);
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", `M ${x1} ${y1} C ${x1 + delta} ${y1}, ${x2 - delta} ${y2}, ${x2} ${y2}`);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "#94a3b8");
    path.setAttribute("stroke-width", "3");
    path.setAttribute("stroke-linecap", "round");
    path.setAttribute("marker-end", "url(#workflowArrow)");
    svg.appendChild(path);
  }
}

function renderStageLibrary() {
  if (!stageLibrary) return;
  stageLibrary.innerHTML = "";
  const grouped = availableStages.reduce((acc, stage) => {
    const family = stageFamily(stage.name);
    acc[family] = acc[family] || [];
    acc[family].push(stage);
    return acc;
  }, {});
  Object.entries(grouped).forEach(([family, stages], familyIndex) => {
    const section = document.createElement("details");
    section.className = "stage-library-group";
    if (familyIndex < 2) section.open = true;
    const summary = document.createElement("summary");
    summary.className = "stage-library-group-head";
    summary.innerHTML = `<span class="stage-library-family">${family}</span><strong>${family} stages</strong><span class="stage-library-count">${stages.length}</span>`;
    section.appendChild(summary);
    const list = document.createElement("div");
    list.className = "stage-library-group-list";
    stages.forEach((stage) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "stage-library-card";
      button.dataset.stageName = stage.name;
      button.innerHTML = `<strong>${stage.label}</strong><span class="stage-library-summary">${stage.summary || ""}</span><span class="stage-library-add">Add</span>`;
      button.addEventListener("click", () => {
        stageLibrary.querySelectorAll(".stage-library-card").forEach((card) => card.classList.remove("active"));
        button.classList.add("active");
        showSelectedStageDetail();
        customStagePlan.push({
          id: crypto.randomUUID(),
          name: stage.name,
          expanded: true,
          config: defaultStageConfig(stage.name),
          ...defaultStagePosition(customStagePlan.length)
        });
        renderCustomStagePlan();
        syncPreview();
      });
      list.appendChild(button);
    });
    section.appendChild(list);
    stageLibrary.appendChild(section);
  });
}

function datasetRoleOptions() {
  const inputs = workflowInputsForBuilder();
  return ["primary", "reference", "source"]
    .filter((value) => workflowSelect.value === "custom_job" ? value !== "source" : inputs.includes(value))
    .map((value) => ({ value, label: value }));
}

function ensureCustomStagePlan() {
  if (!isBuilderWorkflow(workflowSelect.value)) return;
  if (!customStagePlan.length) {
    customStagePlan = defaultCustomStagePlan();
  }
}

function renderMappingForm(inputName) {
  const container = inputName === "primary" ? primaryMappingForm : referenceMappingForm;
  const state = inputName === "primary" ? primaryState : referenceState;
  const fields = collectCanonicalFields(inputName);
  container.innerHTML = "";

  if (!fields.length) {
    container.textContent = "Inspect a file to generate mapping controls.";
    return;
  }

  const headerOptions = state.headers.map((header) => ({ value: header, label: header }));
  fields.forEach((field) => {
    const row = document.createElement("div");
    row.className = "mapping-row";

    const label = document.createElement("label");
    label.textContent = field;
    const inputs = document.createElement("div");
    inputs.className = "mapping-inputs";
    const slots = mappingSlots(state.mapping[field]);
    const selects = slots.map((slotValue, index) => {
      const select = buildMappingSelect(headerOptions, slotValue, MAPPING_SLOT_LABELS[index]);
      select.addEventListener("change", () => {
        updateMappingValue(state, field, selects.map((item) => item.value));
        syncPreview();
      });
      return select;
    });
    selects.forEach((select) => inputs.appendChild(select));

    row.appendChild(label);
    row.appendChild(inputs);
    container.appendChild(row);
  });
}

function renderNormalizeMappingForm() {
  normalizeMappingForm.innerHTML = "";
  const fields = collectNormalizeCanonicalFields();
  if (!normalizeState.headers.length) {
    normalizeMappingForm.textContent = "Inspect a file to generate mapping controls.";
    return;
  }

  const headerOptions = normalizeState.headers.map((header) => ({ value: header, label: header }));
  fields.forEach((field) => {
    const row = document.createElement("div");
    row.className = "mapping-row";

    const labelWrap = document.createElement("div");
    labelWrap.className = "mapping-label-wrap";

    const label = document.createElement("label");
    label.textContent = field;
    labelWrap.appendChild(label);

    const helper = document.createElement("div");
    helper.className = "mapping-helper";
    helper.textContent = NORMALIZE_FIELD_DESCRIPTIONS[field] || "";
    labelWrap.appendChild(helper);

    const inputs = document.createElement("div");
    inputs.className = "mapping-inputs";
    const slots = mappingSlots(normalizeState.mapping[field]);
    const selects = slots.map((slotValue, index) => {
      const select = buildMappingSelect(headerOptions, slotValue, MAPPING_SLOT_LABELS[index]);
      select.addEventListener("change", () => {
        updateMappingValue(normalizeState, field, selects.map((item) => item.value));
      });
      return select;
    });
    selects.forEach((select) => inputs.appendChild(select));

    row.appendChild(labelWrap);
    row.appendChild(inputs);
    normalizeMappingForm.appendChild(row);
  });
}

function renderCustomStagePlan() {
  const isCustom = isBuilderWorkflow(workflowSelect.value);
  customStagePanel.classList.toggle("hidden", !isCustom);
  if (!isCustom) return;
  showSelectedStageDetail();
  setCustomStageZoom(customStageZoomLevel);

  customStageList.innerHTML = "";
  if (customStageSvg) {
    customStageSvg.innerHTML = "";
  }
  if (!customStagePlan.length) {
    customStageList.innerHTML = `<div class="custom-stage-empty">No stages yet. Add a stage or reset to the default match template.</div>`;
    return;
  }

  const roleOptions = datasetRoleOptions();
  customStagePlan.forEach((stage, index) => {
    const step = document.createElement("div");
    step.className = "custom-stage-step";
    if (stage.x == null || stage.y == null) {
      Object.assign(stage, defaultStagePosition(index));
    }
    step.style.left = `${stage.x}px`;
    step.style.top = `${stage.y}px`;
    step.dataset.stageId = stage.id;

    const card = document.createElement("div");
    card.className = "custom-stage-card";
    card.classList.toggle("collapsed", !stage.expanded);
    const definition = stageMeta(stage.name);

    const head = document.createElement("div");
    head.className = "custom-stage-head";
    const meta = document.createElement("div");
    meta.className = "custom-stage-meta";
    const contextLabel = stageContextLabel(stage);
    meta.innerHTML = `
      <div class="custom-stage-topline">
        <span class="custom-stage-number">Step ${index + 1}</span>
        <span class="custom-stage-family">${stageFamily(stage.name)}</span>
      </div>
      <strong>${stageLabel(stage.name)}${contextLabel ? `: ${contextLabel}` : ""}</strong>
      ${definition?.summary ? `<p class="custom-stage-summary">${definition.summary}</p>` : ""}
    `;
    const actions = document.createElement("div");
    actions.className = "custom-stage-actions";
    const toggleButton = document.createElement("button");
    toggleButton.type = "button";
    toggleButton.className = "ghost small";
    toggleButton.textContent = stage.expanded ? "Collapse" : "Expand";
    toggleButton.addEventListener("click", () => {
      stage.expanded = !stage.expanded;
      renderCustomStagePlan();
    });
    actions.appendChild(toggleButton);
    const dragHint = document.createElement("span");
    dragHint.className = "custom-stage-draghint";
    dragHint.textContent = "Drag Card";
    actions.appendChild(dragHint);
    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = "ghost small custom-stage-remove";
    removeButton.textContent = "Remove";
    removeButton.addEventListener("click", () => {
      customStagePlan = customStagePlan.filter((item) => item.id !== stage.id);
      renderCustomStagePlan();
      syncPreview();
    });
    actions.appendChild(removeButton);
    head.appendChild(meta);
    head.appendChild(actions);
    card.appendChild(head);

    const grid = document.createElement("div");
    grid.className = "custom-stage-grid";
    grid.classList.toggle("hidden", !stage.expanded);

    const addSelectField = (labelText, key, options, defaultValue) => {
      const label = document.createElement("label");
      label.innerHTML = `<span>${labelText}</span>`;
      const select = buildMappingSelect(options, stage.config[key] || defaultValue, labelText);
      select.addEventListener("change", () => {
        stage.config[key] = select.value;
        syncPreview();
      });
      label.appendChild(select);
      grid.appendChild(label);
    };

    const addTextField = (labelText, key, defaultValue) => {
      const label = document.createElement("label");
      label.innerHTML = `<span>${labelText}</span>`;
      const input = document.createElement("input");
      input.type = "text";
      input.value = stage.config[key] ?? defaultValue;
      input.addEventListener("change", () => {
        stage.config[key] = input.value;
        syncPreview();
      });
      label.appendChild(input);
      grid.appendChild(label);
    };

    const connectLabel = document.createElement("label");
    connectLabel.innerHTML = `<span>Connect to</span>`;
    const connectOptions = [{ value: "", label: "No arrow" }].concat(
      customStagePlan
        .filter((item) => item.id !== stage.id)
        .map((item, itemIndex) => ({ value: item.id, label: `Step ${itemIndex + 1}: ${stageLabel(item.name)}` }))
    );
    const connectSelect = buildMappingSelect(connectOptions, stage.nextStageId || "", "Connect to");
    connectSelect.addEventListener("change", () => {
      stage.nextStageId = connectSelect.value;
      drawCustomStageConnectors();
      syncPreview();
    });
    connectLabel.appendChild(connectSelect);
    grid.appendChild(connectLabel);

    if (stage.name === "normalize_addresses") {
      addSelectField("Dataset role", "dataset_role", roleOptions, "primary");
      addSelectField("Mode", "mode", [{ value: "member", label: "member" }, { value: "voter", label: "voter" }], "member");
    } else if (["normalize_date_columns", "dedupe_records", "aggregate_contacts", "classify_address_status", "score_priority", "write_records_bundle"].includes(stage.name)) {
      addSelectField("Dataset role", "dataset_role", roleOptions, "primary");
      if (stage.name === "normalize_date_columns") {
        addTextField("Date columns", "columns", "created_at,membership_end_date");
      }
    } else if (["match_records", "flag_reference_identity"].includes(stage.name)) {
      addSelectField("Primary role", "primary_role", roleOptions, "primary");
      addSelectField("Reference role", "reference_role", roleOptions, "reference");
    } else if (stage.name === "join_reference_fields") {
      addSelectField("Primary role", "primary_role", roleOptions, "primary");
      addSelectField("Reference role", "reference_role", roleOptions, "reference");
      addTextField("Primary join key", "left_on", "person_id");
      addTextField("Reference join key", "right_on", "person_id");
      addTextField("Reference fields", "take_columns", "email,phone");
    } else if (stage.name === "project_selected_columns") {
      addSelectField("Source role", "source_role", roleOptions, "primary");
      addTextField("Output:source map", "columns", formatColumnMapForEditor(stage.config.columns) || "first_name:first_name,last_name:last_name,email:email");
    }

    if (grid.children.length) {
      card.appendChild(grid);
    }
    step.appendChild(card);
    customStageList.appendChild(step);
  });
  drawCustomStageConnectors();
}

function serializeCustomStageSequence() {
  return customStagePlan.map((stage) => {
    const config = { ...stage.config };
    if (stage.name === "normalize_addresses") {
      const isReference = config.dataset_role === "reference";
      return {
        name: stage.name,
        config: {
          dataset_role: config.dataset_role || "primary",
          mode: config.mode || (isReference ? "voter" : "member"),
          address1_col: "primary_address1",
          address2_col: "primary_address2",
          city_col: isReference ? "primary_city" : "mail_city",
          state_col: isReference ? "primary_state" : "mail_state",
          zip_col: isReference ? "primary_zip" : "mail_zip"
        }
      };
    }
    if (stage.name === "normalize_date_columns") {
      return {
        name: stage.name,
        config: {
          dataset_role: config.dataset_role || "primary",
          columns: String(config.columns || "").split(",").map((value) => value.trim()).filter(Boolean)
        }
      };
    }
    if (["dedupe_records", "aggregate_contacts", "classify_address_status", "score_priority"].includes(stage.name)) {
      return { name: stage.name, config: { dataset_role: config.dataset_role || "primary" } };
    }
    if (stage.name === "flag_reference_identity") {
      return {
        name: stage.name,
        config: {
          primary_role: config.primary_role || "primary",
          reference_role: config.reference_role || "reference",
          id_col: "person_id",
          first_col: "first_name",
          last_col: "last_name",
          address_col: "_address_norm"
        }
      };
    }
    if (stage.name === "match_records") {
      return {
        name: stage.name,
        config: {
          primary_role: config.primary_role || "primary",
          reference_role: config.reference_role || "reference",
          match_config: {
            primary_id_col: "person_id",
            reference_id_col: "person_id",
            primary_first_col: "first_name",
            reference_first_col: "first_name",
            primary_last_col: "last_name",
            reference_last_col: "last_name",
            primary_address_col: "_address_norm",
            reference_address_col: "_address_norm",
            primary_address1_col: "primary_address1",
            reference_address1_col: "primary_address1",
            primary_address2_col: "primary_address2",
            reference_address2_col: "primary_address2",
            primary_email_col: "email",
            reference_email_col: "email",
            primary_phone_col: "phone",
            reference_phone_col: "phone",
            confident_threshold: Number(confidentThreshold.value || 160),
            possible_threshold: Number(possibleThreshold.value || 120),
            review_threshold: Number(reviewThreshold.value || 85),
            strict_mode: strictMatchMode.checked
          }
        }
      };
    }
    if (stage.name === "write_records_bundle") {
      return {
        name: stage.name,
        config: {
          dataset_role: config.dataset_role || "primary",
          base_dir: outputPath.value.trim(),
          outputs: defaultBundleOutputs()
        }
      };
    }
    if (stage.name === "join_reference_fields") {
      return {
        name: stage.name,
        config: {
          primary_role: config.primary_role || "primary",
          reference_role: config.reference_role || "reference",
          left_on: config.left_on || "person_id",
          right_on: config.right_on || "person_id",
          take_columns: String(config.take_columns || "").split(",").map((value) => value.trim()).filter(Boolean)
        }
      };
    }
    if (stage.name === "project_selected_columns") {
      return {
        name: stage.name,
        config: {
          source_role: config.source_role || "primary",
          columns: parseColumnMapString(config.columns)
        }
      };
    }
    return { name: stage.name, config };
  });
}

function deserializeCustomStageSequence(stageSequence) {
  customStagePlan = (stageSequence || []).map((stage, index) => ({
    id: `${stage.name}-${index}-${Math.random().toString(36).slice(2, 8)}`,
    name: stage.name,
    ...defaultStagePosition(index),
    expanded: false,
    nextStageId: "",
    config: {
      ...(stage.config || {}),
      ...(stage.name === "project_selected_columns" ? { columns: formatColumnMapForEditor(stage.config?.columns) } : {}),
      ...(stage.name === "join_reference_fields" ? { take_columns: (stage.config?.take_columns || []).join(", ") } : {})
    }
  }));
  customStagePlan.forEach((stage, index) => {
    stage.nextStageId = customStagePlan[index + 1]?.id || "";
  });
}

function renderHeaderGroups(inputName) {
  const container = inputName === "primary" ? primaryHeaderGroups : referenceHeaderGroups;
  const state = inputName === "primary" ? primaryState : referenceState;
  container.innerHTML = "";
  const entries = Object.entries(state.groups || {});
  if (!entries.length) {
    return;
  }
  entries.forEach(([groupName, headers]) => {
    const card = document.createElement("div");
    card.className = "header-group-card";
    const head = document.createElement("div");
    head.className = "header-group-head";
    const title = document.createElement("strong");
    title.textContent = groupName.replaceAll("_", " ");
    head.appendChild(title);
    const targetFields = GROUP_CANONICAL_TARGETS[groupName]?.length ? GROUP_CANONICAL_TARGETS[groupName] : collectCanonicalFields(inputName);
    if (targetFields.length) {
      const targetSelect = buildMappingSelect(
        targetFields.map((field) => ({ value: field, label: field })),
        targetFields[0],
        "Target field"
      );
      targetSelect.addEventListener("change", () => {
        card.dataset.target = targetSelect.value;
      });
      card.dataset.target = targetSelect.value;
      head.appendChild(targetSelect);
    }
    const list = document.createElement("div");
    list.className = "header-group-list";
    headers.forEach((header) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "header-chip";
      chip.textContent = header;
      chip.addEventListener("click", () => {
        const target = card.dataset.target;
        if (!target) return;
        const next = mappingSlots(state.mapping[target]);
        if (next.includes(header)) return;
        const emptyIndex = next.findIndex((value) => !value);
        if (emptyIndex === -1) {
          next[next.length - 1] = header;
        } else {
          next[emptyIndex] = header;
        }
        updateMappingValue(state, target, next);
        renderMappingForm(inputName);
        syncPreview();
      });
      list.appendChild(chip);
    });
    card.appendChild(head);
    card.appendChild(list);
    container.appendChild(card);
  });
}

function buildJobSpecObject() {
  const workflow = workflowSelect.value;
  const inputs = {};
  const builderInputs = workflowInputsForBuilder();
  const singleSource = builderInputs.includes("source");
  const needsReconcileIdentity = [
    "compare_records_to_reference",
    "identify_missing_records_from_system"
  ].includes(workflow);

  if (builderInputs.includes("primary") && primaryUploadedPath) {
    inputs.primary = {
      path: primaryUploadedPath,
      columns: primaryState.mapping
    };
  }

  if (builderInputs.includes("reference") && referenceUploadedPath) {
    inputs.reference = {
      path: referenceUploadedPath,
      columns: referenceState.mapping
    };
  }

  if (singleSource) {
    inputs.source = {
      path: primaryUploadedPath,
      columns: primaryState.mapping
    };
  }

  const payload = {
    workflow,
    inputs,
    stages: {
      normalize: true,
      normalize_addresses: true,
      reconcile_identity: needsReconcileIdentity
    },
    match: {
      confident_threshold: Number(confidentThreshold.value || 160),
      possible_threshold: Number(possibleThreshold.value || 120),
      review_threshold: Number(reviewThreshold.value || 85),
      strict_mode: strictMatchMode.checked
    },
    outputs: {
      records: {
        base_dir: outputPath.value.trim()
      }
    }
  };

  if (workflow === "split_alternating_rows") {
    const baseDir = outputPath.value.trim() || defaultOutputDirectory();
    payload.outputs = {
      split: {
        path_a: `${baseDir}/split_a.csv`,
        path_b: `${baseDir}/split_b.csv`
      }
    };
  } else if (workflow === "extract_projection") {
    const baseDir = outputPath.value.trim() || defaultOutputDirectory();
    payload.outputs = {
      projection: {
        path: `${baseDir}/projection.csv`
      }
    };
  }

  if (workflow === "custom_job") {
    payload.stage_sequence = serializeCustomStageSequence();
    payload.builder_layout = customStagePlan.map((stage) => ({
      name: stage.name,
      x: stage.x,
      y: stage.y,
      next_stage_id: stage.nextStageId || "",
      expanded: Boolean(stage.expanded)
    }));
  }

  return payload;
}

function syncPreview() {
  jobSpec.value = pretty(buildJobSpecObject());
}

function renderValidationErrors(errors) {
  clearInlineErrors();
  if (!errors || !errors.length) return;

  errors.forEach((error) => {
    if (error.includes("Unsupported workflow") || error.includes("Missing required input")) {
      appendInlineError(workflowErrors, error);
    } else if (error.includes("Input 'primary' path")) {
      appendInlineError(primaryPathErrors, error);
    } else if (error.includes("Input 'reference' path")) {
      appendInlineError(referencePathErrors, error);
    } else if (error.includes("Input 'source' path")) {
      appendInlineError(primaryPathErrors, error);
    } else if (error.includes("Input 'primary' is missing canonical column mappings") || error.includes("Input 'primary' file")) {
      appendInlineError(primaryMappingErrors, error);
    } else if (error.includes("Input 'reference' is missing canonical column mappings") || error.includes("Input 'reference' file")) {
      appendInlineError(referenceMappingErrors, error);
    } else if (error.toLowerCase().includes("output")) {
      appendInlineError(outputErrors, error);
    } else {
      appendInlineError(workflowErrors, error);
    }
  });
}

function applyPresetToForm(preset) {
  workflowSelect.value = preset.workflow || workflowSelect.value;
  const workflowMeta = workflows.find((workflow) => workflow.workflow === (preset.workflow || workflowSelect.value));
  if (workflowMeta?.category === "utilities") {
    activeWorkflowSection = "utilities";
    renderWorkflowOptions(preset.workflow || workflowSelect.value);
  } else {
    activeWorkflowSection = "match";
    renderWorkflowOptions(preset.workflow || workflowSelect.value);
  }
  currentWorkflowMeta = null;
  const primary = preset.inputs?.primary || {};
  const reference = preset.inputs?.reference || {};
  const source = preset.inputs?.source || {};

  primaryUploadedPath = primary.path || source.path || "";
  referenceUploadedPath = reference.path || "";
  primaryFileStatus.textContent = primaryUploadedPath || "No file selected.";
  referenceFileStatus.textContent = referenceUploadedPath || "No file selected.";
  outputPath.value = preset.outputs?.records?.base_dir || defaultOutputDirectory();
  primaryState.mapping = { ...(primary.columns || source.columns || {}) };
  referenceState.mapping = { ...(reference.columns || {}) };
  strictMatchMode.checked = preset.match?.strict_mode ?? false;
  confidentThreshold.value = preset.match?.confident_threshold ?? 160;
  possibleThreshold.value = preset.match?.possible_threshold ?? 120;
  reviewThreshold.value = preset.match?.review_threshold ?? 85;
  if ((preset.workflow || workflowSelect.value) === "custom_job") {
    deserializeCustomStageSequence(preset.stage_sequence || []);
    if (Array.isArray(preset.builder_layout)) {
      customStagePlan = customStagePlan.map((stage, index) => ({
        ...stage,
        x: preset.builder_layout[index]?.x ?? stage.x,
        y: preset.builder_layout[index]?.y ?? stage.y,
        nextStageId: preset.builder_layout[index]?.next_stage_id ?? stage.nextStageId,
        expanded: preset.builder_layout[index]?.expanded ?? stage.expanded
      }));
    }
  } else {
    customStagePlan = [];
  }
}

function setFileStatus(statusEl, path, label) {
  statusEl.textContent = path ? `${path} (${label}; replace with your own file anytime)` : "No file selected.";
}

function defaultPathsForWorkflow(workflow) {
  return demoDefaults?.workflows?.[workflow] || null;
}

function allDemoPaths() {
  const paths = new Set();
  if (demoDefaults?.prep?.input_path) paths.add(demoDefaults.prep.input_path);
  Object.values(demoDefaults?.workflows || {}).forEach((value) => {
    if (value.primary_path) paths.add(value.primary_path);
    if (value.reference_path) paths.add(value.reference_path);
    if (value.source_path) paths.add(value.source_path);
  });
  return paths;
}

function isDemoPath(path) {
  return !!path && allDemoPaths().has(path);
}

async function applyPrepDemoDefaults(force = false) {
  if (!demoDefaults?.prep) return;
  if (!force && normalizeUploadedPath) return;
  normalizeUploadedPath = demoDefaults.prep.input_path;
  setFileStatus(normalizeInputStatus, normalizeUploadedPath, "shipped demo");
  if (!normalizeOutputName.value) {
    normalizeOutputName.value = defaultNormalizedOutputName(normalizeUploadedPath);
  }
  if (demoDefaults.prep.profile_name && normalizeProfileSelect.querySelector(`option[value="${demoDefaults.prep.profile_name}"]`)) {
    normalizeProfileSelect.value = demoDefaults.prep.profile_name;
    refreshNormalizationMeta();
  }
  await hydrateNormalizeSelection();
}

async function applyWorkflowDemoDefaults(force = false) {
  const defaults = defaultPathsForWorkflow(workflowSelect.value);
  if (!defaults) return;

  if (defaults.source_path) {
    if (force || !primaryUploadedPath || isDemoPath(primaryUploadedPath)) {
      primaryUploadedPath = defaults.source_path;
      setFileStatus(primaryFileStatus, primaryUploadedPath, "shipped demo");
      primaryState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
      await inspectInput("primary");
      applySuggestions("primary");
    }
    referenceUploadedPath = "";
    referenceFileStatus.textContent = "No file selected.";
    referenceState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
    renderMappingForm("reference");
    syncPreview();
    return;
  }

  if (defaults.primary_path && (force || !primaryUploadedPath || isDemoPath(primaryUploadedPath))) {
    primaryUploadedPath = defaults.primary_path;
    setFileStatus(primaryFileStatus, primaryUploadedPath, "shipped demo");
    primaryState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
    await inspectInput("primary");
    applySuggestions("primary");
  }
  if (defaults.reference_path && (force || !referenceUploadedPath || isDemoPath(referenceUploadedPath))) {
    referenceUploadedPath = defaults.reference_path;
    setFileStatus(referenceFileStatus, referenceUploadedPath, "shipped demo");
    referenceState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
    await inspectInput("reference");
    applySuggestions("reference");
  }
  syncPreview();
}

function renderSummary(summary) {
  summaryCards.innerHTML = "";
  summaryOutputs.innerHTML = "";
  summaryReasons.innerHTML = "";
  summaryMatchInputs.innerHTML = "";
  const previewRelevant = activeWorkflowSection === "match" && ["match_records_to_reference", "compare_records_to_reference", "identify_missing_records_from_system", "custom_job"].includes(summary?.workflow || workflowSelect.value);
  matchPreviewPanel.classList.toggle("hidden", !previewRelevant);
  if (!previewRelevant) {
    reviewPreview.innerHTML = "";
    unmatchedPreview.innerHTML = "";
  }
  if (!summary) return;

  Object.entries(summary.counts || {}).forEach(([key, value]) => {
    const card = document.createElement("div");
    card.className = "summary-card";
    card.innerHTML = `<span class="label">${key.replaceAll("_", " ")}</span><span class="value">${value}</span>`;
    summaryCards.appendChild(card);
  });

  Object.entries(summary.outputs || {})
    .filter(([key, value]) => key !== "run_summary" && !String(value || "").endsWith("/run_summary.json"))
    .forEach(([key, value]) => {
    const item = document.createElement("div");
    item.className = "summary-item";
    item.innerHTML = `<span>${key.replaceAll("_", " ")}</span><code>${value}</code>`;
    summaryOutputs.appendChild(item);
  });

  const reasons = Object.entries(summary.match_summary?.by_reason || {});
  if (!reasons.length) {
    const item = document.createElement("div");
    item.className = "summary-item";
    item.innerHTML = "<span>No match summary available</span><span></span>";
    summaryReasons.appendChild(item);
  } else {
    reasons.forEach(([key, value]) => {
      const item = document.createElement("div");
      item.className = "summary-item";
      item.innerHTML = `<span>${key}</span><strong>${value}</strong>`;
      summaryReasons.appendChild(item);
    });
  }

  Object.entries(summary.match_inputs || {}).forEach(([key, value]) => {
    const item = document.createElement("div");
    item.className = "summary-item";
    item.innerHTML = `<span>${key}</span><code>${formatMatchInputValue(value)}</code>`;
    summaryMatchInputs.appendChild(item);
  });
}

function renderTablePreview(container, payload, title) {
  container.innerHTML = "";

  if (!payload || payload.error) {
    container.innerHTML = `<div class="preview-empty">${payload?.error || `No ${title.toLowerCase()} preview available`}</div>`;
    return;
  }

  if (!payload.rows || !payload.rows.length) {
    container.innerHTML = `<div class="preview-empty">No ${title.toLowerCase()} rows available</div>`;
    return;
  }

  const table = document.createElement("table");
  table.className = "preview-table";
  if (title === "Review Records") {
    table.classList.add("review");
  }

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  const columns = [...payload.columns];
  if (title === "Review Records") {
    const priority = ["_match_score", "_match_reason", "_match_explanation"];
    columns.sort((left, right) => {
      const li = priority.indexOf(left);
      const ri = priority.indexOf(right);
      if (li === -1 && ri === -1) return 0;
      if (li === -1) return 1;
      if (ri === -1) return -1;
      return li - ri;
    });
  }
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  payload.rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      td.textContent = row[column] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.appendChild(table);
}

async function loadPreview(path, container, title) {
  if (!path) {
    renderTablePreview(container, null, title);
    return;
  }
  const payload = await fetchJson(`/api/preview-csv?path=${encodeURIComponent(path)}&limit=8`);
  renderTablePreview(container, payload, title);
}

function renderDownloadArea(payload) {
  if (!payload?.download_path) {
    normalizeDownloadArea.textContent = "Run normalization to generate a downloadable CSV.";
    return;
  }
  normalizeDownloadArea.innerHTML = `
    <div class="download-row">
      <span>Normalized file ready</span>
      <a class="download-link" href="${payload.download_path}">Download CSV</a>
    </div>
    <code>${payload.output_dir}/${payload.output_name}</code>
  `;
}

function addCustomProfileRule(rule = {}) {
  customProfileRuleCount += 1;
  const row = document.createElement("div");
  row.className = "custom-rule-row";
  row.dataset.ruleId = String(customProfileRuleCount);
  row.innerHTML = `
    <input class="rule-target" type="text" placeholder="target field" value="${rule.target || customProfileTarget.value || ""}">
    <select class="rule-strategy">
      <option value="copy" ${rule.strategy === "copy" ? "selected" : ""}>copy</option>
      <option value="join" ${rule.strategy === "join" ? "selected" : ""}>join</option>
      <option value="coalesce" ${rule.strategy === "coalesce" ? "selected" : ""}>coalesce</option>
    </select>
    <input class="rule-columns" type="text" placeholder="source columns, comma separated" value="${(rule.columns || []).join(", ")}">
    <input class="rule-separator" type="text" placeholder="separator" value="${rule.separator || " "}">
    <button class="ghost small rule-remove" type="button">Remove</button>
  `;
  row.querySelector(".rule-remove").addEventListener("click", () => {
    row.remove();
    updateCustomProfilePreview();
  });
  row.querySelectorAll("input, select").forEach((el) => {
    el.addEventListener("input", updateCustomProfilePreview);
    el.addEventListener("change", updateCustomProfilePreview);
  });
  customProfileRows.appendChild(row);
  updateCustomProfilePreview();
}

function collectCustomProfileDefinition() {
  const derive = {};
  customProfileRows.querySelectorAll(".custom-rule-row").forEach((row) => {
    const target = row.querySelector(".rule-target").value.trim();
    const strategy = row.querySelector(".rule-strategy").value;
    const columns = row.querySelector(".rule-columns").value
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
    const separator = row.querySelector(".rule-separator").value;

    if (!target || !columns.length) return;
    if (strategy === "copy") {
      derive[target] = { strategy, source: columns[0] };
    } else {
      derive[target] = { strategy, columns };
      if (strategy === "join") {
        derive[target].separator = separator;
      }
    }
  });
  return { derive };
}

function updateCustomProfilePreview() {
  customProfilePreview.textContent = pretty(collectCustomProfileDefinition());
}

async function showWorkflow() {
  const workflow = workflowSelect.value;
  if (!workflow) {
    currentWorkflowMeta = null;
    workflowMeta.textContent = "";
    workflowDescription.innerHTML = "";
    matchContract.innerHTML = "";
    syncPreview();
    return;
  }
  currentWorkflowMeta = await fetchJson(`/api/workflows/${workflow}`);
  if (!outputPath.value.trim() || outputPath.value.includes("/absolute/path/to/output")) {
    outputPath.value = defaultOutputDirectory();
  }
  workflowMeta.textContent = pretty(currentWorkflowMeta);
  const description = currentWorkflowMeta.description || {};
  const topItems = (items, limit = 3) => (items || []).filter(Boolean).slice(0, limit);
  const renderInputRoles = () => {
    const inputs = currentWorkflowMeta.inputs || [];
    if (!inputs.length) {
      return `<div class="workflow-empty">No fixed input roles. This workflow is driven by the stage plan.</div>`;
    }
    return `<div class="workflow-inline-list">${inputs.map((input) => `<span class="workflow-pill">${input}</span>`).join("")}</div>`;
  };
  workflowDescription.innerHTML = `
    <div class="workflow-detail-head">
      <div>
        <p class="workflow-kicker">Selected workflow</p>
        <h3>${currentWorkflowMeta.label}</h3>
      </div>
      <div class="workflow-detail-callout">
        <strong>Best for</strong>
        <p>${description.best_for || "No recommendation available yet."}</p>
      </div>
    </div>
    <div class="workflow-detail-summary">
      <p>${description.summary || "No workflow description available."}</p>
      ${description.operator_goal ? `<p><strong>Operator goal:</strong> ${description.operator_goal}</p>` : ""}
    </div>
    <div class="workflow-detail-grid">
      <section class="workflow-detail-section">
        <h4>Input roles</h4>
        ${renderInputRoles()}
      </section>
      <section class="workflow-detail-section">
        <h4>What it does</h4>
        <ul>${topItems(description.does, 3).map((item) => `<li>${item}</li>`).join("")}</ul>
      </section>
      <section class="workflow-detail-section">
        <h4>Typical steps</h4>
        <ul>${topItems(description.step_by_step, 3).map((item) => `<li>${item}</li>`).join("")}</ul>
      </section>
      <section class="workflow-detail-section">
        <h4>Outputs</h4>
        <ul>${topItems(description.outputs_detail, 3).map((item) => `<li>${item}</li>`).join("")}</ul>
      </section>
      <section class="workflow-detail-section workflow-detail-warning">
        <h4>Watch for</h4>
        <ul>${topItems(description.watch_for, 2).map((item) => `<li>${item}</li>`).join("")}</ul>
      </section>
    </div>
  `;
  const matchInputs = currentWorkflowMeta.match_inputs || {};
  matchContract.innerHTML = `
    <strong>Matching uses:</strong>
    identity ${formatMatchInputValue(matchInputs.identity || [])},
    address ${formatMatchInputValue(matchInputs.address || [])},
    contact ${formatMatchInputValue(matchInputs.contact || [])}.
    Map these if you want them to influence the matcher.
  `;
  const templateName = templateForWorkflow(workflow);
  if (builderTemplateSelect && templateName) {
    builderTemplateSelect.value = templateName;
  }
  if (workflow === "custom_job" && !customStagePlan.length) {
    customStagePlan = buildTemplatePlan(templateName || "match_template");
  } else if (workflow !== "custom_job") {
    customStagePlan = [];
  }
  ensureCustomStagePlan();
  renderCustomStagePlan();
  renderMappingForm("primary");
  renderMappingForm("reference");
  await applyWorkflowDemoDefaults(false);
  syncPreview();
}

async function inspectInput(inputName) {
  const actualPath = inputName === "primary" ? primaryUploadedPath : referenceUploadedPath;
  const headersEl = inputName === "primary" ? primaryHeaders : referenceHeaders;
  const state = inputName === "primary" ? primaryState : referenceState;
  if (!actualPath) {
    appendInlineError(inputName === "primary" ? primaryPathErrors : referencePathErrors, "Choose and upload a CSV first.");
    return;
  }
  const payload = await fetchJson(`/api/suggest-mapping?path=${encodeURIComponent(actualPath)}`);
  state.headers = payload.headers;
  state.suggestions = payload.suggestions;
  state.groups = payload.groups || {};
  state.mapping = { ...payload.suggestions, ...state.mapping };
  headersEl.textContent = pretty(payload.headers);
  renderHeaderGroups(inputName);
  renderMappingForm(inputName);
  syncPreview();
}

async function inspectNormalizeInput() {
  clearInlineErrors();
  if (!normalizeUploadedPath) {
    appendInlineError(normalizePathErrors, "Normalization source path is required.");
    return;
  }
  const payload = await fetchJson(`/api/suggest-mapping?path=${encodeURIComponent(normalizeUploadedPath)}`);
  normalizeState.headers = payload.headers;
  normalizeState.suggestions = payload.suggestions;
  normalizeState.groups = payload.groups || {};
  normalizeState.mapping = { ...payload.suggestions, ...normalizeState.mapping };
  normalizeHeaders.textContent = pretty(payload.headers);
  renderNormalizeMappingForm();
}

async function hydrateNormalizeSelection() {
  if (!normalizeUploadedPath) return;
  await inspectNormalizeInput();
  applyNormalizeSuggestions();
}

function applySuggestions(inputName) {
  const state = inputName === "primary" ? primaryState : referenceState;
  state.mapping = { ...state.suggestions };
  renderMappingForm(inputName);
  syncPreview();
}

function applyNormalizeSuggestions() {
  normalizeState.mapping = { ...normalizeState.suggestions };
  renderNormalizeMappingForm();
}

async function validateJob() {
  const payload = buildJobSpecObject();
  const result = await fetchJson("/api/validate-job", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  syncPreview();
  resultBox.textContent = pretty(result);
  renderValidationErrors(result.errors || []);
  renderSummary(null);
}

async function runJob() {
  const payload = buildJobSpecObject();
  startProgress("match");
  try {
    const started = await fetchJson("/api/run-job-async", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    activeMatchJobId = started.job_id || "";
    syncPreview();
    resultBox.textContent = pretty(started);
    const result = await pollMatchJob(activeMatchJobId);
    activeMatchJobId = "";
    resultBox.textContent = pretty(result);
    renderValidationErrors([]);
    renderSummary(result.summary || null);
    const outputs = result.summary?.outputs || {};
    await Promise.all([
      loadPreview(outputs.review_records, reviewPreview, "Review Records"),
      loadPreview(outputs.unmatched_records, unmatchedPreview, "Unmatched Records")
    ]);
    await loadInventory();
    finishProgress("match", "Match run complete");
  } catch (error) {
    activeMatchJobId = "";
    const message = String(error.message || error);
    resultBox.textContent = message;
    try {
      const parsed = JSON.parse(message);
      renderValidationErrors(parsed.errors || []);
    } catch {
      renderValidationErrors([message]);
    }
    finishProgress("match", "Match run failed");
    return;
  }
}

async function pollMatchJob(jobId) {
  if (!jobId) {
    throw new Error("Missing async job id");
  }
  for (;;) {
    const payload = await fetchJson(`/api/job-status?id=${encodeURIComponent(jobId)}`);
    const status = payload.status || "unknown";
    if (status === "completed") {
      return payload.result || {};
    }
    if (status === "failed") {
      throw new Error((payload.errors || ["Job failed"]).join("; "));
    }
    if (status === "queued") {
      matchProgressLabel.textContent = "Queued...";
      matchProgressValue.textContent = "Waiting";
    } else if (status === "running") {
      matchProgressLabel.textContent = "Running job on server...";
      matchProgressValue.textContent = "Working";
    }
    await new Promise((resolve) => setTimeout(resolve, 1200));
  }
}

async function runNormalization() {
  clearInlineErrors();
  if (!normalizeUploadedPath) {
    appendInlineError(normalizePathErrors, "Normalization source path is required.");
    return;
  }

  try {
    startProgress("normalize");
    const result = await fetchJson("/api/run-normalization", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        input_path: normalizeUploadedPath,
        profile_name: normalizeProfileSelect.value,
        output_name: normalizeOutputName.value.trim(),
        strict_text_cleanup: normalizeStrictCleanup.checked,
        columns: normalizeState.mapping
      })
    });
    normalizeResultBox.textContent = pretty(result);
    renderDownloadArea(result);
    renderTablePreview(normalizedPreview, result.preview, "Normalized Output");
    await loadInventory();
    finishProgress("normalize", "Normalization complete");
  } catch (error) {
    normalizeResultBox.textContent = String(error.message || error);
    try {
      const parsed = JSON.parse(normalizeResultBox.textContent);
      (parsed.errors || []).forEach((message) => appendInlineError(normalizePathErrors, message));
    } catch {
      appendInlineError(normalizePathErrors, normalizeResultBox.textContent);
    }
    finishProgress("normalize", "Normalization failed");
  }
}

async function loadSelectedPreset() {
  if (!presetSelect.value) return;
  const preset = await fetchJson(`/api/presets/${encodeURIComponent(presetSelect.value)}`);
  applyPresetToForm(preset);
  await showWorkflow();
}

async function saveCurrentPreset() {
  const name = presetName.value.trim();
  if (!name) {
    appendInlineError(workflowErrors, "Preset name is required.");
    resultBox.textContent = "Preset name is required.";
    return;
  }
  const result = await fetchJson("/api/save-preset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, preset: buildJobSpecObject() })
  });
  resultBox.textContent = pretty(result);
  await loadPresets();
  presetSelect.value = result.preset.name;
}

async function saveCustomProfile() {
  clearInlineErrors();
  const name = customProfileName.value.trim();
  const profile = collectCustomProfileDefinition();
  if (!name) {
    appendInlineError(customProfileErrors, "Custom normalization profile name is required.");
    return;
  }
  if (!Object.keys(profile.derive || {}).length) {
    appendInlineError(customProfileErrors, "Add at least one normalization rule before saving.");
    return;
  }

  try {
    const result = await fetchJson("/api/save-normalization-profile", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, profile })
    });
    customProfilePreview.textContent = pretty(result);
    await loadNormalizationProfiles();
    normalizeProfileSelect.value = result.profile.name;
    refreshNormalizationMeta();
  } catch (error) {
    try {
      const parsed = JSON.parse(String(error.message || error));
      (parsed.errors || []).forEach((message) => appendInlineError(customProfileErrors, message));
    } catch {
      appendInlineError(customProfileErrors, String(error.message || error));
    }
  }
}

tabNormalize.addEventListener("click", () => setTab("normalize"));
tabMatch.addEventListener("click", () => setTab("match"));
tabUtilities.addEventListener("click", () => setTab("utilities"));

document.getElementById("refreshWorkflows").addEventListener("click", loadWorkflows);
document.getElementById("syncPreview").addEventListener("click", syncPreview);
document.getElementById("inspectPrimary").addEventListener("click", () => inspectInput("primary"));
document.getElementById("inspectReference").addEventListener("click", () => inspectInput("reference"));
document.getElementById("applyPrimarySuggestions").addEventListener("click", () => applySuggestions("primary"));
document.getElementById("applyReferenceSuggestions").addEventListener("click", () => applySuggestions("reference"));
document.getElementById("validateJob").addEventListener("click", validateJob);
document.getElementById("runJob").addEventListener("click", runJob);
document.getElementById("loadPreset").addEventListener("click", loadSelectedPreset);
document.getElementById("savePreset").addEventListener("click", saveCurrentPreset);
document.getElementById("inspectNormalize").addEventListener("click", inspectNormalizeInput);
document.getElementById("applyNormalizeSuggestions").addEventListener("click", applyNormalizeSuggestions);
document.getElementById("runNormalize").addEventListener("click", runNormalization);
document.getElementById("addProfileRule").addEventListener("click", () => addCustomProfileRule());
document.getElementById("saveCustomProfile").addEventListener("click", saveCustomProfile);
deleteAllOutputsButton.addEventListener("click", async () => {
  try {
    await deleteAllOutputs();
  } catch (error) {
    resultBox.textContent = String(error.message || error);
  }
});
document.getElementById("resetCustomStages").addEventListener("click", () => {
  customStagePlan = defaultCustomStagePlan();
  renderCustomStagePlan();
  syncPreview();
});
applyBuilderTemplate?.addEventListener("click", () => {
  const templateName = builderTemplateSelect?.value || "match_template";
  customStagePlan = buildTemplatePlan(templateName);
  renderCustomStagePlan();
  syncPreview();
});
builderTemplateSelect?.addEventListener("change", () => {
  const templateName = builderTemplateSelect?.value || "match_template";
  customStagePlan = buildTemplatePlan(templateName);
  renderCustomStagePlan();
  syncPreview();
});
customStageZoom?.addEventListener("input", () => {
  setCustomStageZoom(Number(customStageZoom.value || 100));
});
zoomOutStages?.addEventListener("click", () => {
  setCustomStageZoom(customStageZoomLevel - 10);
});
zoomInStages?.addEventListener("click", () => {
  setCustomStageZoom(customStageZoomLevel + 10);
  syncPreview();
});
openMatchHelp.addEventListener("click", () => setMatchHelpOpen(true));
closeMatchHelp.addEventListener("click", () => setMatchHelpOpen(false));
matchHelpModal.addEventListener("click", (event) => {
  const target = event.target;
  if (target instanceof HTMLElement && target.dataset.closeMatchHelp === "true") {
    setMatchHelpOpen(false);
  }
});
closeDangerModal.addEventListener("click", () => setDangerModalOpen(false));
cancelDangerModal.addEventListener("click", () => setDangerModalOpen(false));
confirmDangerModal.addEventListener("click", async () => {
  if (!pendingDangerAction) {
    setDangerModalOpen(false);
    return;
  }
  const action = pendingDangerAction;
  setDangerModalOpen(false);
  try {
    await action();
  } catch (error) {
    resultBox.textContent = String(error.message || error);
  }
});
dangerModal.addEventListener("click", (event) => {
  const target = event.target;
  if (target instanceof HTMLElement && target.dataset.closeDangerModal === "true") {
    setDangerModalOpen(false);
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    setMatchHelpOpen(false);
    setDangerModalOpen(false);
  }
});
workflowSelect.addEventListener("change", showWorkflow);
outputPath.addEventListener("change", syncPreview);
strictMatchMode.addEventListener("change", () => {
  renderActiveMatchPreset();
  syncPreview();
});
confidentThreshold.addEventListener("change", () => {
  renderActiveMatchPreset();
  syncPreview();
});
possibleThreshold.addEventListener("change", () => {
  renderActiveMatchPreset();
  syncPreview();
});
reviewThreshold.addEventListener("change", () => {
  renderActiveMatchPreset();
  syncPreview();
});
presetConservative.addEventListener("click", () => applyMatchPreset("conservative"));
presetBalanced.addEventListener("click", () => applyMatchPreset("balanced"));
presetAggressive.addEventListener("click", () => applyMatchPreset("aggressive"));
normalizeProfileSelect.addEventListener("change", refreshNormalizationMeta);
normalizeInputFile.addEventListener("change", async () => {
  const file = normalizeInputFile.files?.[0];
  if (!file) {
    normalizeUploadedPath = "";
    normalizeInputStatus.textContent = "No file selected.";
    return;
  }
  normalizeUploadedPath = await uploadSelectedFile(file, normalizeInputStatus);
  if (!normalizeOutputName.value) {
    normalizeOutputName.value = defaultNormalizedOutputName(normalizeUploadedPath);
  }
  normalizeState = { headers: [], suggestions: {}, mapping: {}, groups: {} };
  normalizeHeaders.textContent = "";
  renderNormalizeMappingForm();
  await hydrateNormalizeSelection();
});
primaryFile.addEventListener("change", async () => {
  const file = primaryFile.files?.[0];
  if (!file) {
    primaryUploadedPath = "";
    primaryFileStatus.textContent = "No file selected.";
    syncPreview();
    return;
  }
  primaryUploadedPath = await uploadSelectedFile(file, primaryFileStatus);
  syncPreview();
});
referenceFile.addEventListener("change", async () => {
  const file = referenceFile.files?.[0];
  if (!file) {
    referenceUploadedPath = "";
    referenceFileStatus.textContent = "No file selected.";
    syncPreview();
    return;
  }
  referenceUploadedPath = await uploadSelectedFile(file, referenceFileStatus);
  syncPreview();
});

addCustomProfileRule();

try {
  const savedTab = localStorage.getItem(ACTIVE_TAB_STORAGE_KEY);
  if (savedTab === "match" || savedTab === "normalize" || savedTab === "utilities") {
    setTab(savedTab);
  }
} catch {
  // Ignore storage failures and fall back to the default markup state.
}

Promise.all([loadNormalizationProfiles(), loadPresets(), loadStages(), loadDemoDefaults(), loadWorkflows()])
  .then(() => {
    return loadInventory();
  })
  .then(async () => {
    await applyPrepDemoDefaults(false);
    await applyWorkflowDemoDefaults(false);
    renderActiveMatchPreset();
    renderNormalizeMappingForm();
    updateCustomProfilePreview();
    syncPreview();
  })
  .catch((error) => {
    resultBox.textContent = error.message;
    normalizeResultBox.textContent = error.message;
  });
