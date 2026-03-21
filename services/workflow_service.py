from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path
import re

from core.canonical import CANONICAL_FIELDS, FIELD_ALIASES
from core.jobs import build_runtime_config, load_job, required_inputs_for_workflow
from core.workflows import (
    WORKFLOW_CATEGORIES,
    WORKFLOW_DESCRIPTIONS,
    WORKFLOW_LABELS,
    WORKFLOW_REQUIRED_FIELDS,
    WORKFLOW_RECOMMENDED_FIELDS,
    resolve_workflow_name,
)
from dataio.csv import read_csv_headers


def list_workflows() -> list[dict]:
    workflows = []
    for workflow_name, label in WORKFLOW_LABELS.items():
        workflows.append(
            {
                "workflow": workflow_name,
                "label": label,
                "category": WORKFLOW_CATEGORIES.get(workflow_name, "match"),
                "description": WORKFLOW_DESCRIPTIONS.get(workflow_name, {}),
                "inputs": required_inputs_for_workflow(workflow_name),
                "required_fields": WORKFLOW_REQUIRED_FIELDS.get(workflow_name, {}),
                "recommended_fields": WORKFLOW_RECOMMENDED_FIELDS.get(workflow_name, {}),
            }
        )
    return workflows


def describe_workflow(workflow_name: str) -> dict:
    resolved = resolve_workflow_name(workflow_name)
    if resolved not in WORKFLOW_LABELS:
        raise ValueError(f"Unsupported workflow: {workflow_name}")
    return {
        "workflow": resolved,
        "label": WORKFLOW_LABELS[resolved],
        "category": WORKFLOW_CATEGORIES.get(resolved, "match"),
        "description": WORKFLOW_DESCRIPTIONS.get(resolved, {}),
        "inputs": required_inputs_for_workflow(resolved),
        "required_fields": WORKFLOW_REQUIRED_FIELDS.get(resolved, {}),
        "recommended_fields": WORKFLOW_RECOMMENDED_FIELDS.get(resolved, {}),
        "match_inputs": {
            "identity": ["person_id", "first_name", "last_name"],
            "address": ["primary_address1", "primary_address2", "mail_city", "mail_state", "mail_zip", "primary_city", "primary_state", "primary_zip"],
            "contact": ["email", "phone"],
        },
    }


def inspect_headers(csv_path: str | Path) -> list[str]:
    return read_csv_headers(str(csv_path))


def _normalize_header(value: str) -> str:
    return "".join(ch for ch in value.strip().lower() if ch.isalnum())


def _is_plausible_mapping(canonical_field: str, header: str) -> bool:
    if canonical_field != "membership_end_date":
        return True

    tokens = set(_tokenize_header(header))
    compact = _normalize_header(header)
    expiry_like = (
        "expiry" in tokens
        or "expires" in tokens
        or "expiration" in tokens
        or "exp" in tokens
        or ("end" in tokens and "date" in tokens)
        or compact in {"membershipenddate", "membexpdate", "expirydate", "expirationdate"}
    )
    if expiry_like:
        return True
    if "type" in tokens:
        return False
    return False


def suggest_mappings(headers: list[str]) -> dict[str, str]:
    suggestions: dict[str, str] = {}
    normalized_headers = {_normalize_header(header): header for header in headers}

    for canonical_field in CANONICAL_FIELDS:
        aliases = [_normalize_header(canonical_field)] + [_normalize_header(alias) for alias in FIELD_ALIASES.get(canonical_field, [])]

        chosen = ""
        for alias in aliases:
            if alias in normalized_headers:
                candidate = normalized_headers[alias]
                if _is_plausible_mapping(canonical_field, candidate):
                    chosen = candidate
                    break

        if not chosen:
            best_ratio = 0.0
            best_header = ""
            for header in headers:
                if not _is_plausible_mapping(canonical_field, header):
                    continue
                ratio = SequenceMatcher(None, _normalize_header(header), _normalize_header(canonical_field)).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_header = header
            if best_ratio >= 0.72:
                chosen = best_header

        if chosen:
            suggestions[canonical_field] = chosen

    return suggestions


def _tokenize_header(value: str) -> list[str]:
    prepared = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(value))
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", prepared.lower()) if part]
    tokens: list[str] = []
    for part in parts:
        tokens.append(part)
        tokens.extend(piece for piece in re.findall(r"[a-z]+|\d+", part) if piece != part)
    return list(dict.fromkeys(tokens))


def _header_matches_any(header: str, aliases: list[str]) -> bool:
    tokens = _tokenize_header(header)
    compact = _normalize_header(header)
    for alias in aliases:
        alias_tokens = _tokenize_header(alias)
        alias_compact = _normalize_header(alias)
        if alias_compact and alias_compact == compact:
            return True
        if alias_tokens and all(token in tokens for token in alias_tokens):
            return True
    return False


def classify_headers(headers: list[str]) -> dict[str, list[str]]:
    alias_groups = {
        "identity": ["id", "externalid", "constituentid", "constituantid", "personid", "firstname", "middlename", "lastname", "surname", "familyname"],
        "email": ["email", "emailaddress"],
        "phone": ["phone", "mobile", "cell", "fax"],
        "address": ["address", "street", "city", "state", "province", "zip", "postal", "unit", "suite"],
        "dates": ["date", "created", "expiry", "exp", "start", "end", "born", "donationdate"],
        "money": ["amount", "donation", "donations", "value", "revenue", "receiptable"],
    }
    groups = {name: [] for name in [*alias_groups.keys(), "other"]}
    for header in headers:
        assigned = False
        for group_name, aliases in alias_groups.items():
            if _header_matches_any(header, aliases):
                groups[group_name].append(header)
                assigned = True
                break
        if not assigned:
            groups["other"].append(header)
    return {key: value for key, value in groups.items() if value}


def validate_runtime_config(config: dict) -> list[str]:
    errors: list[str] = []
    workflow = resolve_workflow_name(config.get("profile", ""))
    stage_sequence = config.get("stage_sequence", [])

    if workflow not in WORKFLOW_LABELS:
        return [f"Unsupported workflow: {workflow}"]

    inputs = config.get("inputs", {})
    required_inputs = required_inputs_for_workflow(workflow)
    if workflow == "custom_job" and inputs:
        required_inputs = list(inputs.keys())
    required_fields = WORKFLOW_REQUIRED_FIELDS.get(workflow, {})
    outputs = config.get("outputs", {})

    if workflow == "custom_job" and not stage_sequence:
        errors.append("Custom jobs must define a non-empty 'stage_sequence'")

    for input_name in required_inputs:
        dataset_cfg = inputs.get(input_name)
        if not dataset_cfg:
            errors.append(f"Missing required input: {input_name}")
            continue

        if "path" not in dataset_cfg and "paths" not in dataset_cfg:
            errors.append(f"Input '{input_name}' must define 'path' or 'paths'")

        columns = dataset_cfg.get("columns", {})
        missing_fields = [field for field in required_fields.get(input_name, []) if field not in columns]
        if missing_fields:
            errors.append(
                f"Input '{input_name}' is missing canonical column mappings for: {', '.join(missing_fields)}"
            )

        dataset_paths = []
        if "path" in dataset_cfg:
            dataset_paths.append(dataset_cfg["path"])
        if "paths" in dataset_cfg:
            dataset_paths.extend(dataset_cfg["paths"])

        for dataset_path in dataset_paths:
            path_obj = Path(dataset_path)
            if not path_obj.exists():
                errors.append(f"Input '{input_name}' path does not exist: {dataset_path}")
                continue

            headers = read_csv_headers(str(path_obj))
            missing_source_columns = []
            for source in columns.values():
                candidates = source if isinstance(source, list) else [source]
                for candidate in candidates:
                    if candidate and candidate not in headers:
                        missing_source_columns.append(candidate)
            if missing_source_columns:
                errors.append(
                    f"Input '{input_name}' file '{dataset_path}' is missing mapped source columns: {', '.join(missing_source_columns)}"
                )

    records_output = outputs.get("records", {})
    base_dir = records_output.get("base_dir", "")
    if workflow in {"compare_records_to_reference", "match_records_to_reference"}:
        if not base_dir:
            errors.append("Output directory is required for this workflow")
        elif base_dir.startswith("/absolute/path/to/"):
            errors.append("Output directory must be set to a real writable folder")

    if workflow == "custom_job":
        if not isinstance(stage_sequence, list):
            errors.append("'stage_sequence' must be a list")
        elif not stage_sequence:
            errors.append("'stage_sequence' must not be empty")

    return errors


def validate_job_file(job_path: str | Path, *, root_dir: str | Path) -> tuple[dict, list[str]]:
    job = load_job(job_path)
    runtime = build_runtime_config(job, root_dir=root_dir)
    errors = validate_runtime_config(runtime)
    return runtime, errors
