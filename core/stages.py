from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from classify.address_status import classify_address_status
from dataio.csv import write_csv
from enrich.contacts import enrich_contacts
from enrich.from_voters import enrich_from_voters
from export.records import project_records
from normalize.address import normalize_address_parts
from normalize.address_split import split_unit_and_street
from normalize.dates import fix_date_columns
from reconcile.reference_identity import flag_identity_matches
from reconcile.generic import match_primary_to_reference
from score.priority import score_priority


StageContext = dict
StageRunner = Callable[[StageContext, dict], StageContext]


@dataclass(frozen=True)
class StageDefinition:
    name: str
    label: str
    required_inputs: tuple[str, ...]
    summary: str
    operator_goal: str
    inputs_detail: tuple[str, ...]
    effects: tuple[str, ...]
    watch_for: tuple[str, ...]
    runner: StageRunner


def _dataset_or_error(context: StageContext, role: str):
    datasets = context.setdefault("datasets", {})
    if role not in datasets:
        raise KeyError(f"Missing dataset role for stage execution: {role}")
    return datasets[role]


def _ensure_context(context: StageContext) -> StageContext:
    context.setdefault("datasets", {})
    context.setdefault("artifacts", {})
    context.setdefault("stats", {})
    context.setdefault("warnings", [])
    context.setdefault("outputs", {})
    return context


def run_join_reference_fields(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    primary_role = config.get("primary_role", "primary")
    reference_role = config.get("reference_role", "reference")
    join_on = config["join_on"]
    take_columns = config["take_columns"]

    primary = _dataset_or_error(context, primary_role)
    reference = _dataset_or_error(context, reference_role)

    enriched = enrich_from_voters(primary, reference, join_on=join_on, take_columns=take_columns)
    context["datasets"][primary_role] = enriched
    context["stats"]["join_reference_fields"] = {
        "primary_role": primary_role,
        "reference_role": reference_role,
        "join_on": join_on,
        "take_columns": take_columns,
        "row_count": len(enriched),
    }
    return context


def run_aggregate_contacts(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "primary")
    df = _dataset_or_error(context, dataset_role)
    enriched = enrich_contacts(df)
    context["datasets"][dataset_role] = enriched
    context["stats"]["aggregate_contacts"] = {"dataset_role": dataset_role, "row_count": len(enriched)}
    return context


def run_classify_address_status(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "primary")
    df = _dataset_or_error(context, dataset_role)
    classified = classify_address_status(df)
    context["datasets"][dataset_role] = classified
    context["stats"]["classify_address_status"] = {"dataset_role": dataset_role, "row_count": len(classified)}
    return context


def run_score_priority(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "primary")
    df = _dataset_or_error(context, dataset_role)
    scored = score_priority(df)
    context["datasets"][dataset_role] = scored
    context["stats"]["score_priority"] = {"dataset_role": dataset_role, "row_count": len(scored)}
    return context


def run_project_records(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    source_role = config.get("source_role", "primary")
    artifact_name = config.get("artifact_name", f"{source_role}_projected")
    df = _dataset_or_error(context, source_role)
    projected = project_records(df)
    context["artifacts"][artifact_name] = projected
    context["stats"]["project_records"] = {
        "source_role": source_role,
        "artifact_name": artifact_name,
        "row_count": len(projected),
    }
    return context


def run_write_records_output(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    source_kind = config.get("source_kind", "artifact")
    source_name = config["source_name"]
    output_key = config.get("output_key", "records")
    base_dir = Path(config["base_dir"])
    filename = config["filename"]

    if source_kind == "artifact":
        frame = context["artifacts"][source_name]
    elif source_kind == "dataset":
        frame = context["datasets"][source_name]
    else:
        raise ValueError(f"Unsupported output source kind: {source_kind}")

    output_path = base_dir / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_csv(frame, str(output_path))
    context["outputs"][output_key] = str(output_path)
    context["stats"]["write_records_output"] = {
        "output_key": output_key,
        "path": str(output_path),
        "row_count": len(frame),
    }
    return context


def run_split_rows(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "source")
    method = config.get("method", "alternate")
    start_file = str(config.get("start_file", "A")).upper()

    df = _dataset_or_error(context, dataset_role)
    if df.empty:
        raise ValueError("Input file is empty")
    if method != "alternate":
        raise ValueError(f"Unsupported split method: {method}")

    first_slice = df.iloc[::2].copy()
    second_slice = df.iloc[1::2].copy()
    if start_file == "B":
        first_slice, second_slice = second_slice, first_slice

    context["artifacts"]["split_a"] = first_slice
    context["artifacts"]["split_b"] = second_slice
    context["stats"]["split_rows"] = {
        "dataset_role": dataset_role,
        "method": method,
        "start_file": start_file,
        "rows_a": len(first_slice),
        "rows_b": len(second_slice),
    }
    return context


def run_extract_projection(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "source")
    projection_cols = config["columns"]

    df = _dataset_or_error(context, dataset_role).copy()
    stats: dict[str, int] = {"rows_loaded": len(df), "rows_removed": 0}

    if config.get("format_created_at") and "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce").dt.strftime("%m/%d/%Y")

    if config.get("format_amount") and "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        before = len(df)
        if config.get("drop_non_positive_amounts"):
            df = df[df["amount"] > 0].copy()
        stats["rows_removed"] = before - len(df)
        df["amount"] = df["amount"].apply(lambda value: f"${value:,.2f}" if pd.notna(value) else "")

    missing = [column for column in projection_cols if column not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for projection: {missing}")

    projected = df[projection_cols].copy()
    artifact_name = config.get("artifact_name", "projection")
    context["artifacts"][artifact_name] = projected
    context["stats"]["extract_projection"] = {
        "dataset_role": dataset_role,
        "artifact_name": artifact_name,
        "rows_loaded": stats["rows_loaded"],
        "rows_removed": stats["rows_removed"],
        "rows_projected": len(projected),
    }
    return context


def run_normalize_addresses(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "primary")
    mode = config.get("mode", "member")
    address1_col = config["address1_col"]
    address2_col = config.get("address2_col", "")
    city_col = config["city_col"]
    state_col = config["state_col"]
    zip_col = config["zip_col"]

    df = _dataset_or_error(context, dataset_role).copy()

    if address1_col in df.columns:
        split_results = df[address1_col].apply(split_unit_and_street)
        df[address1_col] = split_results.apply(lambda value: value[0])
        if address2_col:
            split_units = split_results.apply(lambda value: value[1])
            if mode == "voter" and address2_col in df.columns:
                existing = df[address2_col].fillna("").astype(str).str.strip()
                df[address2_col] = existing.where(existing != "", split_units)
            else:
                df[address2_col] = split_units
        df["_address_split_status"] = split_results.apply(lambda value: value[2])

    df["_address_norm"] = df.apply(
        lambda row: normalize_address_parts(
            row.get(address1_col),
            row.get(city_col),
            row.get(state_col),
            row.get(zip_col),
            row.get(address2_col) if address2_col else None,
        ),
        axis=1,
    )

    context["datasets"][dataset_role] = df
    context["stats"]["normalize_addresses"] = {
        "dataset_role": dataset_role,
        "mode": mode,
        "address1_col": address1_col,
        "address2_col": address2_col,
        "city_col": city_col,
        "state_col": state_col,
        "zip_col": zip_col,
        "row_count": len(df),
    }
    return context


def run_dedupe_records(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "primary")
    df = _dataset_or_error(context, dataset_role).copy()

    dedupe_keys = list(config.get("subset", []))
    if not dedupe_keys:
        if "person_id" in df.columns:
            dedupe_keys.append("person_id")
        for column in ["first_name", "last_name", "_address_norm"]:
            if column in df.columns and column not in dedupe_keys:
                dedupe_keys.append(column)

    if not dedupe_keys:
        context["stats"]["dedupe_records"] = {
            "dataset_role": dataset_role,
            "dedupe_keys": [],
            "rows_before": len(df),
            "rows_after": len(df),
            "rows_removed": 0,
        }
        return context

    deduped = df.drop_duplicates(subset=dedupe_keys, keep=config.get("keep", "first")).copy()
    context["datasets"][dataset_role] = deduped
    context["stats"]["dedupe_records"] = {
        "dataset_role": dataset_role,
        "dedupe_keys": dedupe_keys,
        "rows_before": len(df),
        "rows_after": len(deduped),
        "rows_removed": len(df) - len(deduped),
    }
    return context


def run_normalize_date_columns(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "primary")
    columns = config["columns"]

    df = _dataset_or_error(context, dataset_role).copy()
    normalized = fix_date_columns(df, columns)
    context["datasets"][dataset_role] = normalized
    context["stats"]["normalize_date_columns"] = {
        "dataset_role": dataset_role,
        "columns": [column for column in columns if column in normalized.columns],
        "row_count": len(normalized),
    }
    return context


def run_match_records(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    primary_role = config.get("primary_role", "primary")
    reference_role = config.get("reference_role", "reference")
    output_role = config.get("output_role", primary_role)

    primary = _dataset_or_error(context, primary_role)
    reference = _dataset_or_error(context, reference_role)

    matched = match_primary_to_reference(primary, reference, **config.get("match_config", {}))
    mirror_matched_to = config.get("mirror_matched_to")
    if mirror_matched_to and "_matched_to_reference" in matched.columns:
        matched[mirror_matched_to] = matched["_matched_to_reference"]
    context["datasets"][output_role] = matched
    status_counts = matched["_match_status"].value_counts(dropna=False).to_dict() if "_match_status" in matched.columns else {}
    reason_counts = matched["_match_reason"].value_counts(dropna=False).to_dict() if "_match_reason" in matched.columns else {}
    context["stats"]["match_records"] = {
        "primary_role": primary_role,
        "reference_role": reference_role,
        "output_role": output_role,
        "row_count": len(matched),
        "by_status": status_counts,
        "by_reason": reason_counts,
    }
    return context


def run_flag_reference_identity(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    primary_role = config.get("primary_role", "primary")
    reference_role = config.get("reference_role", "reference")
    output_role = config.get("output_role", primary_role)

    primary = _dataset_or_error(context, primary_role)
    reference = _dataset_or_error(context, reference_role)

    flagged = flag_identity_matches(
        primary,
        reference,
        id_col=config.get("id_col", "person_id"),
        first_col=config.get("first_col", "first_name"),
        last_col=config.get("last_col", "last_name"),
        address_col=config.get("address_col", "_address_norm"),
    )

    if config.get("set_match_fields", True):
        exists = flagged["_exists_by_external_id"]
        flagged["_match_status"] = exists.map(lambda value: "CONFIDENT" if value else "UNMATCHED")
        flagged["_match_reason"] = exists.map(lambda value: "EXTERNAL_ID_EXACT" if value else "NO_MATCH")
        flagged["_matched_to_reference"] = exists
        flagged["_matched_confident"] = exists
        flagged["_matched_possible"] = False
        flagged["_matched_review"] = False
        flagged["_address_matched"] = flagged.get("_exists_by_name_address", False)

    context["datasets"][output_role] = flagged
    context["stats"]["flag_reference_identity"] = {
        "primary_role": primary_role,
        "reference_role": reference_role,
        "output_role": output_role,
        "row_count": len(flagged),
        "external_id_matches": int(flagged.get("_exists_by_external_id", pd.Series(dtype=bool)).fillna(False).sum()),
        "name_address_matches": int(flagged.get("_exists_by_name_address", pd.Series(dtype=bool)).fillna(False).sum()),
        "name_address1_matches": int(flagged.get("_exists_by_name_address1", pd.Series(dtype=bool)).fillna(False).sum()),
    }
    return context


def run_write_records_bundle(context: StageContext, config: dict) -> StageContext:
    context = _ensure_context(context)
    dataset_role = config.get("dataset_role", "primary")
    dataset = _dataset_or_error(context, dataset_role)
    base_dir = Path(config["base_dir"])
    output_rules = config["outputs"]

    written: dict[str, str] = {}
    counts: dict[str, int] = {}

    for output_key, rule in output_rules.items():
        status_values = rule.get("match_status")
        if status_values is None:
            frame = dataset.copy()
        else:
            statuses = status_values if isinstance(status_values, list) else [status_values]
            frame = dataset[dataset["_match_status"].isin(statuses)].copy()

        if frame.empty and not rule.get("write_if_empty", False):
            counts[output_key] = 0
            continue

        projected = project_records(frame)
        output_path = base_dir / rule["filename"]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_csv(projected, str(output_path))
        written[output_key] = str(output_path)
        counts[output_key] = len(frame)

    context["outputs"].update(written)
    context["stats"]["write_records_bundle"] = {
        "dataset_role": dataset_role,
        "counts": counts,
        "outputs": written,
    }
    return context


STAGE_REGISTRY: dict[str, StageDefinition] = {
    "join_reference_fields": StageDefinition(
        name="join_reference_fields",
        label="Join reference fields into primary rows",
        required_inputs=("primary", "reference"),
        summary="Copies selected fields from a reference dataset onto matching primary rows by exact join key.",
        operator_goal="Use a trusted lookup file to enrich an existing working file after you already know how the rows should join.",
        inputs_detail=(
            "Needs a primary dataset and a reference dataset.",
            "Requires a join key plus a list of reference fields to copy over.",
        ),
        effects=(
            "Updates the primary dataset in memory with copied reference fields.",
            "Records join statistics in the runtime summary.",
        ),
        watch_for=(
            "This is an exact-key enrichment stage, not a fuzzy matcher.",
            "Dirty join keys will weaken the result.",
        ),
        runner=run_join_reference_fields,
    ),
    "aggregate_contacts": StageDefinition(
        name="aggregate_contacts",
        label="Aggregate contact fields",
        required_inputs=("primary",),
        summary="Builds cleaner contact coverage from existing email and phone columns in one dataset.",
        operator_goal="Consolidate contact evidence before export or scoring.",
        inputs_detail=("Runs on one dataset role, usually primary.",),
        effects=(
            "Adds or updates aggregated contact fields on the dataset.",
            "Improves downstream exports and review utility.",
        ),
        watch_for=("This only reorganizes existing contact data. It does not create new data.",),
        runner=run_aggregate_contacts,
    ),
    "classify_address_status": StageDefinition(
        name="classify_address_status",
        label="Classify address status",
        required_inputs=("primary",),
        summary="Adds address-quality or address-status labels based on the normalized address fields in the dataset.",
        operator_goal="Separate rows with usable addresses from rows that may still need cleanup.",
        inputs_detail=("Runs on one dataset role after address fields already exist.",),
        effects=("Adds address status fields used for follow-up and prioritization.",),
        watch_for=("This stage is weaker if address normalization has not already happened.",),
        runner=run_classify_address_status,
    ),
    "score_priority": StageDefinition(
        name="score_priority",
        label="Score row priority",
        required_inputs=("primary",),
        summary="Assigns a priority score to rows using the fields already present on the dataset.",
        operator_goal="Push the most actionable rows to the top for operators.",
        inputs_detail=("Runs on one dataset role, usually after enrichment and classification.",),
        effects=("Adds scoring columns used for triage or export ordering.",),
        watch_for=("Scores are only as good as the upstream fields already on the dataset.",),
        runner=run_score_priority,
    ),
    "project_records": StageDefinition(
        name="project_records",
        label="Project a dataset for export",
        required_inputs=("primary",),
        summary="Takes one dataset and creates a projected artifact for downstream writing.",
        operator_goal="Prepare a shaped artifact before writing it to disk.",
        inputs_detail=("Runs on one dataset role.",),
        effects=("Creates an artifact rather than writing a file immediately.",),
        watch_for=("Use write_records_output after this if you actually want a file on disk.",),
        runner=run_project_records,
    ),
    "write_records_output": StageDefinition(
        name="write_records_output",
        label="Write one output file",
        required_inputs=(),
        summary="Writes either a dataset or an artifact to one named CSV output.",
        operator_goal="Persist one result to disk at a known location.",
        inputs_detail=("Needs a source dataset or artifact plus base directory and filename.",),
        effects=("Creates a CSV on disk and records the path in runtime outputs.",),
        watch_for=("Use write_records_bundle instead when you want the standard matched, review, unmatched bundle.",),
        runner=run_write_records_output,
    ),
    "split_rows": StageDefinition(
        name="split_rows",
        label="Split rows into alternating buckets",
        required_inputs=("source",),
        summary="Splits one source dataset into alternating row buckets and stores them as artifacts.",
        operator_goal="Create a simple even split for assignment or paired review.",
        inputs_detail=(
            "Runs on one source dataset.",
            "Current built-in method is alternating row split.",
        ),
        effects=("Creates split_a and split_b artifacts for later writing.",),
        watch_for=("This is a mechanical split. It does not look at row content.",),
        runner=run_split_rows,
    ),
    "extract_projection": StageDefinition(
        name="extract_projection",
        label="Extract and format a projection",
        required_inputs=("source",),
        summary="Builds a projection artifact from one source dataset and applies simple date or amount formatting when configured.",
        operator_goal="Produce a clean reduced export from a larger file.",
        inputs_detail=(
            "Runs on one source dataset.",
            "Needs the list of projection columns to keep.",
        ),
        effects=("Creates a projection artifact with only the selected columns.",),
        watch_for=("Missing projection columns will stop the stage.",),
        runner=run_extract_projection,
    ),
    "normalize_addresses": StageDefinition(
        name="normalize_addresses",
        label="Normalize addresses and split unit lines",
        required_inputs=("primary",),
        summary="Normalizes address fields, splits apartment-style lines when possible, and builds the canonical _address_norm field.",
        operator_goal="Reduce superficial address differences before matching, scoring, or export.",
        inputs_detail=(
            "Runs on one dataset role.",
            "Needs address1, city, state, and zip columns. Address2 is optional but strongly recommended.",
        ),
        effects=(
            "Updates address columns in place.",
            "Adds _address_norm and _address_split_status fields.",
        ),
        watch_for=(
            "Wrong city, state, or zip mappings will weaken the normalized address output.",
            "Reference datasets often need different city, state, zip canonical targets than primary datasets.",
        ),
        runner=run_normalize_addresses,
    ),
    "normalize_date_columns": StageDefinition(
        name="normalize_date_columns",
        label="Normalize date fields",
        required_inputs=("primary",),
        summary="Standardizes configured date columns in one dataset.",
        operator_goal="Make date evidence stable before export or downstream logic.",
        inputs_detail=(
            "Runs on one dataset role.",
            "Needs an explicit list of date columns.",
        ),
        effects=("Rewrites the selected date fields in the dataset.",),
        watch_for=("Only the columns you list will be normalized.",),
        runner=run_normalize_date_columns,
    ),
    "dedupe_records": StageDefinition(
        name="dedupe_records",
        label="Remove duplicate rows",
        required_inputs=("primary",),
        summary="Drops duplicate rows using configured subset keys or the built-in fallback key set.",
        operator_goal="Reduce repeated rows before matching or export.",
        inputs_detail=(
            "Runs on one dataset role.",
            "Can use an explicit subset or fall back to person_id, first_name, last_name, and _address_norm when present.",
        ),
        effects=(
            "Replaces the dataset with a deduplicated version.",
            "Records rows removed in the stage stats.",
        ),
        watch_for=("Be careful running this before you trust the dedupe key set.",),
        runner=run_dedupe_records,
    ),
    "match_records": StageDefinition(
        name="match_records",
        label="Match records and score evidence",
        required_inputs=("primary", "reference"),
        summary="Runs the scored matcher between a primary and reference dataset and writes match annotations back onto the output dataset.",
        operator_goal="Generate confident, possible, review, and unmatched evidence between two files.",
        inputs_detail=(
            "Needs a primary dataset and a reference dataset.",
            "Works best when IDs, names, addresses, email, and phone are mapped coherently first.",
        ),
        effects=(
            "Adds match status, reason, score, and related helper fields to the output dataset.",
            "Records by-status and by-reason counts in stage stats.",
        ),
        watch_for=(
            "If the review bucket explodes, fix mappings and normalization before lowering thresholds.",
            "This stage depends heavily on canonical field quality.",
        ),
        runner=run_match_records,
    ),
    "flag_reference_identity": StageDefinition(
        name="flag_reference_identity",
        label="Flag exact identity overlaps",
        required_inputs=("primary", "reference"),
        summary="Checks for exact identity-style overlaps such as external ID or name-plus-address matches and stamps match fields accordingly.",
        operator_goal="Use hard identity evidence before or instead of fuzzier matching.",
        inputs_detail=(
            "Needs a primary dataset and a reference dataset.",
            "Usually relies on person_id plus name and normalized address fields.",
        ),
        effects=("Adds identity-style existence and match columns to the output dataset.",),
        watch_for=("This stage is stricter than the generic scored matcher.",),
        runner=run_flag_reference_identity,
    ),
    "write_records_bundle": StageDefinition(
        name="write_records_bundle",
        label="Write matched, review, and unmatched outputs",
        required_inputs=("primary",),
        summary="Writes the standard bundle of records outputs based on match status rules.",
        operator_goal="Persist the familiar match buckets without hand-defining each output file.",
        inputs_detail=(
            "Runs on a dataset that already has _match_status.",
            "Needs a base directory and output rules.",
        ),
        effects=(
            "Writes the configured output bundle to disk.",
            "Records per-output counts and paths in stage stats.",
        ),
        watch_for=("If the dataset does not already contain match status fields, this stage will not behave meaningfully.",),
        runner=run_write_records_bundle,
    ),
}


def run_stage(context: StageContext, stage_name: str, config: dict | None = None) -> StageContext:
    stage = STAGE_REGISTRY.get(stage_name)
    if stage is None:
        raise ValueError(f"Unsupported stage: {stage_name}")

    context = _ensure_context(context)
    missing_inputs = [role for role in stage.required_inputs if role not in context["datasets"]]
    if missing_inputs:
        raise ValueError(f"Stage '{stage_name}' is missing dataset roles: {', '.join(missing_inputs)}")

    return stage.runner(context, config or {})


def run_stage_sequence(context: StageContext, stages: list[tuple[str, dict | None]]) -> StageContext:
    context = _ensure_context(context)
    for stage_name, config in stages:
        context = run_stage(context, stage_name, config)
    return context
