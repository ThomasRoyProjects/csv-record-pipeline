from __future__ import annotations

import argparse
from pprint import pformat
from pathlib import Path

import pandas as pd
import yaml

from core.jobs import build_runtime_config, load_job
from core.preset_plans import (
    build_compare_records_plan,
    build_enrich_records_plan,
    build_extract_projection_plan,
    build_match_records_plan,
    build_process_full_records_plan,
    build_split_plan,
)
from core.runtime_loader import load_dataset, load_runtime_datasets
from core.runtime_reporting import (
    build_bundle_counts,
    build_dataset_counts,
    build_records_bundle_summary,
    build_runtime_summary,
    write_run_summary,
)
from core.stages import run_stage_sequence
from core.workflows import WORKFLOW_LABELS, resolve_workflow_name
from export.records import project_records
from services.workflow_service import describe_workflow, inspect_headers, suggest_mappings, validate_job_file


ROOT = Path(__file__).resolve().parent
PROFILES_DIR = ROOT / "profiles"


def load_profile(profile_path: str | Path) -> dict:
    with open(profile_path) as handle:
        return yaml.safe_load(handle)


def list_profiles() -> list[Path]:
    return sorted(PROFILES_DIR.glob("*.yaml"))


def normalize_member_addresses(df: pd.DataFrame, *, address1_col: str, address2_col: str, city_col: str, state_col: str, zip_col: str) -> pd.DataFrame:
    context = run_stage_sequence(
        {"datasets": {"primary": df}},
        [
            (
                "normalize_addresses",
                {
                    "dataset_role": "primary",
                    "mode": "member",
                    "address1_col": address1_col,
                    "address2_col": address2_col,
                    "city_col": city_col,
                    "state_col": state_col,
                    "zip_col": zip_col,
                },
            )
        ],
    )
    return context["datasets"]["primary"]


def normalize_voter_addresses(df: pd.DataFrame, *, address1_col: str, address2_col: str, city_col: str, state_col: str, zip_col: str) -> pd.DataFrame:
    context = run_stage_sequence(
        {"datasets": {"primary": df}},
        [
            (
                "normalize_addresses",
                {
                    "dataset_role": "primary",
                    "mode": "voter",
                    "address1_col": address1_col,
                    "address2_col": address2_col,
                    "city_col": city_col,
                    "state_col": state_col,
                    "zip_col": zip_col,
                },
            )
        ],
    )
    return context["datasets"]["primary"]


def resolve_address_columns(df: pd.DataFrame, *, mode: str) -> dict[str, str]:
    if mode == "member":
        return {
            "address1_col": "primary_address1" if "primary_address1" in df.columns else next((c for c in ["FullResidentialAddress", "address1"] if c in df.columns), "primary_address1"),
            "address2_col": "primary_address2" if "primary_address2" in df.columns else "",
            "city_col": "mail_city" if "mail_city" in df.columns else next((c for c in ["City", "primary_city"] if c in df.columns), ""),
            "state_col": "mail_state" if "mail_state" in df.columns else next((c for c in ["Province", "primary_state"] if c in df.columns), ""),
            "zip_col": "mail_zip" if "mail_zip" in df.columns else next((c for c in ["PostalCode", "primary_zip"] if c in df.columns), ""),
        }
    return {
        "address1_col": "primary_address1" if "primary_address1" in df.columns else next((c for c in ["FullResidentialAddress", "address1"] if c in df.columns), "primary_address1"),
        "address2_col": "primary_address2" if "primary_address2" in df.columns else "",
        "city_col": "primary_city" if "primary_city" in df.columns else next((c for c in ["City", "mail_city"] if c in df.columns), ""),
        "state_col": "primary_state" if "primary_state" in df.columns else next((c for c in ["Province", "mail_state"] if c in df.columns), ""),
        "zip_col": "primary_zip" if "primary_zip" in df.columns else next((c for c in ["PostalCode", "mail_zip"] if c in df.columns), ""),
    }

def make_context(workflow: str, **datasets: pd.DataFrame) -> dict:
    return {
        "workflow": workflow,
        "datasets": datasets,
    }

def infer_summary_path(runtime: dict) -> Path:
    outputs = runtime.get("outputs", {})
    if "records" in outputs and "base_dir" in outputs["records"]:
        return Path(outputs["records"]["base_dir"]) / "run_summary.json"
    if "records" in outputs and "path" in outputs["records"]:
        return Path(outputs["records"]["path"]).parent / "run_summary.json"
    if "projection" in outputs and "path" in outputs["projection"]:
        return Path(outputs["projection"]["path"]).parent / "run_summary.json"
    if "split" in outputs and "path_a" in outputs["split"]:
        return Path(outputs["split"]["path_a"]).parent / "run_summary.json"
    for output_cfg in outputs.values():
        if isinstance(output_cfg, dict):
            if "base_dir" in output_cfg:
                return Path(output_cfg["base_dir"]) / "run_summary.json"
            if "path" in output_cfg:
                return Path(output_cfg["path"]).parent / "run_summary.json"
    return Path("output") / "run_summary.json"


def _normalize_stage_sequence(raw_stage_sequence: list) -> list[tuple[str, dict | None]]:
    stages: list[tuple[str, dict | None]] = []
    for entry in raw_stage_sequence:
        if isinstance(entry, str):
            stages.append((entry, None))
            continue
        if isinstance(entry, dict):
            if "name" in entry:
                stages.append((entry["name"], entry.get("config")))
                continue
            if len(entry) == 1:
                stage_name, config = next(iter(entry.items()))
                stages.append((stage_name, config))
                continue
        raise ValueError(f"Invalid stage sequence entry: {entry!r}")
    return stages


def execute_custom_stage_sequence(runtime: dict) -> dict:
    context = make_context(runtime["profile"], **load_runtime_datasets(runtime))
    stages = _normalize_stage_sequence(runtime.get("stage_sequence", []))
    context = run_stage_sequence(context, stages)

    summary_path = infer_summary_path(runtime)
    summary = build_runtime_summary(
        workflow=runtime["profile"],
        counts={"datasets": build_dataset_counts(context)},
        outputs=context.get("outputs", {}),
        stage_stats=context.get("stats", {}),
    )
    write_run_summary(summary_path.parent, summary)
    return {
        "workflow": runtime["profile"],
        "summary_path": summary_path,
        "summary": summary,
        "context": context,
    }


def summarize_match_inputs(profile: dict, match_cfg: dict) -> dict:
    inputs = profile.get("inputs", {})
    groups = {
        "identity": ["person_id", "first_name", "middle_name", "last_name"],
        "address": [
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
            "primary_city",
            "primary_state",
            "primary_zip",
        ],
        "contact": ["email", "phone"],
    }

    summary: dict[str, dict] = {}
    for input_name in ["primary", "reference", "source"]:
        dataset_cfg = inputs.get(input_name)
        if not dataset_cfg:
            continue
        columns = dataset_cfg.get("columns", {})
        summary[input_name] = {
            group_name: {field: columns.get(field, "") for field in field_names}
            for group_name, field_names in groups.items()
        }

    summary["engine"] = {
        "normalized_address_column": {
            "primary": match_cfg.get("primary_address_col"),
            "reference": match_cfg.get("reference_address_col"),
        },
        "thresholds": {
            "confident": match_cfg.get("confident_threshold"),
            "possible": match_cfg.get("possible_threshold"),
            "review": match_cfg.get("review_threshold"),
        },
    }
    return summary


def summarize_match_results(df: pd.DataFrame) -> dict:
    if "_match_reason" not in df.columns:
        return {}
    reason_counts = df["_match_reason"].value_counts(dropna=False).to_dict()
    status_counts = df["_match_status"].value_counts(dropna=False).to_dict() if "_match_status" in df.columns else {}
    return {
        "by_reason": reason_counts,
        "by_status": status_counts,
    }


def resolve_dataset_config(profile: dict, primary_key: str, fallback_key: str | None = None) -> dict:
    inputs = profile["inputs"]
    if primary_key in inputs:
        return inputs[primary_key]
    if fallback_key and fallback_key in inputs:
        return inputs[fallback_key]
    raise KeyError(f"Missing dataset config for '{primary_key}'")


def select_base_output(outputs: dict, preferred_key: str = "records") -> dict:
    if preferred_key in outputs:
        return outputs[preferred_key]
    for fallback in ["exports", "projection", "split", "nationbuilder"]:
        if fallback in outputs:
            return outputs[fallback]
    return outputs


def normalize_primary_and_reference(primary: pd.DataFrame, reference: pd.DataFrame, profile: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not (profile["stages"].get("normalize") or profile["stages"].get("normalize_addresses")):
        return primary, reference

    context = {
        "datasets": {
            "primary": primary,
            "reference": reference,
        }
    }
    context = run_stage_sequence(
        context,
        [
            (
                "normalize_addresses",
                {
                    "dataset_role": "primary",
                    "mode": "member",
                    "address1_col": "primary_address1",
                    "address2_col": "primary_address2",
                    "city_col": "mail_city" if "mail_city" in primary.columns else "primary_city",
                    "state_col": "mail_state" if "mail_state" in primary.columns else "primary_state",
                    "zip_col": "mail_zip" if "mail_zip" in primary.columns else "primary_zip",
                },
            ),
            (
                "normalize_addresses",
                {
                    "dataset_role": "reference",
                    "mode": "voter",
                    "address1_col": "primary_address1",
                    "address2_col": "primary_address2",
                    "city_col": "primary_city" if "primary_city" in reference.columns else "mail_city",
                    "state_col": "primary_state" if "primary_state" in reference.columns else "mail_state",
                    "zip_col": "primary_zip" if "primary_zip" in reference.columns else "mail_zip",
                },
            ),
        ],
    )
    return context["datasets"]["primary"], context["datasets"]["reference"]


def build_match_config(profile: dict) -> dict:
    match_cfg = profile.get("match", {})
    return {
        "primary_id_col": match_cfg.get("primary_id_col", "person_id"),
        "reference_id_col": match_cfg.get("reference_id_col", "person_id"),
        "primary_first_col": match_cfg.get("primary_first_col", "first_name"),
        "reference_first_col": match_cfg.get("reference_first_col", "first_name"),
        "primary_last_col": match_cfg.get("primary_last_col", "last_name"),
        "reference_last_col": match_cfg.get("reference_last_col", "last_name"),
        "primary_address_col": match_cfg.get("primary_address_col", "_address_norm"),
        "reference_address_col": match_cfg.get("reference_address_col", "_address_norm"),
        "primary_address1_col": match_cfg.get("primary_address1_col", "primary_address1"),
        "reference_address1_col": match_cfg.get("reference_address1_col", "primary_address1"),
        "primary_address2_col": match_cfg.get("primary_address2_col", "primary_address2"),
        "reference_address2_col": match_cfg.get("reference_address2_col", "primary_address2"),
        "primary_postal_col": match_cfg.get("primary_postal_col"),
        "reference_postal_col": match_cfg.get("reference_postal_col"),
        "primary_email_col": match_cfg.get("primary_email_col", "email"),
        "reference_email_col": match_cfg.get("reference_email_col", "email"),
        "primary_phone_col": match_cfg.get("primary_phone_col", "phone"),
        "reference_phone_col": match_cfg.get("reference_phone_col", "phone"),
        "confident_threshold": match_cfg.get("confident_threshold", 160),
        "possible_threshold": match_cfg.get("possible_threshold", 120),
        "review_threshold": match_cfg.get("review_threshold", 85),
        "strict_mode": match_cfg.get("strict_mode", False),
    }


def run_compare_records_to_reference(profile: dict) -> None:
    primary = load_dataset(resolve_dataset_config(profile, "primary", "members"))
    reference = load_dataset(resolve_dataset_config(profile, "reference", "voters"))
    match_cfg = build_match_config(profile)
    base_dir = Path(select_base_output(profile["outputs"]).get("base_dir", profile["outputs"].get("base_dir", "output")))
    is_missing_workflow = profile["profile"] == "identify_missing_records_from_system"
    new_name = "likely_missing_records" if is_missing_workflow else "new_records"
    review_name = "needs_review_records" if is_missing_workflow else "review_records"
    matched_name = "likely_existing_records" if is_missing_workflow else "matched_records"

    bundle_rules = {
        new_name: {"match_status": "UNMATCHED", "filename": f"{new_name}.csv", "write_if_empty": True},
        review_name: {"match_status": ["POSSIBLE", "REVIEW"], "filename": f"{review_name}.csv"},
        matched_name: {"match_status": "CONFIDENT", "filename": f"{matched_name}.csv"},
    }
    context = make_context(profile["profile"], primary=primary, reference=reference)

    if profile["stages"].get("reconcile_identity"):
        normalized_primary, normalized_reference = normalize_primary_and_reference(
            context["datasets"]["primary"],
            context["datasets"]["reference"],
            profile,
        )
        context["datasets"]["primary"] = normalized_primary
        context["datasets"]["reference"] = normalized_reference
    context = run_stage_sequence(
        context,
        build_compare_records_plan(
            bundle_rules=bundle_rules,
            base_dir=base_dir,
            normalize_dates=profile["stages"].get("normalize", False),
            reconcile_identity=profile["stages"].get("reconcile_identity", False),
            match_cfg=match_cfg,
        ),
    )
    primary = context["datasets"]["primary"]
    bundle_counts = build_bundle_counts(context, bundle_rules)
    new_count = bundle_counts[new_name]
    review_count = bundle_counts[review_name]
    matched_count = bundle_counts[matched_name]

    write_run_summary(
        base_dir,
        build_records_bundle_summary(
            workflow=profile["profile"],
            context=context,
            base_dir=base_dir,
            outputs=bundle_rules,
            extra_counts={"total_records": len(primary)},
            extra={
                "match_summary": summarize_match_results(primary),
                "match_inputs": summarize_match_inputs(profile, match_cfg),
            },
        ),
    )

    print(f"{new_name.replace('_', ' ').title()}: {new_count}")
    print(f"{review_name.replace('_', ' ').title()}: {review_count}")
    print(f"{matched_name.replace('_', ' ').title()}: {matched_count}")


def run_match_records_to_reference(profile: dict) -> None:
    primary = load_dataset(resolve_dataset_config(profile, "primary", "members"))
    reference = load_dataset(resolve_dataset_config(profile, "reference", "voters"))

    primary, reference = normalize_primary_and_reference(primary, reference, profile)
    match_cfg = build_match_config(profile)
    base_dir = Path(select_base_output(profile["outputs"]).get("base_dir", "output"))
    bundle_rules = {
        "matched_records": {"match_status": "CONFIDENT", "filename": "matched_records.csv"},
        "review_records": {"match_status": ["POSSIBLE", "REVIEW"], "filename": "review_records.csv"},
        "unmatched_records": {"match_status": "UNMATCHED", "filename": "unmatched_records.csv"},
    }
    context = make_context(profile["profile"], primary=primary, reference=reference)
    context = run_stage_sequence(context, build_match_records_plan(bundle_rules=bundle_rules, base_dir=base_dir, match_cfg=match_cfg))

    primary = context["datasets"]["primary"]
    match_counts = build_bundle_counts(context, bundle_rules)
    matched_count = match_counts["matched_records"]
    review_count = match_counts["review_records"]
    unmatched_count = match_counts["unmatched_records"]
    write_run_summary(
        base_dir,
        build_records_bundle_summary(
            workflow=profile["profile"],
            context=context,
            base_dir=base_dir,
            outputs=bundle_rules,
            extra_counts={"total_records": len(primary)},
            extra={
                "match_summary": summarize_match_results(primary),
                "match_inputs": summarize_match_inputs(profile, match_cfg),
            },
        ),
    )
    print(f"Confident matches: {matched_count}")
    print(f"Review matches: {review_count}")
    print(f"Unmatched records: {unmatched_count}")


def run_enrich_records_from_reference(profile: dict) -> None:
    members = load_dataset(resolve_dataset_config(profile, "primary", "members"))
    voters = load_dataset(resolve_dataset_config(profile, "reference", "voters"))

    context = make_context(profile["profile"], primary=members, reference=voters)
    base_dir = Path(select_base_output(profile["outputs"]).get("base_dir", "output"))
    context = run_stage_sequence(context, build_enrich_records_plan(profile=profile, base_dir=base_dir))

    members = context["datasets"]["primary"]
    write_run_summary(
        base_dir,
        build_runtime_summary(
            workflow=profile["profile"],
            counts={"enriched_records": len(members)},
            outputs={"enriched_records": context["outputs"]["enriched_records"]},
            stage_stats=context.get("stats", {}),
        ),
    )
    print(f"Exported enriched rows: {len(members)}")


def run_extract_projection(profile: dict) -> None:
    df = load_dataset(profile["inputs"]["source"])
    projection_cfg = profile["outputs"]["projection"]

    context = make_context(profile["profile"], source=df)
    context = run_stage_sequence(context, build_extract_projection_plan(projection_cfg=projection_cfg))

    stats = context.get("stats", {}).get("extract_projection", {})
    output_path = Path(projection_cfg["path"])
    write_run_summary(
        output_path.parent,
        build_runtime_summary(
            workflow=profile["profile"],
            counts={
                "rows_loaded": stats.get("rows_loaded", len(df)),
                "rows_removed": stats.get("rows_removed", 0),
                "rows_projected": stats.get("rows_projected", 0),
            },
            outputs={"projection": context["outputs"].get("projection", str(output_path))},
            stage_stats=context.get("stats", {}),
        ),
    )
    print(f"Filtered zero donations: {stats.get('rows_removed', 0)} rows removed")
    print(f"Exported {stats.get('rows_projected', 0)} rows")


def run_split_alternating_rows(profile: dict) -> None:
    df = load_dataset(profile["inputs"]["source"])
    out_cfg = profile["outputs"]["split"]
    split_cfg = profile.get("split", {})

    context = make_context(profile["profile"], source=df)
    context = run_stage_sequence(context, build_split_plan(out_cfg=out_cfg, split_cfg=split_cfg))

    stats = context.get("stats", {}).get("split_rows", {})
    output_dir = Path(out_cfg["path_a"]).parent
    write_run_summary(
        output_dir,
        build_runtime_summary(
            workflow=profile["profile"],
            counts={
                "rows_a": stats.get("rows_a", 0),
                "rows_b": stats.get("rows_b", 0),
                "rows_loaded": len(df),
            },
            outputs={
                "split_a": context["outputs"].get("split_a", out_cfg["path_a"]),
                "split_b": context["outputs"].get("split_b", out_cfg["path_b"]),
            },
            stage_stats=context.get("stats", {}),
        ),
    )
    print(f"File A rows: {stats.get('rows_a', 0)}")
    print(f"File B rows: {stats.get('rows_b', 0)}")


def run_process_full_records(profile: dict) -> None:
    members_input = resolve_dataset_config(profile, "primary", "members")
    voters_input = resolve_dataset_config(profile, "reference", "voters")

    members_cfg = members_input if isinstance(members_input, dict) else {"paths": members_input}
    voters_cfg = voters_input if isinstance(voters_input, dict) else {"paths": voters_input}

    members = load_dataset(members_cfg)
    voters = load_dataset(voters_cfg)
    context = make_context(profile["profile"], primary=members, reference=voters)
    output_path = select_base_output(profile["outputs"]).get("path", profile["outputs"].get("path"))
    if not output_path:
        output_path = "output/processed_records.csv"
    match_cfg = build_match_config(profile) if profile["stages"].get("reconcile") else None
    context = run_stage_sequence(
        context,
        build_process_full_records_plan(
            profile=profile,
            primary_address_columns=resolve_address_columns(context["datasets"]["primary"], mode="member"),
            reference_address_columns=resolve_address_columns(context["datasets"]["reference"], mode="voter"),
            output_path=output_path,
            match_cfg=match_cfg,
        ),
    )
    members = context["datasets"]["primary"]
    dedupe_stats = context.get("stats", {}).get("dedupe_records", {})
    write_run_summary(
        Path(output_path).parent,
        build_runtime_summary(
            workflow=profile["profile"],
            counts={"processed_records": len(members)},
            outputs={"processed_records": context["outputs"]["processed_records"]},
            stage_stats=context.get("stats", {}),
        ),
    )
    if profile["stages"].get("dedupe"):
        print(f"Deduped members: {dedupe_stats.get('rows_removed', 0)} rows removed")
    print(f"Exported processed rows: {len(members)}")


CASE_RUNNERS = {
    "compare_records_to_reference": run_compare_records_to_reference,
    "identify_missing_records_from_system": run_compare_records_to_reference,
    "match_records_to_reference": run_match_records_to_reference,
    "enrich_records_from_reference": run_enrich_records_from_reference,
    "extract_projection": run_extract_projection,
    "split_alternating_rows": run_split_alternating_rows,
    "process_full_records": run_process_full_records,
}


def execute_runtime(runtime: dict) -> dict:
    workflow = resolve_workflow_name(runtime["profile"])
    runtime["profile"] = workflow

    if workflow == "custom_job" or runtime.get("stage_sequence"):
        return execute_custom_stage_sequence(runtime)

    runner = CASE_RUNNERS.get(workflow)
    if runner is None:
        raise ValueError(f"Unsupported workflow: {workflow}")

    runner(runtime)
    summary_path = infer_summary_path(runtime)
    summary = None
    if summary_path.exists():
        import json
        summary = json.loads(summary_path.read_text())
    return {
        "workflow": workflow,
        "summary_path": summary_path,
        "summary": summary,
    }


def run_profile(profile_path: str | Path) -> None:
    profile = load_profile(profile_path)
    profile_name = resolve_workflow_name(profile["profile"])
    print(f"Running workflow: {profile_name}")
    execute_runtime(profile)
    print("Pipeline completed successfully.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run local pipeline cases from YAML profiles.")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run a profile")
    run_parser.add_argument("profile", help="Path to a profile YAML file")

    run_job_parser = subparsers.add_parser("run-job", help="Run a job spec with custom file mappings")
    run_job_parser.add_argument("job", help="Path to a job YAML file")

    validate_job_parser = subparsers.add_parser("validate-job", help="Validate a job spec")
    validate_job_parser.add_argument("job", help="Path to a job YAML file")

    describe_parser = subparsers.add_parser("describe", help="Describe a workflow")
    describe_parser.add_argument("workflow", help="Workflow name")

    headers_parser = subparsers.add_parser("headers", help="Print CSV headers only")
    headers_parser.add_argument("csv_path", help="Path to a CSV file")

    suggest_parser = subparsers.add_parser("suggest-mapping", help="Suggest canonical mappings from CSV headers")
    suggest_parser.add_argument("csv_path", help="Path to a CSV file")

    subparsers.add_parser("list", help="List available bundled profiles")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list":
        for profile_path in list_profiles():
            profile = load_profile(profile_path)
            workflow_name = resolve_workflow_name(profile["profile"])
            label = WORKFLOW_LABELS.get(workflow_name, workflow_name)
            print(f"{profile_path.relative_to(ROOT)} -> {workflow_name}: {label}")
        return 0

    if args.command == "run":
        run_profile(args.profile)
        return 0

    if args.command == "run-job":
        job = load_job(args.job)
        runtime = build_runtime_config(job, root_dir=ROOT)
        errors = validate_job_file(args.job, root_dir=ROOT)[1]
        if errors:
            for error in errors:
                print(f"ERROR: {error}")
            return 1
        print(f"Running job workflow: {runtime['profile']}")
        execute_runtime(runtime)
        print("Job completed successfully.")
        return 0

    if args.command == "validate-job":
        runtime, errors = validate_job_file(args.job, root_dir=ROOT)
        print(f"Workflow: {runtime['profile']}")
        if errors:
            for error in errors:
                print(f"ERROR: {error}")
            return 1
        print("Job is valid.")
        return 0

    if args.command == "describe":
        print(pformat(describe_workflow(args.workflow), sort_dicts=False))
        return 0

    if args.command == "headers":
        for header in inspect_headers(args.csv_path):
            print(header)
        return 0

    if args.command == "suggest-mapping":
        headers = inspect_headers(args.csv_path)
        print(pformat(suggest_mappings(headers), sort_dicts=False))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
