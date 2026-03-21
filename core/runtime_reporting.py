from __future__ import annotations

import json
from pathlib import Path


def write_run_summary(base_dir: str | Path, summary: dict) -> Path:
    summary_path = Path(base_dir) / "run_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2))
    return summary_path


def build_runtime_summary(
    *,
    workflow: str,
    counts: dict,
    outputs: dict,
    stage_stats: dict | None = None,
    extra: dict | None = None,
) -> dict:
    summary = {
        "workflow": workflow,
        "counts": counts,
        "outputs": outputs,
        "stage_stats": stage_stats or {},
    }
    if extra:
        summary.update(extra)
    return summary


def build_dataset_counts(context: dict) -> dict[str, int]:
    return {role: len(df) for role, df in context.get("datasets", {}).items()}


def build_bundle_output_paths(base_dir: str | Path, outputs: dict[str, dict]) -> dict[str, str]:
    base = Path(base_dir)
    return {output_key: str(base / rule["filename"]) for output_key, rule in outputs.items()}


def build_bundle_counts(context: dict, outputs: dict[str, dict]) -> dict[str, int]:
    bundle_counts = context.get("stats", {}).get("write_records_bundle", {}).get("counts", {})
    return {output_key: bundle_counts.get(output_key, 0) for output_key in outputs}


def build_records_bundle_summary(
    *,
    workflow: str,
    context: dict,
    base_dir: str | Path,
    outputs: dict[str, dict],
    extra_counts: dict | None = None,
    extra: dict | None = None,
) -> dict:
    counts = build_bundle_counts(context, outputs)
    if extra_counts:
        counts.update(extra_counts)
    return build_runtime_summary(
        workflow=workflow,
        counts=counts,
        outputs=build_bundle_output_paths(base_dir, outputs),
        stage_stats=context.get("stats", {}),
        extra=extra,
    )
