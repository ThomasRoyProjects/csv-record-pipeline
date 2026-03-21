from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import yaml

from core.workflows import WORKFLOW_INPUTS, resolve_workflow_name


def load_job(job_path: str | Path) -> dict:
    with open(job_path) as handle:
        return yaml.safe_load(handle)


def _deep_merge(base: dict, override: dict) -> dict:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def build_runtime_config(job: dict, *, root_dir: str | Path) -> dict:
    """
    Convert a job spec into the same runtime config shape used by the runner.

    A job may either:
    - reference a template profile and override parts of it
    - declare a workflow and full inputs/outputs directly
    """
    job = deepcopy(job)
    template = job.pop("template", None)

    base = {}
    if template:
        template_path = Path(template)
        if not template_path.is_absolute():
            template_path = Path(root_dir) / template_path
        with open(template_path) as handle:
            base = yaml.safe_load(handle) or {}

    runtime = _deep_merge(base, job)

    workflow = runtime.get("workflow") or runtime.get("profile")
    if not workflow:
        raise ValueError("Job must define 'workflow' or 'profile'")

    runtime["profile"] = resolve_workflow_name(workflow)
    runtime.pop("workflow", None)
    runtime.setdefault("inputs", {})
    runtime.setdefault("outputs", {})
    runtime.setdefault("stages", {})
    runtime.setdefault("stage_sequence", [])
    runtime.setdefault("match", {})
    runtime.setdefault("enrich", {})
    return runtime


def required_inputs_for_workflow(workflow_name: str) -> list[str]:
    return WORKFLOW_INPUTS.get(resolve_workflow_name(workflow_name), [])
