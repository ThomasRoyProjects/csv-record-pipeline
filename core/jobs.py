from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import yaml

from core.workflows import WORKFLOW_INPUTS, resolve_workflow_name


def load_job(job_path: str | Path) -> dict:
    path = Path(job_path).resolve()
    with open(path) as handle:
        job = yaml.safe_load(handle) or {}
    if isinstance(job, dict):
        job["__job_path__"] = str(path)
    return job


def _deep_merge(base: dict, override: dict) -> dict:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _resolve_path_value(value: str, *, base_dir: Path) -> str:
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def _path_base_dir(value: str, *, base_dir: Path, root_dir: Path) -> Path:
    parts = Path(value).parts
    if parts and parts[0] in {
        "demo_data",
        "docs",
        "input",
        "jobs",
        "normalization_profiles",
        "output",
        "presets",
        "profiles",
    }:
        return root_dir
    return base_dir


def _resolve_runtime_paths(payload: dict, *, base_dir: Path, root_dir: Path) -> dict:
    resolved = deepcopy(payload)

    def visit(node):
        if isinstance(node, dict):
            updated = {}
            for key, value in node.items():
                if key == "path" and isinstance(value, str) and value:
                    updated[key] = _resolve_path_value(
                        value,
                        base_dir=_path_base_dir(value, base_dir=base_dir, root_dir=root_dir),
                    )
                elif key == "paths" and isinstance(value, list):
                    updated[key] = [
                        _resolve_path_value(
                            item,
                            base_dir=_path_base_dir(item, base_dir=base_dir, root_dir=root_dir),
                        )
                        if isinstance(item, str) and item
                        else item
                        for item in value
                    ]
                elif key in {"base_dir", "path_a", "path_b"} and isinstance(value, str) and value:
                    updated[key] = _resolve_path_value(
                        value,
                        base_dir=_path_base_dir(value, base_dir=base_dir, root_dir=root_dir),
                    )
                else:
                    updated[key] = visit(value)
            return updated
        if isinstance(node, list):
            return [visit(item) for item in node]
        return node

    return visit(resolved)


def build_runtime_config(job: dict, *, root_dir: str | Path) -> dict:
    """
    Convert a job spec into the same runtime config shape used by the runner.

    A job may either:
    - reference a template profile and override parts of it
    - declare a workflow and full inputs/outputs directly
    """
    job = deepcopy(job)
    job_path = job.pop("__job_path__", "")
    job_base_dir = Path(job_path).resolve().parent if job_path else Path(root_dir).resolve()
    template = job.pop("template", None)

    base = {}
    if template:
        template_path = Path(template)
        if not template_path.is_absolute():
            template_path = job_base_dir / template_path
        template_path = template_path.resolve()
        with open(template_path) as handle:
            base = yaml.safe_load(handle) or {}
        if isinstance(base, dict):
            base = _resolve_runtime_paths(base, base_dir=template_path.parent, root_dir=Path(root_dir).resolve())

    runtime = _deep_merge(base, job)
    runtime = _resolve_runtime_paths(runtime, base_dir=job_base_dir, root_dir=Path(root_dir).resolve())

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
