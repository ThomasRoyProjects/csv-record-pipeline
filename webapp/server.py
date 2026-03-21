from __future__ import annotations

import json
import mimetypes
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.jobs import build_runtime_config
from core.canonical import FIELD_ALIASES
from core.stages import STAGE_REGISTRY
from core.normalization_profiles import (
    NORMALIZATION_PROFILES_DIR,
    load_normalization_profile,
    save_normalization_profile,
)
from core.presets import list_presets, load_preset, save_preset
from dataio.csv import load_csv_safe, write_csv
from pipeline_runner import ROOT, execute_runtime, normalize_voter_addresses
from services.workflow_service import (
    classify_headers,
    describe_workflow,
    inspect_headers,
    list_workflows,
    suggest_mappings,
    validate_runtime_config,
)


WEB_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = WEB_ROOT / "static"
INPUT_ROOT = PROJECT_ROOT / "input"
INPUT_RAW_ROOT = INPUT_ROOT / "raw"
INPUT_NORMALIZED_ROOT = INPUT_ROOT / "normalized"
OUTPUT_ROOT = PROJECT_ROOT / "output"
OUTPUT_RUNS_ROOT = OUTPUT_ROOT / "webapp_runs"
JOB_LOCK = threading.Lock()
JOB_REGISTRY: dict[str, dict] = {}


def list_normalization_profiles() -> list[dict]:
    profiles = []
    for path in sorted(NORMALIZATION_PROFILES_DIR.glob("*.yaml")):
        profile = load_normalization_profile(path)
        derive = profile.get("derive", {})
        profiles.append(
            {
                "name": path.stem,
                "path": str(path),
                "targets": list(derive.keys()),
                "strategies": {
                    key: (value.get("strategy", "copy") if isinstance(value, dict) else "copy")
                    for key, value in derive.items()
                },
            }
        )
    return profiles


def ensure_app_dirs() -> None:
    for path in [INPUT_ROOT, INPUT_RAW_ROOT, INPUT_NORMALIZED_ROOT, OUTPUT_ROOT, OUTPUT_RUNS_ROOT]:
        path.mkdir(parents=True, exist_ok=True)


def _file_payload(path: Path, *, base: Path) -> dict:
    stat = path.stat()
    return {
        "name": path.name,
        "path": str(path),
        "relative_path": str(path.relative_to(base)),
        "size": stat.st_size,
        "modified": stat.st_mtime,
        "download_path": f"/api/download-file?path={path.as_posix()}",
    }


def list_inventory_files(root: Path, suffixes: tuple[str, ...]) -> list[dict]:
    ensure_app_dirs()
    files = [path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in suffixes]
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return [_file_payload(path, base=root) for path in files[:50]]


def file_inventory() -> dict:
    return {
        "inputs": list_inventory_files(INPUT_RAW_ROOT, (".csv",)),
        "normalized": list_inventory_files(INPUT_NORMALIZED_ROOT, (".csv",)),
        "outputs": list_inventory_files(OUTPUT_ROOT, (".csv", ".json")),
    }


def demo_defaults() -> dict:
    demo_root = PROJECT_ROOT / "demo_data"
    return {
        "prep": {
            "input_path": str(demo_root / "demo_profiled_import.csv"),
            "profile_name": "split_address_3col",
        },
        "workflows": {
            "compare_records_to_reference": {
                "primary_path": str(demo_root / "demo_match_primary.csv"),
                "reference_path": str(demo_root / "demo_match_reference.csv"),
            },
            "identify_missing_records_from_system": {
                "primary_path": str(demo_root / "demo_match_primary.csv"),
                "reference_path": str(demo_root / "demo_match_reference.csv"),
            },
            "match_records_to_reference": {
                "primary_path": str(demo_root / "demo_match_primary.csv"),
                "reference_path": str(demo_root / "demo_match_reference.csv"),
            },
            "custom_job": {
                "primary_path": str(demo_root / "demo_match_primary.csv"),
                "reference_path": str(demo_root / "demo_match_reference.csv"),
            },
            "process_full_records": {
                "primary_path": str(demo_root / "demo_process_members.csv"),
                "reference_path": str(demo_root / "demo_match_reference.csv"),
            },
            "enrich_records_from_reference": {
                "primary_path": str(demo_root / "demo_match_primary.csv"),
                "reference_path": str(demo_root / "demo_match_reference.csv"),
            },
            "extract_projection": {
                "source_path": str(demo_root / "demo_legacy_members.csv"),
            },
            "split_alternating_rows": {
                "source_path": str(demo_root / "demo_split_source.csv"),
            },
        },
    }


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def delete_managed_file(path_value: str) -> dict:
    path = Path(path_value).resolve()
    allowed_roots = [INPUT_RAW_ROOT.resolve(), INPUT_NORMALIZED_ROOT.resolve(), OUTPUT_ROOT.resolve()]
    if not any(_path_is_within(path, root) for root in allowed_roots):
        raise ValueError("Refusing to delete a file outside managed project directories")
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    path.unlink()
    return {"ok": True, "deleted_path": str(path)}


def delete_all_managed_outputs() -> dict:
    deleted = 0
    for path in sorted(OUTPUT_ROOT.rglob("*"), reverse=True):
        if path.is_file():
            path.unlink()
            deleted += 1
        elif path.is_dir():
            try:
                path.rmdir()
            except OSError:
                pass
    return {"ok": True, "deleted_count": deleted}


def list_stage_definitions() -> list[dict]:
    return [
        {
            "name": stage.name,
            "label": stage.label,
            "required_inputs": list(stage.required_inputs),
            "summary": stage.summary,
            "operator_goal": stage.operator_goal,
            "inputs_detail": list(stage.inputs_detail),
            "effects": list(stage.effects),
            "watch_for": list(stage.watch_for),
        }
        for stage in STAGE_REGISTRY.values()
    ]


def load_run_summary(summary_path: Path) -> dict | None:
    if not summary_path.exists():
        return None
    return json.loads(summary_path.read_text())


def _job_payload(job_id: str) -> dict:
    with JOB_LOCK:
        job = dict(JOB_REGISTRY.get(job_id, {}))
    if not job:
        return {}
    return {
        "job_id": job_id,
        "status": job.get("status", "unknown"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "workflow": job.get("workflow"),
        "result": job.get("result"),
        "errors": job.get("errors", []),
    }


def start_runtime_job(runtime: dict) -> dict:
    job_id = uuid4().hex
    now = time.time()
    with JOB_LOCK:
        JOB_REGISTRY[job_id] = {
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "workflow": runtime.get("profile"),
            "result": None,
            "errors": [],
        }

    def _runner() -> None:
        with JOB_LOCK:
            job = JOB_REGISTRY[job_id]
            job["status"] = "running"
            job["updated_at"] = time.time()
        try:
            result = execute_runtime(runtime)
            payload = {
                "ok": True,
                "workflow": result["workflow"],
                "summary_path": str(result["summary_path"]),
                "summary": result.get("summary") or load_run_summary(result["summary_path"]),
            }
            with JOB_LOCK:
                job = JOB_REGISTRY[job_id]
                job["status"] = "completed"
                job["updated_at"] = time.time()
                job["result"] = payload
        except Exception as exc:
            with JOB_LOCK:
                job = JOB_REGISTRY[job_id]
                job["status"] = "failed"
                job["updated_at"] = time.time()
                job["errors"] = [str(exc)]

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    return _job_payload(job_id)


def preview_csv(csv_path: Path, limit: int = 10) -> dict:
    if not csv_path.exists():
        return {"path": str(csv_path), "columns": [], "rows": [], "error": "File not found"}
    frame = load_csv_safe(str(csv_path)).head(limit)
    return {
        "path": str(csv_path),
        "columns": list(frame.columns),
        "rows": frame.to_dict(orient="records"),
    }


def _normalize_header_name(value: str) -> str:
    return "".join(ch for ch in str(value).strip().casefold() if ch.isalnum())


def _non_empty_count(frame, column_name: str) -> int:
    if column_name not in frame.columns:
        return -1
    values = frame[column_name]
    if hasattr(values, "columns"):
        values = values.iloc[:, 0]
    return int(values.fillna("").astype(str).str.strip().ne("").sum())


def _find_column(frame, canonical_field: str) -> str:
    alias_pool = [canonical_field] + FIELD_ALIASES.get(canonical_field, [])
    normalized_map = {_normalize_header_name(column): column for column in frame.columns}
    candidates = []
    for alias in alias_pool:
        key = _normalize_header_name(alias)
        if key in normalized_map:
            candidates.append(normalized_map[key])
    if not candidates:
        return ""
    candidates = list(dict.fromkeys(candidates))
    return max(
        candidates,
        key=lambda column: (_non_empty_count(frame, column), column == canonical_field),
    )


def _apply_builtin_address_normalization(frame):
    frame = frame.copy()
    source_address1 = _find_column(frame, "primary_address1")
    if not source_address1:
        return frame

    source_address2 = _find_column(frame, "primary_address2")
    source_city = _find_column(frame, "mail_city") or _find_column(frame, "primary_city")
    source_state = _find_column(frame, "mail_state") or _find_column(frame, "primary_state")
    source_zip = _find_column(frame, "mail_zip") or _find_column(frame, "primary_zip")

    frame["primary_address1"] = frame.get(source_address1, "")
    if source_address2:
        frame["primary_address2"] = frame.get(source_address2, "")
    elif "primary_address2" not in frame.columns:
        frame["primary_address2"] = ""
    if source_city and "mail_city" not in frame.columns:
        frame["mail_city"] = frame.get(source_city, "")
    if source_state and "mail_state" not in frame.columns:
        frame["mail_state"] = frame.get(source_state, "")
    if source_zip and "mail_zip" not in frame.columns:
        frame["mail_zip"] = frame.get(source_zip, "")

    return normalize_voter_addresses(
        frame,
        address1_col="primary_address1",
        address2_col="primary_address2",
        city_col="mail_city" if "mail_city" in frame.columns else "",
        state_col="mail_state" if "mail_state" in frame.columns else "",
        zip_col="mail_zip" if "mail_zip" in frame.columns else "",
    )


def _apply_canonical_mapping(frame, columns: dict | None):
    if not columns:
        return frame

    frame = frame.copy()
    for canonical_field, source_column in columns.items():
        source_columns = source_column if isinstance(source_column, list) else [source_column]
        source_columns = [column for column in source_columns if column]
        if not source_columns:
            continue
        source_values = None
        for column_name in source_columns:
            if column_name not in frame.columns:
                continue
            candidate_values = frame[column_name]
            if hasattr(candidate_values, "columns"):
                candidate_values = candidate_values.iloc[:, 0]
            if source_values is None:
                source_values = candidate_values
                continue
            existing_text = source_values.fillna("").astype(str).str.strip()
            source_values = source_values.where(existing_text != "", candidate_values)
        if source_values is None:
            continue

        if canonical_field in frame.columns:
            existing = frame[canonical_field]
            if hasattr(existing, "columns"):
                existing = existing.iloc[:, 0]
            existing_text = existing.fillna("").astype(str).str.strip()
            frame[canonical_field] = existing.where(existing_text != "", source_values)
        else:
            frame[canonical_field] = source_values
    return frame


def run_normalization(
    input_path: str,
    profile_name: str,
    output_name: str,
    *,
    strict_text_cleanup: bool = False,
    columns: dict | None = None,
) -> dict:
    ensure_app_dirs()
    source_path = Path(input_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {input_path}")
    output = build_normalized_output_path(source_path, profile_name, output_name)
    if not columns:
        columns = suggest_mappings(inspect_headers(str(source_path)))

    frame = load_csv_safe(str(source_path))
    if profile_name:
        profile = load_normalization_profile(profile_name)
        from core.normalization_profiles import apply_normalization_profile
        frame = apply_normalization_profile(frame, profile)
    if strict_text_cleanup:
        from core.normalization_profiles import apply_strict_text_cleanup
        frame = apply_strict_text_cleanup(frame)
    frame = _apply_canonical_mapping(frame, columns)
    frame = _apply_builtin_address_normalization(frame)
    write_csv(frame, str(output))
    return {
        "ok": True,
        "input_path": str(source_path),
        "output_name": output.name,
        "output_dir": str(output.parent),
        "output_path": str(output),
        "download_path": f"/api/download-file?path={output.as_posix()}",
        "preview": preview_csv(output, limit=8),
        "row_count": len(frame),
        "columns": list(frame.columns),
        "used_columns": columns or {},
        "strict_text_cleanup": strict_text_cleanup,
    }


def build_normalized_output_path(source_path: Path, profile_name: str, output_name: str) -> Path:
    ensure_app_dirs()
    suffix = profile_name or "normalized"
    requested_name = Path((output_name or "").strip()).name
    if requested_name in {"", ".", ".."}:
        requested_name = f"{source_path.stem}_{suffix}.csv"
    if not requested_name.lower().endswith(".csv"):
        requested_name = f"{requested_name}.csv"

    candidate = INPUT_NORMALIZED_ROOT / requested_name
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    extension = candidate.suffix
    counter = 2
    while True:
        next_candidate = candidate.with_name(f"{stem}_{counter}{extension}")
        if not next_candidate.exists():
            return next_candidate
        counter += 1


def save_uploaded_file(filename: str, content: str) -> dict:
    safe_name = Path(filename or "upload.csv").name
    stem = Path(safe_name).stem or "upload"
    suffix = Path(safe_name).suffix or ".csv"
    ensure_app_dirs()
    stored_path = INPUT_RAW_ROOT / f"{stem}_{uuid4().hex[:8]}{suffix}"
    stored_path.write_text(content)
    return {
        "filename": safe_name,
        "stored_path": str(stored_path),
    }


class AppHandler(BaseHTTPRequestHandler):
    def _json(self, status: int, payload: dict | list, *, head_only: bool = False) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _text_file(self, path: Path, *, head_only: bool = False) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(404)
            return
        content = path.read_bytes()
        mime, _ = mimetypes.guess_type(path.name)
        self.send_response(200)
        self.send_header("Content-Type", mime or "application/octet-stream")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if not head_only:
            self.wfile.write(content)

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b"{}"
        return json.loads(body.decode("utf-8"))

    def _run_runtime_job(self, runtime: dict) -> dict:
        errors = validate_runtime_config(runtime)
        if errors:
            return {"ok": False, "errors": errors}

        try:
            result = execute_runtime(runtime)
        except Exception as exc:
            return {"ok": False, "errors": [str(exc)]}
        return {
            "ok": True,
            "workflow": result["workflow"],
            "summary_path": str(result["summary_path"]),
            "summary": result.get("summary") or load_run_summary(result["summary_path"]),
        }

    def _handle_get_like(self, *, head_only: bool = False) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            return self._text_file(STATIC_ROOT / "index.html", head_only=head_only)
        if parsed.path == "/app.js":
            return self._text_file(STATIC_ROOT / "app.js", head_only=head_only)
        if parsed.path == "/styles.css":
            return self._text_file(STATIC_ROOT / "styles.css", head_only=head_only)
        if parsed.path == "/api/workflows":
            return self._json(200, list_workflows(), head_only=head_only)
        if parsed.path == "/api/stages":
            return self._json(200, list_stage_definitions(), head_only=head_only)
        if parsed.path == "/api/normalization-profiles":
            return self._json(200, list_normalization_profiles(), head_only=head_only)
        if parsed.path == "/api/file-inventory":
            return self._json(200, file_inventory(), head_only=head_only)
        if parsed.path == "/api/demo-defaults":
            return self._json(200, demo_defaults(), head_only=head_only)
        if parsed.path == "/api/presets":
            return self._json(200, list_presets(), head_only=head_only)
        if parsed.path.startswith("/api/presets/"):
            preset_name = parsed.path.split("/api/presets/", 1)[1]
            return self._json(200, load_preset(preset_name), head_only=head_only)
        if parsed.path.startswith("/api/workflows/"):
            workflow = parsed.path.split("/api/workflows/", 1)[1]
            return self._json(200, describe_workflow(workflow), head_only=head_only)
        if parsed.path == "/api/headers":
            query = parse_qs(parsed.query)
            path = query.get("path", [""])[0]
            return self._json(200, {"headers": inspect_headers(path)}, head_only=head_only)
        if parsed.path == "/api/suggest-mapping":
            query = parse_qs(parsed.query)
            path = query.get("path", [""])[0]
            headers = inspect_headers(path)
            return self._json(
                200,
                {
                    "headers": headers,
                    "suggestions": suggest_mappings(headers),
                    "groups": classify_headers(headers),
                },
                head_only=head_only,
            )
        if parsed.path == "/api/run-summary":
            query = parse_qs(parsed.query)
            path = query.get("path", [""])[0]
            return self._json(200, {"summary": load_run_summary(Path(path))}, head_only=head_only)
        if parsed.path == "/api/job-status":
            query = parse_qs(parsed.query)
            job_id = query.get("id", [""])[0]
            payload = _job_payload(job_id)
            if not payload:
                return self._json(404, {"ok": False, "errors": [f"Unknown job id: {job_id}"]}, head_only=head_only)
            return self._json(200, payload, head_only=head_only)
        if parsed.path == "/api/preview-csv":
            query = parse_qs(parsed.query)
            path = query.get("path", [""])[0]
            limit = int(query.get("limit", ["10"])[0])
            return self._json(200, preview_csv(Path(path), limit=limit), head_only=head_only)
        if parsed.path == "/api/download-file":
            query = parse_qs(parsed.query)
            path = query.get("path", [""])[0]
            return self._text_file(Path(path), head_only=head_only)

        self.send_error(404)

    def do_GET(self) -> None:
        self._handle_get_like(head_only=False)

    def do_HEAD(self) -> None:
        self._handle_get_like(head_only=True)

    def do_POST(self) -> None:
        if self.path == "/api/validate-job":
            payload = self._read_json_body()
            runtime = build_runtime_config(payload, root_dir=ROOT)
            errors = validate_runtime_config(runtime)
            return self._json(200, {"runtime": runtime, "errors": errors})

        if self.path == "/api/run-job":
            payload = self._read_json_body()
            runtime = build_runtime_config(payload, root_dir=ROOT)
            result = self._run_runtime_job(runtime)
            return self._json(200 if result.get("ok") else 400, result)

        if self.path == "/api/run-job-async":
            payload = self._read_json_body()
            runtime = build_runtime_config(payload, root_dir=ROOT)
            errors = validate_runtime_config(runtime)
            if errors:
                return self._json(400, {"ok": False, "errors": errors})
            return self._json(200, {"ok": True, **start_runtime_job(runtime)})

        if self.path == "/api/save-preset":
            payload = self._read_json_body()
            preset_name = payload.get("name", "")
            preset_payload = payload.get("preset", {})
            saved = save_preset(preset_name, preset_payload)
            return self._json(200, {"ok": True, "preset": saved})

        if self.path == "/api/upload-file":
            payload = self._read_json_body()
            filename = payload.get("filename", "")
            content = payload.get("content", "")
            if not filename:
                return self._json(400, {"ok": False, "errors": ["Filename is required"]})
            saved = save_uploaded_file(filename, content)
            return self._json(200, {"ok": True, **saved})

        if self.path == "/api/delete-file":
            payload = self._read_json_body()
            path_value = payload.get("path", "")
            try:
                result = delete_managed_file(path_value)
            except Exception as exc:
                return self._json(400, {"ok": False, "errors": [str(exc)]})
            return self._json(200, result)

        if self.path == "/api/delete-all-outputs":
            try:
                result = delete_all_managed_outputs()
            except Exception as exc:
                return self._json(400, {"ok": False, "errors": [str(exc)]})
            return self._json(200, result)

        if self.path == "/api/run-normalization":
            payload = self._read_json_body()
            try:
                result = run_normalization(
                    payload.get("input_path", ""),
                    payload.get("profile_name", ""),
                    payload.get("output_name", ""),
                    strict_text_cleanup=payload.get("strict_text_cleanup", False),
                    columns=payload.get("columns"),
                )
            except Exception as exc:
                return self._json(400, {"ok": False, "errors": [str(exc)]})
            return self._json(200, result)

        if self.path == "/api/save-normalization-profile":
            payload = self._read_json_body()
            profile_name = payload.get("name", "")
            profile_body = payload.get("profile", {})
            try:
                saved = save_normalization_profile(profile_name, profile_body)
            except Exception as exc:
                return self._json(400, {"ok": False, "errors": [str(exc)]})
            return self._json(200, {"ok": True, "profile": saved})

        self.send_error(404)


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    ensure_app_dirs()
    server = ThreadingHTTPServer((host, port), AppHandler)
    print(f"Web app running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    serve()
