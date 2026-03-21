from __future__ import annotations

import json
from pathlib import Path


PRESETS_DIR = Path(__file__).resolve().parent.parent / "presets"
PRESETS_DIR.mkdir(exist_ok=True)


def _preset_path(name: str) -> Path:
    safe = "".join(ch for ch in name if ch.isalnum() or ch in ("-", "_")).strip("_-")
    if not safe:
        raise ValueError("Preset name must contain letters or numbers")
    return PRESETS_DIR / f"{safe}.json"


def list_presets() -> list[dict]:
    presets = []
    for path in sorted(PRESETS_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text())
        except Exception:
            payload = {}
        presets.append(
            {
                "name": path.stem,
                "path": str(path),
                "workflow": payload.get("workflow", ""),
            }
        )
    return presets


def load_preset(name: str) -> dict:
    path = _preset_path(name)
    if not path.exists():
        raise FileNotFoundError(f"Preset not found: {name}")
    return json.loads(path.read_text())


def save_preset(name: str, payload: dict) -> dict:
    path = _preset_path(name)
    path.write_text(json.dumps(payload, indent=2))
    return {"name": path.stem, "path": str(path)}
