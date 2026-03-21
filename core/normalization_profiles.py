from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pandas as pd
import yaml


NORMALIZATION_PROFILES_DIR = Path(__file__).resolve().parent.parent / "normalization_profiles"


def load_normalization_profile(profile_name_or_path: str | Path) -> dict:
    path = Path(profile_name_or_path)
    if not path.is_absolute():
        if path.suffix in {".yaml", ".yml"}:
            path = NORMALIZATION_PROFILES_DIR / path
        else:
            path = NORMALIZATION_PROFILES_DIR / f"{path}.yaml"
    with open(path) as handle:
        return yaml.safe_load(handle) or {}


def save_normalization_profile(profile_name: str, profile: dict) -> dict:
    safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in profile_name.strip()).strip("_")
    if not safe_name:
        raise ValueError("Profile name is required")

    NORMALIZATION_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    path = NORMALIZATION_PROFILES_DIR / f"{safe_name}.yaml"
    path.write_text(yaml.safe_dump(profile, sort_keys=False))
    return {"name": safe_name, "path": str(path), "profile": profile}


def _join_columns(df: pd.DataFrame, columns: list[str], separator: str = " ", strip: bool = True) -> pd.Series:
    existing = [column for column in columns if column in df.columns]
    if not existing:
        return pd.Series([""] * len(df), index=df.index)

    def combine(row) -> str:
        values = []
        for column in existing:
            value = row.get(column, "")
            if value is None:
                value = ""
            text = str(value)
            if strip:
                text = text.strip()
            if text:
                values.append(text)
        return separator.join(values)

    return df.apply(combine, axis=1)


def apply_normalization_profile(df: pd.DataFrame, normalization_profile: dict | None) -> pd.DataFrame:
    if not normalization_profile:
        return df

    df = df.copy()
    derive_cfg = deepcopy(normalization_profile.get("derive", {}))

    for target_column, config in derive_cfg.items():
        if isinstance(config, str):
            if config in df.columns:
                df[target_column] = df[config]
            continue

        strategy = config.get("strategy", "copy")
        if strategy == "copy":
            source = config.get("source")
            if source in df.columns:
                df[target_column] = df[source]
        elif strategy == "join":
            df[target_column] = _join_columns(
                df,
                config.get("columns", []),
                separator=config.get("separator", " "),
                strip=config.get("strip", True),
            )
        elif strategy == "coalesce":
            sources = config.get("columns", [])
            df[target_column] = ""
            for source in sources:
                if source not in df.columns:
                    continue
                empty_mask = df[target_column].astype(str).str.strip() == ""
                df.loc[empty_mask, target_column] = df.loc[empty_mask, source]
        else:
            raise ValueError(f"Unsupported normalization strategy: {strategy}")

    return df


def apply_strict_text_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply conservative text cleanup across the frame for comparison-oriented workflows.

    This intentionally:
    - strips leading/trailing whitespace
    - casefolds text for case-insensitive matching
    - collapses repeated internal whitespace
    """
    df = df.copy()
    for column in df.columns:
        series = df[column]
        if series.dtype == object:
            df[column] = (
                series.fillna("")
                .astype(str)
                .str.strip()
                .str.casefold()
                .str.replace(r"\s+", " ", regex=True)
            )
    return df
