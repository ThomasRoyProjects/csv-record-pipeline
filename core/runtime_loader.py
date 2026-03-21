from __future__ import annotations

from typing import Iterable

import pandas as pd

from core.normalization_profiles import apply_normalization_profile, apply_strict_text_cleanup, load_normalization_profile
from dataio.csv import load_csv_safe


def _mapping_sources(source) -> list[str]:
    if isinstance(source, list):
        return [item for item in source if item]
    if source:
        return [source]
    return []


def _coalesce_columns(df: pd.DataFrame, sources: list[str]) -> pd.Series:
    if not sources:
        return pd.Series([""] * len(df), index=df.index, dtype="object")

    result = pd.Series([""] * len(df), index=df.index, dtype="object")
    for source in sources:
        if source not in df.columns:
            continue
        source_values = df[source]
        if isinstance(source_values, pd.DataFrame):
            source_values = source_values.iloc[:, 0]
        existing = result.fillna("").astype(str).str.strip()
        result = result.where(existing != "", source_values)
    return result


def resolve_columns(df: pd.DataFrame, column_map: dict | None) -> pd.DataFrame:
    if not column_map:
        return df
    df = df.copy()
    for canonical, source in column_map.items():
        sources = _mapping_sources(source)
        if not sources:
            continue
        if len(sources) == 1 and sources[0] == canonical:
            continue

        if len(sources) == 1 and canonical not in df.columns and sources[0] in df.columns:
            df = df.rename(columns={sources[0]: canonical})
            continue

        source_values = _coalesce_columns(df, sources)
        if canonical in df.columns:
            canonical_values = df[canonical]
            if isinstance(canonical_values, pd.DataFrame):
                canonical_values = canonical_values.iloc[:, 0]
            canonical_text = canonical_values.fillna("").astype(str).str.strip()
            df[canonical] = canonical_values.where(canonical_text != "", source_values)
        else:
            df[canonical] = source_values
    return df


def load_many(paths: Iterable[str]) -> pd.DataFrame:
    frames = [load_csv_safe(path) for path in paths]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_dataset(dataset_cfg: dict) -> pd.DataFrame:
    if "paths" in dataset_cfg:
        df = load_many(dataset_cfg["paths"])
    elif "path" in dataset_cfg:
        df = load_csv_safe(dataset_cfg["path"])
    else:
        raise ValueError("Dataset config must include 'path' or 'paths'")

    normalization_profile = dataset_cfg.get("normalization_profile")
    if normalization_profile:
        if isinstance(normalization_profile, str):
            normalization_profile = load_normalization_profile(normalization_profile)
        df = apply_normalization_profile(df, normalization_profile)

    if dataset_cfg.get("strict_text_cleanup"):
        df = apply_strict_text_cleanup(df)

    return resolve_columns(df, dataset_cfg.get("columns"))


def load_runtime_datasets(runtime: dict) -> dict[str, pd.DataFrame]:
    return {role: load_dataset(dataset_cfg) for role, dataset_cfg in runtime.get("inputs", {}).items()}
