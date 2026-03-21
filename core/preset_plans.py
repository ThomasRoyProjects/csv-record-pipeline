from __future__ import annotations

from pathlib import Path


def build_compare_records_plan(
    *,
    bundle_rules: dict[str, dict],
    base_dir: str | Path,
    normalize_dates: bool,
    reconcile_identity: bool,
    match_cfg: dict,
) -> list[tuple[str, dict | None]]:
    stages_to_run: list[tuple[str, dict | None]] = []

    if normalize_dates:
        stages_to_run.append(
            ("normalize_date_columns", {"dataset_role": "primary", "columns": ["memb_start_date", "memb_exp_date"]})
        )

    if reconcile_identity:
        stages_to_run.append(
            ("match_records", {"primary_role": "primary", "reference_role": "reference", "match_config": match_cfg})
        )
    else:
        stages_to_run.append(
            (
                "flag_reference_identity",
                {
                    "primary_role": "primary",
                    "reference_role": "reference",
                    "id_col": "person_id",
                    "first_col": "first_name",
                    "last_col": "last_name",
                    "address_col": "_address_norm",
                    "set_match_fields": True,
                },
            )
        )

    stages_to_run.append(
        (
            "write_records_bundle",
            {
                "dataset_role": "primary",
                "base_dir": str(base_dir),
                "outputs": bundle_rules,
            },
        )
    )
    return stages_to_run


def build_match_records_plan(
    *,
    bundle_rules: dict[str, dict],
    base_dir: str | Path,
    match_cfg: dict,
) -> list[tuple[str, dict | None]]:
    return [
        ("match_records", {"primary_role": "primary", "reference_role": "reference", "match_config": match_cfg}),
        (
            "write_records_bundle",
            {
                "dataset_role": "primary",
                "base_dir": str(base_dir),
                "outputs": bundle_rules,
            },
        ),
    ]


def build_enrich_records_plan(*, profile: dict, base_dir: str | Path) -> list[tuple[str, dict | None]]:
    enrich_cfg = (
        profile.get("enrich", {}).get("from_reference")
        or profile.get("enrich", {}).get("from_voters")
    )

    stages_to_run: list[tuple[str, dict | None]] = []
    if profile["stages"].get("enrich_from_voters") and enrich_cfg:
        stages_to_run.append(
            (
                "join_reference_fields",
                {
                    "primary_role": "primary",
                    "reference_role": "reference",
                    "join_on": enrich_cfg["join_on"],
                    "take_columns": enrich_cfg["take_columns"],
                },
            )
        )

    stages_to_run.extend(
        [
            ("project_records", {"source_role": "primary", "artifact_name": "enriched_records"}),
            (
                "write_records_output",
                {
                    "source_kind": "artifact",
                    "source_name": "enriched_records",
                    "output_key": "enriched_records",
                    "base_dir": str(base_dir),
                    "filename": "enriched_records.csv",
                },
            ),
        ]
    )
    return stages_to_run


def build_extract_projection_plan(*, projection_cfg: dict) -> list[tuple[str, dict | None]]:
    output_path = Path(projection_cfg["path"])
    return [
        (
            "extract_projection",
            {
                "dataset_role": "source",
                "artifact_name": "projection",
                "columns": projection_cfg["columns"],
                "format_created_at": True,
                "format_amount": True,
                "drop_non_positive_amounts": True,
            },
        ),
        (
            "write_records_output",
            {
                "source_kind": "artifact",
                "source_name": "projection",
                "output_key": "projection",
                "base_dir": str(output_path.parent),
                "filename": output_path.name,
            },
        ),
    ]


def build_split_plan(*, out_cfg: dict, split_cfg: dict | None = None) -> list[tuple[str, dict | None]]:
    split_cfg = split_cfg or {}
    return [
        (
            "split_rows",
            {
                "dataset_role": "source",
                "method": split_cfg.get("method", "alternate"),
                "start_file": split_cfg.get("start_file", "A"),
            },
        ),
        (
            "write_records_output",
            {
                "source_kind": "artifact",
                "source_name": "split_a",
                "output_key": "path_a",
                "base_dir": str(Path(out_cfg["path_a"]).parent),
                "filename": Path(out_cfg["path_a"]).name,
            },
        ),
        (
            "write_records_output",
            {
                "source_kind": "artifact",
                "source_name": "split_b",
                "output_key": "path_b",
                "base_dir": str(Path(out_cfg["path_b"]).parent),
                "filename": Path(out_cfg["path_b"]).name,
            },
        ),
    ]


def build_process_full_records_plan(
    *,
    profile: dict,
    primary_address_columns: dict[str, str],
    reference_address_columns: dict[str, str],
    output_path: str | Path,
    match_cfg: dict | None = None,
) -> list[tuple[str, dict | None]]:
    stages_to_run: list[tuple[str, dict | None]] = []

    if profile["stages"].get("normalize"):
        stages_to_run.extend(
            [
                ("normalize_date_columns", {"dataset_role": "primary", "columns": ["memb_start_date", "memb_exp_date"]}),
                ("normalize_addresses", {"dataset_role": "primary", "mode": "member", **primary_address_columns}),
                ("normalize_addresses", {"dataset_role": "reference", "mode": "voter", **reference_address_columns}),
            ]
        )

    if profile["stages"].get("dedupe"):
        stages_to_run.append(("dedupe_records", {"dataset_role": "primary"}))

    if profile["stages"].get("reconcile") and match_cfg is not None:
        stages_to_run.append(
            (
                "match_records",
                {
                    "primary_role": "primary",
                    "reference_role": "reference",
                    "match_config": match_cfg,
                    "mirror_matched_to": "_matched_to_voter",
                },
            )
        )

    if profile["stages"].get("classify_address_status"):
        stages_to_run.append(("classify_address_status", {"dataset_role": "primary"}))

    if profile["stages"].get("enrich_contacts"):
        stages_to_run.append(("aggregate_contacts", {"dataset_role": "primary"}))

    if profile["stages"].get("score_priority"):
        stages_to_run.append(("score_priority", {"dataset_role": "primary"}))

    output_path = Path(output_path)
    stages_to_run.extend(
        [
            ("project_records", {"source_role": "primary", "artifact_name": "processed_records"}),
            (
                "write_records_output",
                {
                    "source_kind": "artifact",
                    "source_name": "processed_records",
                    "output_key": "processed_records",
                    "base_dir": str(output_path.parent),
                    "filename": output_path.name,
                },
            ),
        ]
    )

    return stages_to_run
