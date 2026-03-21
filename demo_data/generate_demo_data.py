#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parent

MATCHED_COUNT = 600
REVIEW_COUNT = 50
UNMATCHED_COUNT = 350
REFERENCE_ONLY_COUNT = 350
TOTAL_REFERENCE = MATCHED_COUNT + REVIEW_COUNT + REFERENCE_ONLY_COUNT
TOTAL_PRIMARY = MATCHED_COUNT + REVIEW_COUNT + UNMATCHED_COUNT
LEGACY_POSITIVE_COUNT = 800
LEGACY_ZERO_COUNT = 200
SPLIT_COUNT = 1000


def write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def letter(index: int) -> str:
    return chr(ord("A") + (index % 26))


def city(index: int) -> str:
    return ["Victoria", "Esquimalt", "Saanich", "Langford"][index % 4]


def postal(index: int) -> str:
    return f"V8V{index % 10}{letter(index)}{(index * 3) % 10}"


def primary_address(index: int) -> str:
    return f"{100 + index} Harbour Street"


def first_name(index: int) -> str:
    return f"Alex{index:03d}"


def last_name(index: int) -> str:
    return f"Taylor{index:03d}"


def phone(index: int) -> str:
    return f"250-555-{1000 + index:04d}"


def email(prefix: str, index: int) -> str:
    return f"{prefix}{index:04d}@example.test"


def build_match_reference_rows() -> list[list[str]]:
    rows: list[list[str]] = []
    for i in range(1, MATCHED_COUNT + 1):
        rows.append(
            [
                f"M{i:04d}",
                first_name(i),
                "",
                last_name(i),
                primary_address(i),
                "",
                city(i),
                "BC",
                postal(i),
                phone(i),
                email("match", i),
                "core" if i % 2 == 0 else "field",
            ]
        )
    for i in range(1, REVIEW_COUNT + 1):
        idx = 5000 + i
        rows.append(
            [
                f"RV{i:04d}",
                f"A{i:04d}",
                "",
                last_name(idx),
                primary_address(idx),
                "",
                city(idx),
                "BC",
                postal(idx),
                "",
                "",
                "review",
            ]
        )
    for i in range(1, REFERENCE_ONLY_COUNT + 1):
        idx = 7000 + i
        rows.append(
            [
                f"X{i:04d}",
                first_name(idx),
                "",
                last_name(idx),
                primary_address(idx),
                "",
                city(idx),
                "BC",
                postal(idx),
                phone(idx),
                email("reference", idx),
                "lookup",
            ]
        )
    return rows


def build_match_primary_rows() -> list[list[str]]:
    rows: list[list[str]] = []
    for i in range(1, MATCHED_COUNT + 1):
        rows.append(
            [
                f"M{i:04d}",
                first_name(i),
                "",
                last_name(i),
                primary_address(i),
                "",
                city(i),
                "BC",
                postal(i),
                email("primary", i),
                phone(i),
                f"2027/{(i % 12) + 1:02d}/28",
            ]
        )
    for i in range(1, REVIEW_COUNT + 1):
        idx = 5000 + i
        rows.append(
            [
                f"PR{i:04d}",
                first_name(idx),
                "",
                last_name(idx),
                primary_address(idx),
                "",
                city(idx),
                "BC",
                postal(idx),
                "",
                "",
                "",
            ]
        )
    for i in range(1, UNMATCHED_COUNT + 1):
        idx = 9000 + i
        rows.append(
            [
                f"N{i:04d}",
                first_name(idx),
                "",
                last_name(idx),
                primary_address(idx),
                "",
                city(idx),
                "BC",
                postal(idx),
                email("unmatched", idx),
                phone(idx),
                "",
            ]
        )
    return rows


def build_random_import_rows() -> list[list[str]]:
    rows: list[list[str]] = []
    for source_rows, prefix in ((build_match_primary_rows()[:MATCHED_COUNT], "matched"), (build_match_primary_rows()[MATCHED_COUNT:], "new")):
        for row in source_rows:
            rows.append(
                [
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                ]
            )
    return rows


def build_profiled_import_rows() -> list[list[str]]:
    rows: list[list[str]] = []
    all_rows = build_match_primary_rows()
    for row in all_rows:
        number, street_name, street_type = row[4].split(" ", 2)
        unit = "7" if row[0].endswith("5") else ""
        rows.append(
            [
                row[0],
                row[1],
                row[2],
                row[3],
                number,
                street_name,
                street_type,
                unit,
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
            ]
        )
    return rows


def build_process_members_rows() -> list[list[str]]:
    rows: list[list[str]] = []
    base_rows = build_match_primary_rows()
    for row in base_rows:
        rows.append(
            [
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5] or ("4" if row[0] == "M0005" else ""),
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                "2025/01/15",
                row[11] or "2026/12/31",
            ]
        )
    rows.insert(1, rows[0][:])
    return rows


def build_legacy_members_rows() -> list[list[str]]:
    rows: list[list[str]] = []
    for i in range(1, LEGACY_POSITIVE_COUNT + LEGACY_ZERO_COUNT + 1):
        amount = "0" if i > LEGACY_POSITIVE_COUNT else f"{25 + (i % 7) * 10:.2f}"
        rows.append(
            [
                f"M{i:04d}",
                first_name(i),
                "",
                last_name(i),
                f"{100 + i} Legacy Avenue",
                city(i),
                "BC",
                postal(i),
                f"2025-{(i % 12) + 1:02d}-15",
                amount,
            ]
        )
    return rows


def build_split_rows() -> list[list[str]]:
    rows: list[list[str]] = []
    for i in range(1, SPLIT_COUNT + 1):
        rows.append([f"S{i:04d}", first_name(i), last_name(i), city(i)])
    return rows


def main() -> None:
    match_reference_headers = [
        "userID",
        "first_name",
        "middle_name",
        "last_name",
        "FullResidentialAddress",
        "primary_address2",
        "primary_city",
        "primary_state",
        "primary_zip",
        "PhoneNumber",
        "email",
        "reference_segment",
    ]
    match_primary_headers = [
        "person_id",
        "first_name",
        "middle_name",
        "last_name",
        "primary_address1",
        "primary_address2",
        "mail_city",
        "mail_state",
        "mail_zip",
        "email",
        "phone_number",
        "membership_end_date",
    ]
    random_headers = [
        "ExternalID",
        "GivenName",
        "MiddleInitial",
        "Surname",
        "StreetLine",
        "UnitNo",
        "Town",
        "ProvinceCode",
        "Postal",
        "EmailAddress",
        "MobilePhone",
        "ExpiryLabel",
    ]
    profiled_headers = [
        "RecordID",
        "GivenName",
        "MiddleInitial",
        "Surname",
        "StreetNumber",
        "StreetName",
        "StreetType",
        "Unit",
        "City",
        "Province",
        "PostalCode",
        "EmailAddress",
        "MobilePhone",
    ]
    process_headers = [
        "person_id",
        "first_name",
        "middle_name",
        "last_name",
        "primary_address1",
        "primary_address2",
        "mail_city",
        "mail_state",
        "mail_zip",
        "email",
        "phone_number",
        "memb_start_date",
        "memb_exp_date",
    ]
    legacy_headers = [
        "userID",
        "FirstName",
        "MiddleName",
        "LastName",
        "FullResidentialAddress",
        "City",
        "Province",
        "PostalCode",
        "LastDonationDate",
        "LastDonationReceiptableAmount",
    ]
    split_headers = ["person_id", "first_name", "last_name", "city"]

    match_reference_rows = build_match_reference_rows()
    match_primary_rows = build_match_primary_rows()

    write_csv(ROOT / "demo_match_reference.csv", match_reference_headers, match_reference_rows)
    write_csv(ROOT / "demo_match_primary.csv", match_primary_headers, match_primary_rows)
    write_csv(ROOT / "demo_reference_people.csv", match_reference_headers, match_reference_rows)
    write_csv(ROOT / "demo_primary_people.csv", match_primary_headers, match_primary_rows)
    write_csv(ROOT / "demo_random_import.csv", random_headers, build_random_import_rows())
    write_csv(ROOT / "demo_profiled_import.csv", profiled_headers, build_profiled_import_rows())
    write_csv(ROOT / "demo_process_members.csv", process_headers, build_process_members_rows())
    write_csv(ROOT / "demo_legacy_members.csv", legacy_headers, build_legacy_members_rows())
    write_csv(ROOT / "demo_split_source.csv", split_headers, build_split_rows())


if __name__ == "__main__":
    main()
