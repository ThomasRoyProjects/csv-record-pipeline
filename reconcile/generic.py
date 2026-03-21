from __future__ import annotations

import pandas as pd
from difflib import SequenceMatcher


def _norm_text(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().casefold().split())


def _norm_postal(value) -> str:
    return "".join(ch for ch in _norm_text(value) if ch.isalnum())


def _norm_phone(value) -> str:
    return "".join(ch for ch in _norm_text(value) if ch.isdigit())


def _norm_email(value) -> str:
    return _norm_text(value)


def _first_initial(value) -> str:
    text = _norm_text(value)
    return text[:1]


def _similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def _build_match_explanation(reason: str, score: int, flags: dict) -> str:
    parts = [reason, f"score={score}"]
    if flags["id_matched"]:
        parts.append("id")
    if flags["name_matched"]:
        parts.append("name")
    elif flags["name_initial_matched"]:
        parts.append("name_initial")
    if flags["address_matched"]:
        parts.append("address")
    elif flags["address1_matched"]:
        parts.append("address1")
    if flags["postal_matched"]:
        parts.append("postal")
    if flags["email_matched"]:
        parts.append("email")
    if flags["phone_matched"]:
        parts.append("phone")
    if flags["unit_matched"]:
        parts.append("unit")
    if flags["unit_conflict"]:
        parts.append("unit_conflict")
    return " | ".join(parts)


def _score_candidate(primary_row: dict, candidate: dict) -> tuple[int, dict]:
    id_matched = bool(primary_row["id"] and primary_row["id"] == candidate["id"])
    first_exact = bool(primary_row["first"] and primary_row["first"] == candidate["first"])
    last_exact = bool(primary_row["last"] and primary_row["last"] == candidate["last"])
    name_exact = first_exact and last_exact
    name_initial = bool(
        primary_row["last"]
        and primary_row["last"] == candidate["last"]
        and _first_initial(primary_row["first"])
        and _first_initial(primary_row["first"]) == candidate["first_initial"]
    )
    address_exact = bool(primary_row["address"] and primary_row["address"] == candidate["address"])
    address1_exact = bool(primary_row["address1"] and primary_row["address1"] == candidate["address1"])
    unit_exact = bool(primary_row["address2"] and primary_row["address2"] == candidate["address2"])
    unit_conflict = bool(
        primary_row["address2"]
        and candidate["address2"]
        and primary_row["address2"] != candidate["address2"]
    )
    postal_exact = bool(primary_row["postal"] and primary_row["postal"] == candidate["postal"])
    email_exact = bool(primary_row["email"] and primary_row["email"] == candidate["email"])
    phone_exact = bool(primary_row["phone"] and primary_row["phone"] == candidate["phone"])
    first_similarity = _similarity(primary_row["first"], candidate["first"])
    last_similarity = _similarity(primary_row["last"], candidate["last"])
    address_similarity = _similarity(primary_row["address"], candidate["address"])
    address1_similarity = _similarity(primary_row["address1"], candidate["address1"])

    score = 0
    if id_matched:
        score += 130
    if last_exact:
        score += 35
    if first_exact:
        score += 30
    elif name_initial:
        score += 12
    if address_exact:
        score += 70
    elif address1_exact:
        score += 30
    if unit_exact:
        score += 18
    elif unit_conflict:
        score -= 35
    if postal_exact:
        score += 20
    if email_exact:
        score += 45
    if phone_exact:
        score += 35
    if not first_exact and first_similarity >= 0.88:
        score += 18
    if not last_exact and last_similarity >= 0.9:
        score += 18
    if not address_exact and address_similarity >= 0.9:
        score += 35
    elif not address1_exact and address1_similarity >= 0.9:
        score += 15

    return score, {
        "id_matched": id_matched,
        "name_matched": name_exact,
        "name_initial_matched": name_initial,
        "address_matched": address_exact,
        "address1_matched": address1_exact,
        "unit_matched": unit_exact,
        "unit_conflict": unit_conflict,
        "postal_matched": postal_exact,
        "email_matched": email_exact,
        "phone_matched": phone_exact,
        "first_similarity": first_similarity,
        "last_similarity": last_similarity,
        "address_similarity": address_similarity,
        "address1_similarity": address1_similarity,
    }


def _classify_best_match(
    score: int,
    flags: dict,
    *,
    confident_threshold: int,
    possible_threshold: int,
    review_threshold: int,
    strict_mode: bool = False,
) -> tuple[str, str]:
    if flags["unit_conflict"]:
        if flags["id_matched"]:
            return "REVIEW", "ID_MATCH_WITH_UNIT_CONFLICT"
        return "REVIEW", "ADDRESS_UNIT_CONFLICT"
    if flags["id_matched"] and flags["name_matched"] and flags["address_matched"]:
        return "CONFIDENT", "ID_NAME_ADDRESS_EXACT"
    if flags["id_matched"] and flags["name_matched"]:
        return "CONFIDENT", "ID_NAME_EXACT"
    if flags["name_matched"] and flags["address_matched"]:
        return "CONFIDENT", "NAME_ADDRESS_EXACT"
    if flags["email_matched"] and flags["name_matched"]:
        return "CONFIDENT", "EMAIL_NAME_EXACT"
    if flags["phone_matched"] and flags["name_matched"]:
        return "CONFIDENT", "PHONE_NAME_EXACT"
    if strict_mode:
        if score >= possible_threshold and flags["name_matched"] and (
            (flags["address1_matched"] and flags["postal_matched"])
            or (flags["email_matched"] and (flags["address1_matched"] or flags["phone_matched"]))
            or (flags["phone_matched"] and (flags["address1_matched"] or flags["postal_matched"]))
        ):
            return "POSSIBLE", "STRICT_NAME_WITH_STRONG_SUPPORT"
        if score >= review_threshold and (
            flags["name_matched"]
            or (flags["name_initial_matched"] and (flags["address1_matched"] or flags["postal_matched"]))
            or flags["id_matched"]
        ):
            return "REVIEW", "STRICT_REVIEW_REQUIRED"
        return "UNMATCHED", "NO_MATCH"
    if score >= possible_threshold and flags["name_matched"] and (
        flags["address1_matched"] or flags["postal_matched"] or flags["email_matched"] or flags["phone_matched"]
    ):
        return "POSSIBLE", "NAME_WITH_SUPPORTING_EVIDENCE"
    if score >= review_threshold and (flags["name_matched"] or flags["name_initial_matched"]) and (
        flags["postal_matched"] or flags["address1_matched"]
    ):
        return "REVIEW", "PARTIAL_NAME_WITH_ADDRESS_OR_POSTAL"
    if score >= confident_threshold and (
        (flags["name_matched"] and (flags["address1_matched"] or flags["postal_matched"]))
        or (flags["email_matched"] and flags["phone_matched"])
    ):
        return "CONFIDENT", "HIGH_SCORE_COMPOSITE_MATCH"
    if flags["id_matched"]:
        return "REVIEW", "ID_ONLY"
    return "UNMATCHED", "NO_MATCH"


def match_primary_to_reference(
    primary_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    *,
    primary_id_col: str = "person_id",
    reference_id_col: str = "person_id",
    primary_first_col: str = "first_name",
    reference_first_col: str = "first_name",
    primary_last_col: str = "last_name",
    reference_last_col: str = "last_name",
    primary_address_col: str | None = "_address_norm",
    reference_address_col: str | None = "_address_norm",
    primary_address1_col: str = "primary_address1",
    reference_address1_col: str = "primary_address1",
    primary_address2_col: str = "primary_address2",
    reference_address2_col: str = "primary_address2",
    primary_postal_col: str | None = None,
    reference_postal_col: str | None = None,
    primary_email_col: str = "email",
    reference_email_col: str = "email",
    primary_phone_col: str = "phone",
    reference_phone_col: str = "phone",
    confident_threshold: int = 160,
    possible_threshold: int = 120,
    review_threshold: int = 85,
    strict_mode: bool = False,
) -> pd.DataFrame:
    """
    Annotate each primary row with the best available reference match.

    The matcher now scores candidate records instead of relying solely on
    one or two exact checks, which is safer for "is this person already in my
    database?" workflows.
    """

    primary = primary_df.copy()
    reference = reference_df.copy()

    if primary_postal_col is None:
        primary_postal_col = "mail_zip" if "mail_zip" in primary.columns else "primary_zip"
    if reference_postal_col is None:
        reference_postal_col = "primary_zip" if "primary_zip" in reference.columns else "mail_zip"

    ref_records = []
    for idx, row in reference.iterrows():
        ref_records.append(
            {
                "_index": idx,
                "id": _norm_text(row.get(reference_id_col)),
                "first": _norm_text(row.get(reference_first_col)),
                "first_initial": _first_initial(row.get(reference_first_col)),
                "last": _norm_text(row.get(reference_last_col)),
                "address": _norm_text(row.get(reference_address_col)) if reference_address_col else "",
                "address1": _norm_text(row.get(reference_address1_col)),
                "address2": _norm_text(row.get(reference_address2_col)),
                "postal": _norm_postal(row.get(reference_postal_col)),
                "email": _norm_email(row.get(reference_email_col)),
                "phone": _norm_phone(row.get(reference_phone_col)),
            }
        )

    by_id: dict[str, list[dict]] = {}
    by_email: dict[str, list[dict]] = {}
    by_phone: dict[str, list[dict]] = {}
    by_last_postal: dict[tuple[str, str], list[dict]] = {}
    by_last_address1: dict[tuple[str, str], list[dict]] = {}
    by_last: dict[str, list[dict]] = {}
    by_postal: dict[str, list[dict]] = {}
    by_address1: dict[str, list[dict]] = {}

    for record in ref_records:
        if record["id"]:
            by_id.setdefault(record["id"], []).append(record)
        if record["email"]:
            by_email.setdefault(record["email"], []).append(record)
        if record["phone"]:
            by_phone.setdefault(record["phone"], []).append(record)
        if record["last"]:
            by_last.setdefault(record["last"], []).append(record)
        if record["postal"]:
            by_postal.setdefault(record["postal"], []).append(record)
        if record["address1"]:
            by_address1.setdefault(record["address1"], []).append(record)
        if record["last"] and record["postal"]:
            by_last_postal.setdefault((record["last"], record["postal"]), []).append(record)
        if record["last"] and record["address1"]:
            by_last_address1.setdefault((record["last"], record["address1"]), []).append(record)

    def classify_row(row):
        primary_row = {
            "id": _norm_text(row.get(primary_id_col)),
            "first": _norm_text(row.get(primary_first_col)),
            "last": _norm_text(row.get(primary_last_col)),
            "address": _norm_text(row.get(primary_address_col)) if primary_address_col else "",
            "address1": _norm_text(row.get(primary_address1_col)),
            "address2": _norm_text(row.get(primary_address2_col)),
            "postal": _norm_postal(row.get(primary_postal_col)),
            "email": _norm_email(row.get(primary_email_col)),
            "phone": _norm_phone(row.get(primary_phone_col)),
        }

        candidate_pool = []
        if primary_row["id"]:
            candidate_pool.extend(by_id.get(primary_row["id"], []))
        if primary_row["email"]:
            candidate_pool.extend(by_email.get(primary_row["email"], []))
        if primary_row["phone"]:
            candidate_pool.extend(by_phone.get(primary_row["phone"], []))
        if primary_row["last"] and primary_row["postal"]:
            candidate_pool.extend(by_last_postal.get((primary_row["last"], primary_row["postal"]), []))
        if primary_row["last"] and primary_row["address1"]:
            candidate_pool.extend(by_last_address1.get((primary_row["last"], primary_row["address1"]), []))
        if not candidate_pool and primary_row["last"]:
            candidate_pool.extend(by_last.get(primary_row["last"], [])[:250])
        if not candidate_pool and primary_row["postal"]:
            candidate_pool.extend(by_postal.get(primary_row["postal"], [])[:250])
        if not candidate_pool and primary_row["address1"]:
            candidate_pool.extend(by_address1.get(primary_row["address1"], [])[:250])

        seen = set()
        deduped_candidates = []
        for candidate in candidate_pool:
            if candidate["_index"] in seen:
                continue
            seen.add(candidate["_index"])
            deduped_candidates.append(candidate)

        best_candidate = None
        best_score = 0
        best_flags = {
            "id_matched": False,
            "name_matched": False,
            "name_initial_matched": False,
            "address_matched": False,
            "address1_matched": False,
            "unit_matched": False,
            "unit_conflict": False,
            "postal_matched": False,
            "email_matched": False,
            "phone_matched": False,
            "first_similarity": 0.0,
            "last_similarity": 0.0,
            "address_similarity": 0.0,
            "address1_similarity": 0.0,
        }

        for candidate in deduped_candidates:
            score, flags = _score_candidate(primary_row, candidate)
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_flags = flags

        status, reason = _classify_best_match(
            best_score,
            best_flags,
            confident_threshold=confident_threshold,
            possible_threshold=possible_threshold,
            review_threshold=review_threshold,
            strict_mode=strict_mode,
        )
        candidate_count = len(deduped_candidates)

        return pd.Series(
            {
                "_match_status": status,
                "_match_reason": reason,
                "_match_score": best_score,
                "_match_explanation": _build_match_explanation(reason, best_score, best_flags),
                "_matched_to_reference": status != "UNMATCHED",
                "_matched_confident": status == "CONFIDENT",
                "_matched_possible": status == "POSSIBLE",
                "_matched_review": status == "REVIEW",
                "_matched_reference_id": best_candidate["id"] if best_candidate else "",
                "_reference_match_count": candidate_count,
                "_id_matched": best_flags["id_matched"],
                "_name_matched": best_flags["name_matched"],
                "_name_initial_matched": best_flags["name_initial_matched"],
                "_address_matched": best_flags["address_matched"],
                "_address1_matched": best_flags["address1_matched"],
                "_postal_matched": best_flags["postal_matched"],
                "_email_matched": best_flags["email_matched"],
                "_phone_matched": best_flags["phone_matched"],
                "_first_similarity": best_flags["first_similarity"],
                "_last_similarity": best_flags["last_similarity"],
                "_address_similarity": best_flags["address_similarity"],
                "_address1_similarity": best_flags["address1_similarity"],
            }
        )

    flags = primary.apply(classify_row, axis=1)
    return pd.concat([primary, flags], axis=1)
