import pandas as pd


def _normalize_name(series):
    return series.fillna("").astype(str).str.strip().str.casefold()


def _normalize_address(series):
    return series.fillna("").astype(str).str.strip().str.casefold()


def flag_identity_matches(
    primary_df,
    reference_df,
    id_col,
    first_col,
    last_col,
    address_col=None,
):
    df = primary_df.copy()

    reference_ids = set(reference_df[id_col].fillna("").astype(str))
    df["_exists_by_external_id"] = df[id_col].fillna("").astype(str).isin(reference_ids)

    df["_exists_by_name_address"] = False
    df["_exists_by_name_address1"] = False

    if address_col and address_col in df.columns and address_col in reference_df.columns:
        reference_name_addr = set(
            zip(
                _normalize_name(reference_df[first_col]),
                _normalize_name(reference_df[last_col]),
                _normalize_address(reference_df[address_col]),
            )
        )

        df["_exists_by_name_address"] = df.apply(
            lambda row: (
                _normalize_name(pd.Series([row[first_col]])).iloc[0],
                _normalize_name(pd.Series([row[last_col]])).iloc[0],
                _normalize_address(pd.Series([row[address_col]])).iloc[0],
            )
            in reference_name_addr,
            axis=1,
        )

    if "primary_address1" in df.columns and "primary_address1" in reference_df.columns:
        reference_name_addr1 = set(
            zip(
                _normalize_name(reference_df[first_col]),
                _normalize_name(reference_df[last_col]),
                _normalize_address(reference_df["primary_address1"]),
            )
        )

        df["_exists_by_name_address1"] = df.apply(
            lambda row: (
                _normalize_name(pd.Series([row[first_col]])).iloc[0],
                _normalize_name(pd.Series([row[last_col]])).iloc[0],
                _normalize_address(pd.Series([row["primary_address1"]])).iloc[0],
            )
            in reference_name_addr1,
            axis=1,
        )

    return df
