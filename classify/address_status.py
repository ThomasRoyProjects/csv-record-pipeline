def classify_address_status(df):
    """
    Classify address status based on reconciliation signals.
    Defensive: works even if reconcile stage was skipped.
    """

    def classify_row(r):
        if "_matched_to_voter" not in r:
            return "NOT_COMPARED"

        if not r.get("_matched_to_voter", False):
            return "NOT_FOUND_IN_VOTERS"

        # Matched by ID
        address_match = r.get("_address_matched", False)

        if address_match:
            return "SAME_ADDRESS_2024"

        return "MOVED_WITHIN_EDA"

    df = df.copy()
    df["address_status"] = df.apply(classify_row, axis=1)
    return df
