def score_priority(df):
    """
    Compute priority score and band.
    """

    def score_row(r):
        score = 0

        if r.get("address_status") == "SAME_ADDRESS_2024":
            score += 30
        elif r.get("address_status") == "MOVED_WITHIN_EDA":
            score += 20

        score += min(r.get("email_count", 0), 3) * 5
        score += min(r.get("phone_count", 0), 3) * 5

        if str(r.get("is_donor", "")).lower() in ("true", "1", "yes"):
            score += 50

        return score

    def band(score):
        if score >= 70:
            return "HIGH"
        if score >= 40:
            return "MEDIUM"
        return "LOW"

    df = df.copy()
    df["priority_score"] = df.apply(score_row, axis=1)
    df["priority_band"] = df["priority_score"].apply(band)

    return df
