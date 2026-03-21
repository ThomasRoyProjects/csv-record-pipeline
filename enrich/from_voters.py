import pandas as pd


def enrich_from_voters(members, voters, join_on, take_columns):
    """
    Left-join voter data onto members using a shared ID.
    Only specified columns are added.
    No rows are dropped or modified.
    """

    cols = [join_on] + take_columns
    voters_subset = voters[cols].drop_duplicates(join_on)

    enriched = members.merge(
        voters_subset,
        on=join_on,
        how="left",
        suffixes=("", "_from_voters")
    )

    return enriched
