from export.records import project_records


def project_to_nationbuilder(df):
    """
    Compatibility wrapper around the generic export projection.
    """

    return project_records(df)
