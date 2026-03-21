def project_records(df):
    """
    Generic record export projection.

    For now this remains a passthrough, but the generic name avoids coupling
    the project to one downstream platform.
    """

    return df.copy()
