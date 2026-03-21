import re

def normalize_date_regex(val):
    if not val:
        return val
    v = re.sub(r"[^\d/-]", "", str(val))
    if re.fullmatch(r"(\d{4})(\d{2})(\d{2})", v):
        return f"{v[:4]}/{v[4:6]}/{v[6:]}"
    if re.fullmatch(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", v):
        y,m,d = re.split("[-/]", v)
        return f"{y}/{m.zfill(2)}/{d.zfill(2)}"
    if re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", v):
        m,d,y = v.split("/")
        return f"{y}/{m.zfill(2)}/{d.zfill(2)}"
    return val

def fix_date_columns(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(normalize_date_regex)
    return df
