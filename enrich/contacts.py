import re

def split_multi(val):
    if not val:
        return []
    return [v.strip() for v in re.split(r"[;,]", str(val)) if v.strip()]

def normalize_email(e):
    return e.strip().lower()

def normalize_phone(p):
    return re.sub(r"\D", "", p)

def enrich_contacts(df):
    """
    Union emails and phones across all known fields.
    """

    email_fields = [
        "email",
        "email1",
        "email2",
        "email3",
        "email4",
    ]

    phone_fields = [
        "phone",
        "home_phone",
        "work_phone",
        "mobile_phone",
    ]

    def collect_emails(r):
        emails = set()
        for f in email_fields:
            for e in split_multi(r.get(f)):
                emails.add(normalize_email(e))
        return sorted(emails)

    def collect_phones(r):
        phones = set()
        for f in phone_fields:
            for p in split_multi(r.get(f)):
                phones.add(normalize_phone(p))
        return sorted(phones)

    df = df.copy()

    df["emails_all"] = df.apply(collect_emails, axis=1)
    df["phones_all"] = df.apply(collect_phones, axis=1)

    df["email_count"] = df["emails_all"].apply(len)
    df["phone_count"] = df["phones_all"].apply(len)

    return df
