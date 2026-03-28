import pandas as pd

# Clean raw value
def clean_raw(x):
    if pd.isna(x):
        return None

    x = str(x).strip()

    if x in ['', '-', 'NULL', 'None', 'N/A']:
        return None

    return x


# Parse Date
def parse_date(x):
    x = clean_raw(x)

    if x is None:
        return pd.NaT

    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y"
    ]

    for fmt in formats:
        try:
            return pd.to_datetime(x, format=fmt)
        except:
            continue

    return pd.NaT


# Parse Datetime
def parse_datetime(x):
    x = clean_raw(x)

    if x is None:
        return pd.NaT

    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M"
    ]

    for fmt in formats:
        try:
            return pd.to_datetime(x, format=fmt)
        except:
            continue

    return pd.NaT


# Clean date columns
def clean_date_column(series):
    return series.apply(parse_date)


def clean_datetime_column(series):
    return series.apply(parse_datetime)