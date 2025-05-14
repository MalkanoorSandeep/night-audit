import re
import pandas as pd
from datetime import datetime


def clean_column_names(df, replacements=None):
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("/", "_")
    if replacements:
        for old, new in replacements.items():
            df.columns = df.columns.str.replace(old, new)
    return df


def clean_numeric_column(df, columns):
    for col in columns:
        df[col] = df[col].str.replace(",", "").str.replace("(", "-").str.replace(")", "").astype(float)
    return df


def add_metadata(df, prop_code=None, user_id=None, report_date=None, business_date=None):
    if prop_code: df["property_code"] = prop_code
    if user_id: df["user"] = user_id
    if report_date: df["report_date"] = pd.to_datetime(report_date).date()
    if business_date: df["business_date"] = pd.to_datetime(business_date).date()
    return df


def safe_float(val):
    try:
        val = val.strip().replace(",", "").replace("(", "-").replace(")", "")
        return float(val) if val else None
    except:
        return None


def convert_date(val):
    if not val:
        return None
    for fmt in ['%m/%d/%Y', '%m/%d/%y', '%m-%d-%Y']:
        try:
            return datetime.strptime(val, fmt).date()
        except:
            continue
    return None


def is_strictly_numeric(val):
    return bool(re.fullmatch(r"\(?-?\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?", val.strip()))


def extract_amount(text):
    text = text.replace(',', '')
    if '(' in text and ')' in text:
        return -float(text.replace('(', '').replace(')', ''))
    return float(text)
