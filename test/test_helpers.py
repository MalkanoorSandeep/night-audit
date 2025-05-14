import pandas as pd
from night_audit_etl_pipeline.helpers import (
    clean_column_names,
    clean_numeric_column,
    add_metadata,
    safe_float,
    convert_date,
    is_strictly_numeric,
    extract_amount
)


def test_clean_column_names_basic():
    df = pd.DataFrame(columns=["Guest Name", "Room/Rate"])
    result = clean_column_names(df)
    assert result.columns.tolist() == ["guest_name", "room_rate"]


def test_clean_column_names_with_replacements():
    df = pd.DataFrame(columns=["Total$", "Count/Value"])
    result = clean_column_names(df, replacements={"total$": "total", "count_value": "count"})
    assert result.columns.tolist() == ["total", "count"]




def test_clean_numeric_column():
    df = pd.DataFrame({
        "amount": ["1,000.00", "(500.00)", "250.75"]
    })
    df = clean_numeric_column(df, ["amount"])
    assert df["amount"].tolist() == [1000.00, -500.00, 250.75]


def test_add_metadata():
    df = pd.DataFrame({"a": [1]})
    df = add_metadata(df, prop_code="P001", user_id="user1", report_date="01/01/2025", business_date="01/01/2025")
    assert df["property_code"].iloc[0] == "P001"
    assert df["user"].iloc[0] == "user1"
    assert str(df["report_date"].iloc[0]) == "2025-01-01"
    assert str(df["business_date"].iloc[0]) == "2025-01-01"


def test_safe_float_valid():
    assert safe_float("1,234.56") == 1234.56
    assert safe_float("(1,000.00)") == -1000.00
    assert safe_float("0") == 0.0
    assert safe_float("   99.99 ") == 99.99


def test_safe_float_invalid():
    assert safe_float("") is None
    assert safe_float("abc") is None
    assert safe_float(None) is None


def test_convert_date_valid():
    assert str(convert_date("01/01/2025")) == "2025-01-01"
    assert str(convert_date("01-01-2025")) == "2025-01-01"
    assert str(convert_date("01/01/25")) == "2025-01-01"


def test_convert_date_invalid():
    assert convert_date("bad-date") is None
    assert convert_date("") is None
    assert convert_date(None) is None


def test_is_strictly_numeric():
    assert is_strictly_numeric("1,000.00")
    assert is_strictly_numeric("(1,000.00)")
    assert is_strictly_numeric("0.00")
    assert not is_strictly_numeric("abc")
    assert not is_strictly_numeric("123abc")


def test_extract_amount():
    assert extract_amount("1,000.00") == 1000.00
    assert extract_amount("(1,000.00)") == -1000.00
    assert extract_amount("500") == 500.00
