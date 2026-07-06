
import pandas as pd
import pytest

from pipeline import (
    DEFAULT_EMAIL,
    clean_customers,
    clean_orders,
    convert_to_usd,
    filter_invalid_orders,
    standardize_phone,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("+1 (555) 123-4567", "15551234567"),  # spec example
        ("555-987-6543", "5559876543"),         # dashes
        ("(555) 333 4444", "5553334444"),        # parens + spaces
        ("+44 20 7123 1234", "442071231234"),    # international
        ("Ext 444", "444"),                       # leading letters stripped
        ("1-800-555-DINO", "1800555"),            # trailing letters stripped
        ("15551234567", "15551234567"),           # already clean, unchanged
        ("", ""),                                  # empty stays empty
        (None, ""),                                # missing -> empty
    ],
)
def test_standardize_phone(raw, expected):
    assert standardize_phone(raw) == expected


def test_standardize_phone_handles_nan():
    assert standardize_phone(float("nan")) == ""


# ---------------------------------------------------------------------------
# convert_to_usd  (the currency-conversion logic)
# ---------------------------------------------------------------------------

@pytest.fixture
def rates():
    return pd.DataFrame(
        {
            "currency": ["EUR", "EUR", "JPY"],
            "rate_to_usd": [1.1, 1.12, 0.007],
            "date": ["2023-05-01", "2023-05-02", "2023-05-01"],
        }
    )


def _order(currency, order_date, total_amount, order_id=1):
    return pd.DataFrame(
        {
            "order_id": [order_id],
            "order_date": [order_date],
            "total_amount": [total_amount],
            "currency": [currency],
        }
    )


def test_convert_uses_matching_daily_rate(rates):
    out = convert_to_usd(_order("EUR", "2023-05-01", 200.0), rates)
    assert out.loc[0, "usd_amount"] == 220.0  # 200 * 1.1


def test_convert_picks_rate_for_the_right_date(rates):
    out = convert_to_usd(_order("EUR", "2023-05-02", 300.0), rates)
    assert out.loc[0, "usd_amount"] == 336.0  # 300 * 1.12, not 1.1


def test_convert_usd_passthrough(rates):
    out = convert_to_usd(_order("USD", "2023-05-01", 150.0), rates)
    assert out.loc[0, "usd_amount"] == 150.0  # USD absent from rates -> rate 1


def test_convert_missing_rate_falls_back_to_usd(rates):
    # GBP has no rate anywhere -> assumed already USD (rate 1).
    out = convert_to_usd(_order("GBP", "2023-05-08", 500.0), rates)
    assert out.loc[0, "usd_amount"] == 500.0


def test_convert_null_currency_falls_back_to_usd(rates):
    out = convert_to_usd(_order(None, "2023-05-05", 120.0), rates)
    assert out.loc[0, "usd_amount"] == 120.0


def test_convert_rounds_to_two_decimals():
    rates = pd.DataFrame(
        {"currency": ["EUR"], "rate_to_usd": [1.13333], "date": ["2023-05-01"]}
    )
    out = convert_to_usd(_order("EUR", "2023-05-01", 100.0), rates)
    assert out.loc[0, "usd_amount"] == 113.33  # 113.333 rounded


def test_convert_keeps_every_order_row(rates):
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "order_date": ["2023-05-01", "2023-05-08", "2023-05-05"],
            "total_amount": [200.0, 500.0, 120.0],
            "currency": ["EUR", "GBP", None],
        }
    )
    out = convert_to_usd(orders, rates)
    assert len(out) == 3
    assert "usd_amount" in out.columns
    assert "rate_to_usd" not in out.columns



def test_filter_drops_nonpositive_and_null_amounts():
    orders = pd.DataFrame(
        {"order_id": [1, 2, 3, 4, 5], "total_amount": [150.0, -50.0, 0.0, None, 99.99]}
    )
    out = filter_invalid_orders(orders)
    assert list(out["order_id"]) == [1, 5]


# ---------------------------------------------------------------------------
# clean_customers
# ---------------------------------------------------------------------------

def test_clean_customers_dedup_keeps_latest_signup():
    customers = pd.DataFrame(
        {
            "customer_id": [1, 1],
            "full_name": ["Alice Smith", "Alice Smith"],
            "email": ["alice@example.com", "alice.smith@example.com"],
            "phone": ["+1 (555) 123-4567", "15551234567"],
            "signup_date": ["2023-01-15", "2023-06-01"],
        }
    )
    out = clean_customers(customers)
    assert len(out) == 1
    assert out.loc[0, "signup_date"] == "2023-06-01"
    assert out.loc[0, "email"] == "alice.smith@example.com"


def test_clean_customers_fills_missing_email_and_phone():
    customers = pd.DataFrame(
        {
            "customer_id": [8],
            "full_name": ["Hannah Abbott"],
            "email": [None],
            "phone": [None],
            "signup_date": ["2023-07-01"],
        }
    )
    out = clean_customers(customers)
    assert out.loc[0, "email"] == DEFAULT_EMAIL
    assert out.loc[0, "phone"] == ""


# ---------------------------------------------------------------------------
# clean_orders  (filter + convert composed)
# ---------------------------------------------------------------------------

def test_clean_orders_filters_then_converts(rates):
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "order_date": ["2023-05-01", "2023-05-01", "2023-05-01"],
            "total_amount": [200.0, -50.0, 100.0],
            "currency": ["EUR", "USD", "USD"],
        }
    )
    out = clean_orders(orders, rates)
    assert list(out["order_id"]) == [1, 3]  # negative order 2 dropped
    assert out.loc[out["order_id"] == 1, "usd_amount"].iloc[0] == 220.0
