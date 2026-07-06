
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task

# --- Configuration ----------------------------------------------------------
SOURCE_DB = "shopdata.db"
OUTPUT_DB = "analytics.db"
DEFAULT_EMAIL = "unknown@domain.com"


# ============================================================================
# Pure transformation functions  (no DB, no Prefect -> unit-testable)
# ============================================================================

def standardize_phone(phone) -> str:
    if phone is None or (isinstance(phone, float) and pd.isna(phone)):
        return ""
    return re.sub(r"\D", "", str(phone))


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # 1. Deduplicate -- sort by signup_date, keep the latest row per customer.
    out["signup_date"] = pd.to_datetime(out["signup_date"], errors="coerce")
    out = (
        out.sort_values("signup_date")
        .drop_duplicates(subset="customer_id", keep="last")
        .reset_index(drop=True)
    )
    out["signup_date"] = out["signup_date"].dt.strftime("%Y-%m-%d")

    # 2. Standardize phone.
    out["phone"] = out["phone"].apply(standardize_phone)

    # 3. Fill missing email (NULL or blank) with the placeholder.
    missing_email = out["email"].isna() | (out["email"].astype(str).str.strip() == "")
    out["email"] = out["email"].mask(missing_email, DEFAULT_EMAIL)

    return out


def filter_invalid_orders(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    valid = out["total_amount"].notna() & (out["total_amount"] > 0)
    return out[valid].reset_index(drop=True)


def convert_to_usd(orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    out = orders.copy()
    rate_lookup = rates.rename(columns={"date": "order_date"})[
        ["currency", "order_date", "rate_to_usd"]
    ]

    merged = out.merge(rate_lookup, on=["currency", "order_date"], how="left")
    merged["rate_to_usd"] = merged["rate_to_usd"].fillna(1.0)
    merged["usd_amount"] = (merged["total_amount"] * merged["rate_to_usd"]).round(2)
    return merged.drop(columns=["rate_to_usd"])


def clean_orders(orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    return convert_to_usd(filter_invalid_orders(orders), rates)


@task
def extract_view(db_path: str, view: str) -> pd.DataFrame:
    logger = get_run_logger()
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Source database not found: {db_path}")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {view}", conn)  # view name is a trusted constant
    except Exception:
        logger.exception("Failed to extract view '%s' from %s", view, db_path)
        raise
    finally:
        conn.close()

    logger.info("Extracted %d rows from %s", len(df), view)
    return df


@task
def transform_customers(raw: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    cleaned = clean_customers(raw)
    logger.info(
        "Customers: %d raw rows -> %d unique customers (%d duplicate rows removed)",
        len(raw), len(cleaned), len(raw) - len(cleaned),
    )
    return cleaned


@task
def transform_orders(raw: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    cleaned = clean_orders(raw, rates)
    logger.info(
        "Orders: %d raw rows -> %d valid orders (%d dropped as amount <= 0)",
        len(raw), len(cleaned), len(raw) - len(cleaned),
    )
    return cleaned


@task
def load(customers: pd.DataFrame, orders: pd.DataFrame, output_db: str) -> None:
    logger = get_run_logger()
    try:
        conn = sqlite3.connect(output_db)
        try:
            customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
            orders.to_sql("fct_orders", conn, if_exists="replace", index=False)
            conn.commit()
        finally:
            conn.close()
        logger.info(
            "Loaded %d rows -> dim_customers, %d rows -> fct_orders in %s",
            len(customers), len(orders), output_db,
        )
    except Exception:
        logger.exception("Could not write %s; falling back to CSV export", output_db)
        customers.to_csv("clean_customers.csv", index=False)
        orders.to_csv("clean_orders.csv", index=False)
        logger.warning("Wrote clean_customers.csv and clean_orders.csv as fallback")


# ============================================================================
# Flow
# ============================================================================

@flow(name="shopdata-etl")
def etl_flow(source_db: str = SOURCE_DB, output_db: str = OUTPUT_DB) -> None:
    logger = get_run_logger()
    logger.info("Starting ShopData ETL: %s -> %s", source_db, output_db)

    raw_customers = extract_view(source_db, "vw_raw_customers")
    raw_orders = extract_view(source_db, "vw_raw_orders")
    rates = extract_view(source_db, "vw_exchange_rates")

    dim_customers = transform_customers(raw_customers)
    fct_orders = transform_orders(raw_orders, rates)

    load(dim_customers, fct_orders, output_db)
    logger.info("ETL complete.")


if __name__ == "__main__":
    etl_flow()
