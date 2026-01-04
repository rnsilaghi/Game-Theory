import sqlite3
import pandas as pd
from db import DB_PATH


def infer_trades_per_manager():
    """
    Returns per-manager inferred trades using shares when available,
    otherwise value_k (USD thousands). Drops the first observation per
    manager+ticker (no change info).
    filed_at is cleaned to filed_date (YYYY-MM-DD).
    """
    conn = sqlite3.connect(DB_PATH)

    query = """
    WITH base AS (
        SELECT
            manager,
            ticker,
            quarter,
            SUBSTR(filed_at, 1, 10) AS filed_date,
            shares,
            value_k,
            CASE
                WHEN shares IS NOT NULL THEN CAST(shares AS REAL)
                ELSE CAST(value_k AS REAL)
            END AS qty_proxy,
            CASE
                WHEN shares IS NOT NULL THEN 'shares'
                ELSE 'value_k'
            END AS qty_source
        FROM holdings
        WHERE value_k IS NOT NULL
    ),
    ordered AS (
        SELECT
            manager,
            ticker,
            quarter,
            filed_date,
            shares,
            value_k,
            qty_proxy,
            qty_source,
            LAG(qty_proxy) OVER (
                PARTITION BY manager, ticker
                ORDER BY quarter
            ) AS prev_qty_proxy
        FROM base
    )
    SELECT
        manager,
        ticker,
        quarter,
        filed_date,
        shares,
        value_k,
        prev_qty_proxy,
        qty_proxy,
        (qty_proxy - prev_qty_proxy) AS delta_qty_proxy,
        CASE
            WHEN (qty_proxy - prev_qty_proxy) > 0 THEN 'BUY'
            WHEN (qty_proxy - prev_qty_proxy) < 0 THEN 'SELL'
            ELSE 'HOLD'
        END AS action,
        qty_source
    FROM ordered
    WHERE prev_qty_proxy IS NOT NULL   -- ✅ removes the “first” per manager+ticker
    ORDER BY ticker, manager, quarter
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df
