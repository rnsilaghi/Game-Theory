import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "my_db.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_db():
    conn = get_connection()
    cur = conn.cursor()

    # Store each manager-quarter-ticker observation coming from ONE filing (accession_no)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS holdings (
        accession_no TEXT NOT NULL,
        manager      TEXT NOT NULL,
        quarter      TEXT NOT NULL,
        ticker       TEXT NOT NULL,
        shares       INTEGER,          -- may be NULL
        value_k      REAL NOT NULL,    -- USD thousands; stable from your API
        filed_at     TEXT             -- optional, when filing was filed
    )
    """)

    # Hard dedupe rule: same filing + manager + ticker should never be inserted twice
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_holdings_unique
    ON holdings(accession_no, manager, ticker)
    """)

    conn.commit()
    conn.close()


def insert_holdings(rows):
    if not rows:
        return

    conn = get_connection()
    cur = conn.cursor()

    cur.executemany("""
        INSERT OR IGNORE INTO holdings
        (accession_no, manager, quarter, ticker, shares, value_k, filed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
