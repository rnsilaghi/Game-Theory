import requests
from datetime import datetime
from typing import List
from db import (
    create_db,
    upsert_prices_eod,
    get_connection,
)
from api import STOCKDATA_API_KEY, STOCKDATA_BASE_URL

TICKERS = ["ORCL", "UNH", "FDS"]
MAX_QUARTERS = 28  # 7 years


def get_recent_quarters_for_ticker(ticker: str, limit: int) -> List[str]:
    """
    Pull distinct quarter-end dates from holdings for a ticker.
    Returns most recent `limit` dates as YYYY-MM-DD strings.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT quarter
        FROM holdings
        WHERE ticker = ?
        ORDER BY date(quarter) DESC
        LIMIT ?
    """, (ticker.upper(), limit))

    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows


def fetch_close_on_date(ticker: str, date_str: str):
    """
    Fetch EOD close for ticker on a specific date.
    API returns nearest previous trading day if market was closed.
    """
    url = f"{STOCKDATA_BASE_URL}/data/eod"
    params = {
        "symbols": ticker,
        "date": date_str,
        "api_token": STOCKDATA_API_KEY,
    }

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()

    data = r.json().get("data", [])
    if not data:
        return None

    bar = data[0]
    return bar.get("date"), bar.get("close")


def main():
    create_db()

    total_calls = 0

    for ticker in TICKERS:
        quarters = get_recent_quarters_for_ticker(ticker, MAX_QUARTERS)

        print(f"{ticker}: fetching prices for {len(quarters)} quarter dates")

        rows = []
        for q in quarters:
            result = fetch_close_on_date(ticker, q)
            total_calls += 1

            if result is None:
                continue

            price_date, close = result
            rows.append((ticker, price_date[:10], float(close)))

        changed = upsert_prices_eod(rows)
        print(f"{ticker}: stored {changed} prices")

    print(f"TOTAL API CALLS USED: {total_calls}")


if __name__ == "__main__":
    main()
