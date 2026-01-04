import requests
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from api import SEC_API_KEY, SEC_BASE_URL


def _safe_int(x):
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def get_13f_filings_for_ticker(ticker: str, limit: int = 200) -> List[Dict]:
    """
    Fetch up to `limit` 13F-HR filings for `ticker` from the last 3 years.
    Uses your proven API style: POST {SEC_BASE_URL}?token=KEY
    """
    if not SEC_API_KEY or SEC_API_KEY.startswith("YOUR_"):
        raise ValueError("SEC_API_KEY missing or invalid")

    url = f"{SEC_BASE_URL}?token={SEC_API_KEY}"

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=3 * 365)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    payload = {
        "query": (
            f'formType:"13F-HR" '
            f'AND filedAt:[{start_str} TO {end_str}] '
            f'AND holdings.ticker:{ticker}'
        ),
        "from": "0",
        "size": str(limit),
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    r = requests.post(url, json=payload)
    r.raise_for_status()

    filings = r.json().get("filings", [])

    # Deduplicate at the filings level too (safety)
    seen = set()
    unique = []
    for f in filings:
        acc = f.get("accessionNo") or f.get("accessionNumber") or f.get("id") or f.get("linkToHtml")
        if not acc:
            continue
        if acc in seen:
            continue
        seen.add(acc)
        unique.append(f)

    return unique


def extract_holdings(filings: List[Dict], ticker: str) -> List[Tuple]:
    """
    Returns rows ready for SQLite insertion:
    (accession_no, manager, quarter, ticker, shares, value_k, filed_at)

    IMPORTANT: shares may be NULL (your issue).
    We always store value_k (USD thousands) as the reliable quantity.
    """
    rows = []

    for f in filings:
        manager = f.get("companyName")
        quarter = f.get("periodOfReport")
        filed_at = f.get("filedAt")

        accession_no = f.get("accessionNo") or f.get("accessionNumber") or f.get("id") or f.get("linkToHtml")
        if not accession_no or not manager or not quarter:
            continue

        # Some plans return holdings, some return partial objects.
        holdings = f.get("holdings") or []
        if not holdings:
            # If holdings are missing entirely, we can't extract position rows.
            # Keep skipping rather than inserting null rows.
            continue

        for h in holdings:
            if (h.get("ticker") or "").upper() != ticker.upper():
                continue

            # Shares may be null in your environment
            shares = _safe_int(h.get("shares") or h.get("sshPrnamt"))

            # Value in USD thousands is usually present as h["value"]
            value_k = _safe_float(h.get("value") or h.get("marketValue") or h.get("valueK"))
            if value_k is None:
                # If even value is missing, skip row
                continue

            rows.append((
                str(accession_no),
                str(manager),
                str(quarter),
                ticker.upper(),
                shares,
                value_k,
                filed_at
            ))

    return rows
