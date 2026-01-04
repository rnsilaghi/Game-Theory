from sec_edgar import get_13f_filings_for_ticker, extract_holdings
from db import create_db, insert_holdings
from analysis import infer_trades_per_manager

TICKERS = ["ORCL", "UNH", "FDS"]


def write_pretty_txt_by_ticker(df, ticker: str):
    out_path = f"{ticker}_trades.txt"
    sub = df[df["ticker"] == ticker].copy()

    # Ensure managers are grouped and stable
    sub = sub.sort_values(["manager", "quarter"])

    cols = [
        "manager",
        "ticker",
        "quarter",
        "filed_date",
        "value_k",
        "prev_qty_proxy",
        "qty_proxy",
        "delta_qty_proxy",
        "action",
        "qty_source",
    ]

    # (shares is always NULL in your DB screenshot, so we omit it for readability)
    # If you later have non-null shares and want it back, tell me.

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("=== " + ticker + " (per-manager position changes; qty_source=shares or value_k) ===\n\n")

        for manager, g in sub.groupby("manager", sort=True):
            f.write(f"MANAGER: {manager}\n")
            f.write("-" * (9 + len(manager)) + "\n")
            # header for this manager block
            f.write("quarter | filed_date | action | value_k | prev_qty | qty | delta | qty_source\n")

            for _, row in g.iterrows():
                # tighter formatting; avoids repeating column names like "value_k" in every row
                line = (
                    f"{row['quarter']} | "
                    f"{row['filed_date']} | "
                    f"{row['action']} | "
                    f"{row['value_k']:.0f} | "
                    f"{row['prev_qty_proxy']:.0f} | "
                    f"{row['qty_proxy']:.0f} | "
                    f"{row['delta_qty_proxy']:.0f} | "
                    f"{row['qty_source']}\n"
                )
                f.write(line)

            f.write("\n")  # ✅ blank line between managers

    print(f"Saved {out_path}")


if __name__ == "__main__":
    create_db()

    # Pull + store holdings (dedupe already handled by DB unique index)
    for ticker in TICKERS:
        filings = get_13f_filings_for_ticker(ticker, limit=200)
        rows = extract_holdings(filings, ticker)
        insert_holdings(rows)

    # Infer trades (drops first per manager+ticker automatically)
    trades = infer_trades_per_manager()

    # Write 3 separate pretty .txt files
    for ticker in TICKERS:
        write_pretty_txt_by_ticker(trades, ticker)
