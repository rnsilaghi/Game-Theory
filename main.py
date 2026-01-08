from sec_edgar import get_13f_filings_for_ticker_backfill, extract_holdings
from db import create_db, insert_holdings, get_backfill_checkpoint, set_backfill_checkpoint
from analysis import infer_trades_per_manager

TICKERS = ["ORCL", "UNH", "FDS"]


def write_txt_by_ticker(df, ticker: str):
    out_path = f"{ticker}_trades.txt"
    sub = df[df["ticker"] == ticker].copy()
    sub = sub.sort_values(["manager", "quarter"])

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("=== " + ticker + " (per-manager position changes; qty_proxy=value_k) ===\n\n")

        for manager, g in sub.groupby("manager", sort=True):
            f.write(f"MANAGER: {manager}\n")
            f.write("-" * (9 + len(manager)) + "\n")
            f.write("quarter | filed_date | action | prev_qty | qty | delta\n")

            for _, row in g.iterrows():
                line = (
                    f"{row['quarter']} | "
                    f"{row['filed_date']} | "
                    f"{row['action']} | "
                    f"{row['prev_qty_proxy']:.0f} | "
                    f"{row['qty_proxy']:.0f} | "
                    f"{row['delta_qty_proxy']:.0f}\n"
                )
                f.write(line)

            f.write("\n")

    print(f"Saved {out_path}")


if __name__ == "__main__":
    create_db()
    for ticker in TICKERS:
        checkpoint = get_backfill_checkpoint(ticker)

        filings = get_13f_filings_for_ticker_backfill(
            ticker=ticker,
            limit=200,
            end_checkpoint_filed_at=checkpoint,
            years=5
        )

        rows = extract_holdings(filings, ticker)
        inserted = insert_holdings(rows)

        filed_ats = [f.get("filedAt") for f in filings if f.get("filedAt")]
        if filed_ats:
            oldest = min(filed_ats)
            set_backfill_checkpoint(ticker, oldest)

        print(
            f"{ticker}: filings fetched={len(filings)} rows extracted={len(rows)} inserted={inserted} "
            f"checkpoint(oldest_so_far)={checkpoint}"
        )

    trades = infer_trades_per_manager()

    for ticker in TICKERS:
        write_txt_by_ticker(trades, ticker)
