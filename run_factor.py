from db import create_db, get_prices_eod_for_ticker
from analysis_stock import compute_exposure_vs_next_q_return
from stats_tests import run_stats


TICKERS = ["ORCL", "UNH", "FDS"]


def write_exposure_summary_txt(df, path="exposure_vs_next_q_return.txt"):
    with open(path, "w", encoding="utf-8") as f:
        if df.empty:
            f.write("No matched rows. Likely原因: price history window does not overlap holdings quarters.\n")
            return

        for ticker, g in df.groupby("ticker", sort=True):
            f.write(f"=== {ticker} — Net 13F Exposure vs Next-Quarter Return ===\n\n")
            f.write("quarter | net_exposure | next_q_return | signal\n")
            f.write("-" * 56 + "\n")

            for _, row in g.iterrows():
                net_exp = float(row["net_exposure_change"])
                ret = float(row["price_return_next_q"])

                if net_exp == 0 or ret == 0:
                    signal = "NEUTRAL"
                elif (net_exp > 0 and ret > 0) or (net_exp < 0 and ret < 0):
                    signal = "MATCH"
                else:
                    signal = "MISMATCH"

                f.write(
                    f"{row['quarter']} | "
                    f"{net_exp:>12,.0f} | "
                    f"{ret:>8.2%} | "
                    f"{signal}\n"
                )
            f.write("\n\n")

    print(f"Saved {path}")

def write_stats_txt(stats, path="stats_summary.txt"):
    with open(path, "w", encoding="utf-8") as f:
        f.write("=== Statistical Tests: Net Exposure vs Next-Quarter Returns ===\n\n")

        f.write("Pearson correlation:\n")
        f.write(f"  r = {stats['pearson']['r']:.3f}\n")
        f.write(f"  p-value = {stats['pearson']['p_value']:.4f}\n\n")

        f.write("Spearman rank correlation:\n")
        f.write(f"  rho = {stats['spearman']['r']:.3f}\n")
        f.write(f"  p-value = {stats['spearman']['p_value']:.4f}\n\n")

        f.write("Regression: return = alpha + beta * exposure\n")
        f.write(f"  beta = {stats['regression']['beta']:.6f}\n")
        f.write(f"  t-stat = {stats['regression']['t_stat']:.2f}\n")
        f.write(f"  p-value = {stats['regression']['p_value']:.4f}\n")
        f.write(f"  R^2 = {stats['regression']['r_squared']:.4f}\n")
        f.write(f"  N = {stats['regression']['n_obs']}\n\n")

        f.write("Directional accuracy:\n")
        f.write(f"  hit rate = {stats['directional']['hit_rate']:.2%}\n")
        f.write(f"  hits = {stats['directional']['hits']} / {stats['directional']['n_obs']}\n")
        f.write(f"  binomial p-value = {float(stats['directional']['p_value']):.4f}\n")


    print(f"Saved {path}")


if __name__ == "__main__":
    create_db()

    # Quick visibility into your price cache range
    for t in TICKERS:
        rows = get_prices_eod_for_ticker(t)
        if not rows:
            print(f"{t}: prices_eod empty (run update_prices.py)")
        else:
            print(f"{t}: prices_eod range {rows[0][0]} -> {rows[-1][0]} (rows={len(rows)})")

    df = compute_exposure_vs_next_q_return(TICKERS)

    print("Merged rows:", len(df))
    if not df.empty:
        print(df.head(20))

    df.to_csv("exposure_vs_next_q_return.csv", index=False)
    print("Saved exposure_vs_next_q_return.csv")

    write_exposure_summary_txt(df)

    stats_results = run_stats(df)
    write_stats_txt(stats_results)




