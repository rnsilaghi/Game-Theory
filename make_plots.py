import os
import pandas as pd
import matplotlib.pyplot as plt

# --------------------------------------------------
# Output directory (your project folder)
# --------------------------------------------------
OUT_DIR = r"C:\Users\rsila\OneDrive\Desktop\Robert\Trading and Models\Quant Projects\Game Theory"

# --------------------------------------------------
# Load data
# --------------------------------------------------
df = pd.read_csv("exposure_vs_next_q_return.csv")
df = df.sort_values("quarter")

# --------------------------------------------------
# 1) Scatter: Exposure vs Next-Quarter Return
# --------------------------------------------------
plt.figure()
plt.scatter(
    df["net_exposure_change"],
    df["price_return_next_q"]
)
plt.xlabel("Net Exposure Change")
plt.ylabel("Next-Quarter Return")
plt.title("Net 13F Exposure vs Next-Quarter Return")
plt.grid(True)
plt.tight_layout()

plt.savefig(os.path.join(OUT_DIR, "scatter_exposure_vs_next_q_return.png"), dpi=150)
plt.show()

# --------------------------------------------------
# 2) Time Series: Net Exposure (raw)
# --------------------------------------------------
expo_ts = (
    df.groupby("quarter", as_index=False)["net_exposure_change"]
      .sum()
      .sort_values("quarter")
)

plt.figure()
plt.plot(expo_ts["quarter"], expo_ts["net_exposure_change"])
plt.xlabel("Quarter")
plt.ylabel("Net Exposure Change")
plt.title("Net 13F Exposure Over Time")
plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig(os.path.join(OUT_DIR, "timeseries_net_exposure.png"), dpi=150)
plt.show()

# --------------------------------------------------
# 3) Time Series: Z-scored Exposure vs Next-Quarter Return
# --------------------------------------------------
expo_ts["exposure_z"] = (
    (expo_ts["net_exposure_change"] - expo_ts["net_exposure_change"].mean())
    / expo_ts["net_exposure_change"].std()
)

rets_ts = (
    df.groupby("quarter", as_index=False)["price_return_next_q"]
      .mean()
      .sort_values("quarter")
)

plt.figure()
plt.plot(
    expo_ts["quarter"],
    expo_ts["exposure_z"],
    label="Exposure (z-score)"
)
plt.plot(
    rets_ts["quarter"],
    rets_ts["price_return_next_q"],
    label="Next-Quarter Return"
)

plt.xlabel("Quarter")
plt.title("Z-scored Exposure vs Next-Quarter Return")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()

plt.savefig(os.path.join(OUT_DIR, "overlay_exposure_z_vs_next_q_return.png"), dpi=150)
plt.show()
