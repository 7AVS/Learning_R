# q5_age_volume_recompute.py — Q5a v3 input: Cards email volume + unsubs by age band.
# Pattern: pm_asks_recompute.py. Run in Lumina (spark pre-initialized), NOT locally.
# Reads the already-landed client-grain UCP-joined Piece A frame — NO new Teradata pull.
# Output: ~/unsub_unified_out/q5_age_volume.csv (tiny cube, consumed by notebook cell [25i]).

# %% [1] read + schema proof
import os
from pyspark.sql import functions as F

BASE = "hdfs:///user/427966379/unsub_unified/"
UCPA_DIR = BASE + "ucp_enriched_a3_v1/"   # unsub_unified.py Cell [5] landing (UCPA_DIR)

ucpa = spark.read.parquet(UCPA_DIR + "bite_?")
need = {"age_band", "n_emails_cards", "cards_unsub_flag", "unsub_flag_any"}
missing = need - set(ucpa.columns)
if missing:
    print("COLUMNS PRESENT:", sorted(ucpa.columns))
    raise SystemExit(f"STOP: missing expected columns {missing} — check UCPA_DIR/version")
n_all = ucpa.count()
print(f"ucp_enriched_a3_v1 loaded: {n_all:,} client rows (enterprise-wide, WIN_A Jan-Apr 2026).")

# %% [2] restrict to Cards-mailed universe — this IS the denominator fix
# Q5a v1/v2 denominated the Cards series on ALL RBC-mailed clients. Here the
# universe is clients with >= 1 Cards email in-window, so per-client and
# per-email Cards rates are both denominated on Cards-mailed only.
cm = ucpa.filter(F.col("n_emails_cards") >= 1)
n_cm = cm.count()
print(f"Cards-mailed clients (n_emails_cards >= 1): {n_cm:,} "
      f"of {n_all:,} enterprise-mailed ({n_cm / n_all * 100:.1f}%).")

# %% [3] aggregate by age band
cube = (cm.groupBy("age_band")
        .agg(F.count("*").alias("clients"),
             F.sum("n_emails_cards").alias("emails_cards"),
             F.sum("cards_unsub_flag").alias("cards_unsubs"),
             F.sum("unsub_flag_any").alias("any_unsubs"))
        .toPandas())
cube["emails_per_client"] = cube["emails_cards"] / cube["clients"]
cube["unsubs_per_1k_emails"] = cube["cards_unsubs"] / cube["emails_cards"] * 1000
cube["cards_unsub_rate_pct"] = cube["cards_unsubs"] / cube["clients"] * 100
print(cube.sort_values("age_band").to_string(index=False))

# %% [4] sanity — internal consistency + eyeball anchors
assert (cube["cards_unsubs"] <= cube["clients"]).all(), "unsubs > clients in a band"
assert (cube["emails_cards"] >= cube["clients"]).all(), "fewer emails than clients (each has >=1)"
tot_u = int(cube["cards_unsubs"].sum())
tot_e = int(cube["emails_cards"].sum())
print(f"TOTALS: {n_cm:,} Cards-mailed clients | {tot_e:,} Cards emails | "
      f"{tot_u:,} Cards unsubs -> overall {tot_u / n_cm * 100:.2f}% per-client, "
      f"{tot_u / tot_e * 1000:.2f} per 1k emails.")
print("EYEBALL: per-client overall should sit near the ~0.31%-band rates seen in Q5a v2 "
      "(v2 was diluted by non-Cards-mailed denominators -> expect HIGHER here).")

# %% [5] write
out = os.path.expanduser("~/unsub_unified_out/q5_age_volume.csv")
cube.to_csv(out, index=False)
print(f"WROTE {out} ({len(cube)} rows).")
