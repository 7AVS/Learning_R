# q5_age_volume_recompute.py — Q5a v3 input: Cards email volume + unsubs by age band.
# Pattern: pm_asks_recompute.py. Run in Lumina (spark pre-initialized), NOT locally.
# v2 fix (2026-08-06): ucp_enriched_a3_v1 holds ONLY the UCP attributes
# (age_band/tenure/TIBC per clnt_no) — email counts and unsub flags live in
# a1_client_v2. Join the two here on clnt_no. Still no new Teradata pull.
# Output: ~/unsub_unified_out/q5_age_volume.csv (tiny cube, notebook cell [25i]).

# %% [1] read both frames + schema proof
import os
from pyspark.sql import functions as F

BASE = "hdfs:///user/427966379/unsub_unified/"
A1_DIR = BASE + "a1_client_v2/"            # client grain: counts + unsub flags
UCPA_DIR = BASE + "ucp_enriched_a3_v1/"    # client grain: UCP attrs (age_band etc.)

a1 = spark.read.parquet(A1_DIR + "bite_?")
ucpa = spark.read.parquet(UCPA_DIR + "bite_?")
need_a1 = {"clnt_no", "n_emails_cards", "cards_unsub_flag", "unsub_flag_any"}
need_uc = {"clnt_no", "age_band"}
miss = (need_a1 - set(a1.columns)) | (need_uc - set(ucpa.columns))
if miss:
    print("a1 columns:", sorted(a1.columns))
    print("ucpa columns:", sorted(ucpa.columns))
    raise SystemExit(f"STOP: missing expected columns {miss}")
print(f"a1_client_v2: {a1.count():,} rows | ucp_enriched_a3_v1: {ucpa.count():,} rows.")

# %% [2] Cards-mailed universe FIRST (push filter before the join), then age join
# Universe = clients with >= 1 Cards email in-window. This is the denominator
# fix vs Q5a v2 (which used all RBC-mailed clients for the Cards series).
cm = (a1.filter(F.col("n_emails_cards") >= 1)
      .withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long")))
uc = (ucpa.select("clnt_no", "age_band")
      .withColumn("clnt_no", F.col("clnt_no").cast("decimal(18,0)").cast("long"))
      .dropDuplicates(["clnt_no"]))
n_cm = cm.count()
print(f"Cards-mailed clients (n_emails_cards >= 1): {n_cm:,} of {a1.count():,} enterprise-mailed.")
j = (cm.join(uc, "clnt_no", "left")
     .withColumn("age_band", F.coalesce(F.col("age_band"), F.lit("no_ucp_match"))))
n_j = j.count()
assert n_j == n_cm, f"join changed row count: {n_cm:,} -> {n_j:,} (UCP side not unique?)"
n_nomatch = j.filter(F.col("age_band") == "no_ucp_match").count()
print(f"Join kept {n_j:,} rows (unchanged). no_ucp_match: {n_nomatch:,} "
      f"({n_nomatch / n_j * 100:.1f}%) — expect near the ~9-10% seen in Q5a.")

# %% [3] aggregate by age band
cube = (j.groupBy("age_band")
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
print("EYEBALL: per-client overall should sit ABOVE the ~0.23% overall seen in Q5a v2 "
      "(v2 was diluted by non-Cards-mailed denominators).")

# %% [5] write
out = os.path.expanduser("~/unsub_unified_out/q5_age_volume.csv")
cube.to_csv(out, index=False)
print(f"WROTE {out} ({len(cube)} rows).")
