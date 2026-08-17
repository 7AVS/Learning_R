# %% [markdown]
# # 43 — probe: UCP's cpc_ent_eligible (is this the mock's 14.2MM?)
#
# UCP = monthly HDFS snapshots, PySpark only (canon: references/ucp/README.md).
# Question: what does cpc_ent_eligible count, and does it explain the gap between the
# mock's ~14.2MM emailable base and CIDM EM_DTL's 11.37MM (EM_ELIGIBLE_IND = Y)?
# Assumes a live `spark` session (Lumina pre-init). Personal table only (ucp4/).

# %% [0] setup
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim

UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"

# %% [1] Which month-end partitions exist (history depth)
print("--- partitions under ucp4/ (each = one month-end snapshot) ---")
get_ipython().system(f"hdfs dfs -ls {UCP_BASE} | tail -25")

# %% [2] Latest month: does the cpc_* family exist, and what does a row look like?
LATEST = "2026-07-31"          # <- set to the newest partition from [1]
ucp = spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={LATEST}/")

cpc_cols = [c for c in ucp.columns if "cpc" in c.lower()]
print(f"columns containing 'cpc' in ucp4 @ {LATEST}: {cpc_cols}")
elig_cols = [c for c in ucp.columns if "elig" in c.lower()]
print(f"columns containing 'elig': {elig_cols}")

ucp.select(["clnt_no", "clnt_typ"] + cpc_cols).show(10, truncate=False)

# %% [3] THE NUMBER - cpc_ent_eligible value counts on the latest month
# Compare against: mock ~14.2MM (Jan'25) / 14.5MM (Jul'26); CIDM EM_DTL EM_ELIGIBLE Y
# = 11,372,774 (2026-08-17). Match to EM_DTL -> UCP is a monthly copy of CIDM's answer.
# ~14-15MM -> the mock's source found, and the definition gap vs CIDM is real.
print(f"--- cpc_ent_eligible on {LATEST}, all clients ---")
ucp.groupBy("cpc_ent_eligible").count().orderBy(F.desc("count")).show(truncate=False)

print(f"--- same, split by clnt_typ (personal vs anything else in this table) ---")
ucp.groupBy("clnt_typ", "cpc_ent_eligible").count().orderBy("clnt_typ", F.desc("count")).show(truncate=False)

# %% [4] The channel siblings - dm/tm/olb (and em if it exists) on the latest month
for c in [c for c in cpc_cols if c != "cpc_ent_eligible"]:
    print(f"--- {c} ---")
    ucp.groupBy(c).count().orderBy(F.desc("count")).show(truncate=False)

# %% [5] History: cpc_ent_eligible='Y' count per month-end, last 19 months
# One partition at a time (partition pruning), light aggregate per month.
MONTHS = ["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30", "2025-05-31",
          "2025-06-30", "2025-07-31", "2025-08-31", "2025-09-30", "2025-10-31",
          "2025-11-30", "2025-12-31", "2026-01-31", "2026-02-28", "2026-03-31",
          "2026-04-30", "2026-05-31", "2026-06-30", "2026-07-31"]
rows = []
for m in MONTHS:
    try:
        d = spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m}/")
        n = d.filter(trim(col("cpc_ent_eligible")) == "Y").count()
        rows.append((m, n))
        print(f"{m}: cpc_ent_eligible=Y -> {n:,}")
    except Exception as e:
        rows.append((m, None))
        print(f"{m}: partition unreadable ({str(e)[:80]})")
print("--- series complete: this is the mock's top-waterfall line if [3] matched ~14MM ---")
