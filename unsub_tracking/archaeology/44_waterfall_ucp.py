# %% [markdown]
# # 44 — the emailable-base waterfall, UCP source (view 1 of 2)
#
# Two snapshots, client-level transitions, five buckets:
#   START eligible (month A)
#   + existing clients who OPENED consent (in both months, flag 0 -> 1)
#   + NEW arrivals eligible in B (absent in A; split new-to-bank vs re-entered by dt_opened)
#   - existing clients who LOST consent (in both months, flag 1 -> 0)
#   - ATTRITION (eligible in A, absent from B)
#   = END eligible (month B)
# One bar each, no LOB colors. Flag parameterized: CPC_EM_ELIGIBLE (email consent view,
# default) vs CPC_ENT_ELIGIBLE (the mock's enterprise flag) - one-line switch.
# CPC-MTHLY-sourced view 2 comes as its own pack once "active RB personal client" is
# defined there. Assumes live `spark` session.

# %% [0] parameters
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim

UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
MONTH_A  = "2025-01-31"          # start endpoint (mock window)
MONTH_B  = "2026-07-31"          # end endpoint
FLAG     = "CPC_EM_ELIGIBLE"     # or "CPC_ENT_ELIGIBLE" to reproduce the mock's number

# %% [1] Load the two snapshots, slim
def load_month(m):
    d = spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m}/")
    cols = ["CLNT_NO", FLAG] + [c for c in d.columns if c.upper() == "DT_OPENED"]
    return (d.select([col(c) for c in cols])
              .withColumn("elig", (trim(col(FLAG).cast("string")) == "1").cast("int"))
              .drop(FLAG))

a = load_month(MONTH_A).withColumnRenamed("elig", "elig_a")
b = load_month(MONTH_B).withColumnRenamed("elig", "elig_b")
has_dt = "DT_OPENED" in [c.upper() for c in b.columns]
print(f"{MONTH_A}: {a.count():,} clients | {MONTH_B}: {b.count():,} clients | dt_opened available: {has_dt}")

# %% [2] The waterfall - client-level transitions between the two snapshots
j = (a.select("CLNT_NO", "elig_a")
      .join(b.select(["CLNT_NO", "elig_b"] + (["DT_OPENED"] if has_dt else [])),
            "CLNT_NO", "full_outer"))

j = j.withColumn("bucket",
    F.when((col("elig_a") == 1) & (col("elig_b") == 1), "stayed eligible (no bar)")
     .when((col("elig_a") == 1) & (col("elig_b") == 0), "- lost consent (in both months, flag 1->0)")
     .when((col("elig_a") == 1) & col("elig_b").isNull(), "- attrition (eligible in A, gone from B)")
     .when((col("elig_a") == 0) & (col("elig_b") == 1), "+ opened consent (in both months, flag 0->1)")
     .when(col("elig_a").isNull() & (col("elig_b") == 1),
           F.when(col("DT_OPENED") > F.lit(MONTH_A), "+ new to bank (opened after A, eligible in B)")
            .otherwise("+ re-entered universe (existing client, absent in A)") if has_dt
           else F.lit("+ new arrival (absent in A, eligible in B)"))
     .when((col("elig_a") == 0) & col("elig_b").isNull(), "ineligible in A, gone from B (no bar)")
     .when(col("elig_a").isNull() & (col("elig_b") == 0), "arrived ineligible (no bar)")
     .otherwise("stayed ineligible (no bar)"))

wf = j.groupBy("bucket").count().orderBy(F.desc("count")).toPandas()
print(f"Waterfall components, {MONTH_A} -> {MONTH_B}, flag = {FLAG}:")
print(wf.to_string(index=False))

# %% [3] The identity check - start + adds - drops must equal end
import pandas as pd
get_n = lambda pat: int(wf.loc[wf["bucket"].str.contains(pat, regex=False), "count"].sum())
start = get_n("stayed eligible") + get_n("- lost consent") + get_n("- attrition")
adds  = sum(get_n(p) for p in ["+ opened consent", "+ new to bank", "+ re-entered", "+ new arrival"])
drops = get_n("- lost consent") + get_n("- attrition")
end   = start + adds - drops
summary = pd.DataFrame([
    {"waterfall element": f"START eligible @ {MONTH_A}", "n_clients": start},
    {"waterfall element": "+ opened consent (existing)", "n_clients": get_n("+ opened consent")},
    {"waterfall element": "+ new to bank / arrivals", "n_clients": adds - get_n("+ opened consent")},
    {"waterfall element": "- lost consent", "n_clients": -get_n("- lost consent")},
    {"waterfall element": "- attrition", "n_clients": -get_n("- attrition")},
    {"waterfall element": f"END eligible @ {MONTH_B} (computed)", "n_clients": end},
])
print(summary.to_string(index=False))
direct_end = int(b.filter(col("elig_b") == 1).count())
print(f"\nEND measured directly in {MONTH_B}: {direct_end:,} - identity {'HOLDS' if direct_end == end else 'BROKEN - investigate before using'}")
