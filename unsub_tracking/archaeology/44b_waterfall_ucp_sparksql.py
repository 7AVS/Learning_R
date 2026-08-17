# %% [markdown]
# # 44b — the same waterfall, Spark SQL surface (SQL text, HDFS underneath)
#
# Identical logic to pack 44, but the parquet snapshots are registered as TEMP VIEWS
# and every step is a visible SQL statement run by spark.sql(). Same engine, same
# data - just SQL instead of the DataFrame API. (UCP is not in Starburst/Trino;
# Spark SQL is the only SQL route to it.) Assumes live `spark` session.

# %% [0] parameters + register the two snapshots as SQL views
UCP_BASE = "/prod/sz/tsz/00172/data/ucp4/"
MONTH_A  = "2026-01-31"
MONTH_B  = "2026-07-31"
FLAG     = "CPC_EM_ELIGIBLE"     # or CPC_ENT_ELIGIBLE

spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={MONTH_A}/").createOrReplaceTempView("ucp_a")
spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={MONTH_B}/").createOrReplaceTempView("ucp_b")
print("views registered: ucp_a, ucp_b")

# %% [1] The waterfall buckets - one SQL
wf = spark.sql(f"""
-- step 1: read each snapshot; the flag is stored as 1/0, this just reads it as a number
WITH a AS (SELECT CLNT_NO,
                  CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS elig_a   -- 1 = emailable
           FROM ucp_a),
     b AS (SELECT CLNT_NO,
                  CAST(TRIM(CAST({FLAG} AS STRING)) = '1' AS INT) AS elig_b,
                  DT_OPENED                                                    -- when the client joined the bank
           FROM ucp_b)
-- step 2: line up every client across the two months (FULL OUTER = keep people who
-- exist in only one of them) and name what happened to each
SELECT CASE
         WHEN a.elig_a = 1 AND b.elig_b = 1 THEN 'stayed eligible (no bar)'
         WHEN a.elig_a = 1 AND b.elig_b = 0 THEN '- lost consent (flag 1->0)'
         WHEN a.elig_a = 1 AND b.CLNT_NO IS NULL THEN '- attrition (gone from B)'
         WHEN a.elig_a = 0 AND b.elig_b = 1 THEN '+ opened consent (flag 0->1)'
         WHEN a.CLNT_NO IS NULL AND b.elig_b = 1 AND b.DT_OPENED > DATE('{MONTH_A}')
              THEN '+ new to bank (opened after A)'
         WHEN a.CLNT_NO IS NULL AND b.elig_b = 1
              THEN '+ re-entered universe'
         ELSE 'no bar (other)'
       END AS bucket,
       COUNT(*) AS n_clients
FROM a FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
GROUP BY 1
ORDER BY n_clients DESC
""").toPandas()
print(f"Waterfall components {MONTH_A} -> {MONTH_B}, flag = {FLAG}:")
print(wf.to_string(index=False))
print("\nCopy-paste block (tab-separated):")
print(wf.to_csv(sep="\t", index=False))

# %% [2] Identity check - SQL both sides
end_direct = spark.sql(f"""
SELECT COUNT(*) AS n FROM ucp_b
WHERE TRIM(CAST({FLAG} AS STRING)) = '1'
""").collect()[0]["n"]
g = lambda pat: int(wf.loc[wf["bucket"].str.contains(pat, regex=False), "n_clients"].sum())
start = g("stayed eligible") + g("- lost consent") + g("- attrition")
end   = start \
      + g("+ opened consent") + g("+ new to bank") + g("+ re-entered") \
      - g("- lost consent") - g("- attrition")
print(f"START {start:,} -> computed END {end:,} | measured END {end_direct:,} "
      f"| identity {'HOLDS' if end == end_direct else 'BROKEN'}")

# %% [3] Monthly chain - one SQL per consecutive pair (visible, parameterized)
import pandas as pd
MONTHS_CHAIN = ["2026-01-31", "2026-02-28", "2026-03-31", "2026-04-30",
                "2026-05-31", "2026-06-30", "2026-07-31"]

PAIR_SQL = """
-- same two-step shape as [1], one month against the next:
-- e0/e1 = was the client emailable last month / this month (flag stored as 1/0)
WITH m0 AS (SELECT CLNT_NO, CAST(TRIM(CAST({flag} AS STRING)) = '1' AS INT) AS e0
            FROM ucp_m0),
     m1 AS (SELECT CLNT_NO, CAST(TRIM(CAST({flag} AS STRING)) = '1' AS INT) AS e1,
                   DT_OPENED
            FROM ucp_m1)
-- each SUM counts one kind of movement between the two months
SELECT SUM(CASE WHEN m0.e0 = 0 AND m1.e1 = 1 THEN 1 ELSE 0 END)              AS opted_in,
       SUM(CASE WHEN m0.e0 = 1 AND m1.e1 = 0 THEN 1 ELSE 0 END)              AS lost_consent,
       SUM(CASE WHEN m0.e0 = 1 AND m1.CLNT_NO IS NULL THEN 1 ELSE 0 END)     AS attrition,
       SUM(CASE WHEN m0.CLNT_NO IS NULL AND m1.e1 = 1
                 AND m1.DT_OPENED > DATE('{m0}') THEN 1 ELSE 0 END)          AS new_to_bank,
       SUM(CASE WHEN m0.CLNT_NO IS NULL AND m1.e1 = 1
                 AND (m1.DT_OPENED <= DATE('{m0}') OR m1.DT_OPENED IS NULL)
                THEN 1 ELSE 0 END)                                           AS re_entered
FROM m0 FULL OUTER JOIN m1 ON m0.CLNT_NO = m1.CLNT_NO
"""

flows = []
for m0, m1 in zip(MONTHS_CHAIN[:-1], MONTHS_CHAIN[1:]):
    spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m0}/").createOrReplaceTempView("ucp_m0")
    spark.read.parquet(f"{UCP_BASE}MONTH_END_DATE={m1}/").createOrReplaceTempView("ucp_m1")
    r = spark.sql(PAIR_SQL.format(flag=FLAG, m0=m0)).toPandas()
    r.insert(0, "month", m1[:7])
    flows.append(r)

fdf = pd.concat(flows, ignore_index=True)
print("Gross monthly flows (each month vs the previous):")
print(fdf.to_string(index=False))
print("\nCopy-paste block for PowerPoint (tab-separated):")
print(fdf.to_csv(sep="\t", index=False))
