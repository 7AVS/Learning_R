# SAVE CSV NOW - run this AFTER unsub_value_museum.py has produced L1/L6/L7/L8 in the same kernel.
#
# Why this exists: the long-format summary cell (3 roles x 4 metrics with percentiles) keeps dying
# on the driver. This file skips it. Everything here is either a table that is ALREADY computed and
# tiny, or a plain groupBy().count() - no explode, no stack, no percentile sketches, nothing that
# can blow the heap. It cannot fail the way that cell fails.
#
# Requires in the kernel: BASE, banded, l6, l7, l8 (all defined by the main file before it broke).
# Everything writes to HDFS. Fetch commands are printed at the end.

# %% [A] The four tables you already saw on screen, written straight out. Each is a few hundred rows.
for _name, _df in [("l6_campaign_summary", l6),
                   ("l7_leaver_vs_stayer", l7),
                   ("l8_cards_mailed_vs_leaver", l8)]:
    _df.coalesce(1).write.mode("overwrite").option("header", True).csv(BASE + "csv_" + _name)
    _n = spark.read.option("header", True).csv(BASE + "csv_" + _name).count()
    print(_name, "->", BASE + "csv_" + _name, "|", _n, "rows, readback confirmed")

# %% [B] The pivot cube. ONE groupBy on the banded client frame - counts only, no percentiles.
# This is the file to open in Excel: drop it in a pivot table and slice any way you like.
# Grain: one row per (campaign, program, bucket, age band, tenure band, product band, value
# quintile, high-potential flag) with the client count. bucket = leaver / stayer / already_out,
# so leaver-vs-stayer comparisons are a pivot filter, not a separate export.
cube = (banded.groupBy("mne", "program", "bucket", "age_band", "tenure_band",
                       "prod_band", "tibc_mix", "prof_quintile", "high_potential")
        .count().withColumnRenamed("count", "clients"))

cube.write.mode("overwrite").parquet(BASE + "cube")
_cube = spark.read.parquet(BASE + "cube")
_n_cube = _cube.count()
print("cube:", _n_cube, "rows ->", BASE + "cube")
assert _n_cube > 0, "cube landed empty - investigate before trusting it"

_cube.coalesce(1).write.mode("overwrite").option("header", True).csv(BASE + "csv_cube")
print("cube csv ->", BASE + "csv_cube", "| readback",
      spark.read.option("header", True).csv(BASE + "csv_cube").count(), "rows")

# %% [C] Client-level spine - one row per client, every attribute and band. The durable artifact:
# any future cut re-derives from this without touching EDW or UCP again. Parquet only (10M+ rows is
# not an Excel file).
client_spine = banded.select("CLNT_NO", "bucket", "mne", "program", "unsub_tm", "ucp_matched",
                             "AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT",
                             "C_TOT_CNT", "PROF_TOT_ANNUAL", "prod_cnt", "prod_band", "tibc_mix",
                             "tenure_band", "age_band", "prof_quintile", "high_potential")
client_spine.write.mode("overwrite").parquet(BASE + "client_spine")
print("client_spine:", spark.read.parquet(BASE + "client_spine").count(), "rows ->",
      BASE + "client_spine")

# %% [D] Fetch commands. Run these in a TERMINAL, not in this kernel.
print("\n--- fetch to local files, from a terminal ---")
for _n in ["csv_cube", "csv_l6_campaign_summary", "csv_l7_leaver_vs_stayer",
           "csv_l8_cards_mailed_vs_leaver"]:
    print("hdfs dfs -getmerge /user/427966379/unsub_value_museum/" + _n + " " + _n + ".csv")
