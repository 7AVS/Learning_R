# CPC x UNSUBSCRIBE - FINAL EVIDENCE. Runs 100% off the HDFS reservoir. No Teradata.
#
# This file is the single source for every number in the deck. If a figure is not produced here, it
# does not go on a slide. Superseded cells have been removed rather than left in place; git history
# holds them.
#
# ===================== RULES, SETTLED - DO NOT RE-DERIVE =====================
# CLNT_CONSENT_TYP: 5001 Yes | 5002 No | 5003 blank | 5004 other.
#
# 1. "No" is defined per switch. On 1014 and 1015 an explicit No and a blank are ONE population and
#    are counted together. On 1002, 1006 and 1012 a blank counts as Yes, so "No" means explicit 5002.
# 2. NO ROW IS NOT A BLANK. CPC_RB_PREF_LOG is an event log. A client with no row on a switch has no
#    consent record anywhere in the corporation - they are excluded from every "No" population, not
#    counted as one.
# 3. STANDING = the client's LAST event on that switch. Earlier events do not stand.
# 4. Switches tracked as a set: 1002, 1012, 1014. 1006 is carried for comparison only.
# 5. Nothing here is a claim. These cells describe what the two records contain. No cell asserts that
#    a system should have done anything.
# ============================================================================

# %% [0] Setup
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window as W

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
pd.set_option("display.max_rows", 60)
pd.set_option("display.width", 160)

def T(label, df):
    """Render a titled table AND return it. A bare name renders only as a cell's last expression;
    this always renders, and still hands back the object to export or plot."""
    out = df.toPandas() if hasattr(df, "toPandas") else df
    display(Markdown("**" + label + "**  ·  " + str(len(out)) + " rows"))
    display(out)
    return out

def top_writer(df, keys):
    """Collapse APP_SYS_CD to the dominant writer per group. Aggregates - never truncates:
    clients_total sums every writer, n_writers counts how many are behind the top one."""
    tot = df.groupBy(*keys).agg(F.sum("clients").alias("clients_total"),
                                F.countDistinct("APP_SYS_CD").alias("n_writers"))
    w = W.partitionBy(*keys).orderBy(F.col("clients").desc())
    top = (df.withColumn("rk", F.row_number().over(w)).filter("rk = 1")
             .select(*keys, F.col("APP_SYS_CD").alias("top_writer"), F.col("clients").alias("tw_clients")))
    return (tot.join(top, keys)
               .withColumn("top_writer_share", F.round(F.col("tw_clients") / F.col("clients_total"), 3))
               .select(*keys, "clients_total", "top_writer", "top_writer_share", "n_writers"))

BASE = "hdfs:///user/427966379/unsub_cpc/"
SWITCHES = [1002, 1012, 1014]
WIN_FROM, WIN_TO = "2025-07-01", "2026-07-01"

cpc  = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")
ub   = spark.read.parquet(BASE + "unsub_base/*")
recn = spark.read.parquet(BASE + "q2_recipients_named/*").distinct().withColumn("got_named", F.lit(1))

# No, defined per switch. Applied to rows that exist; a missing row is handled by the join, never here.
IS_NO = (F.when(F.col("PREF_ID").isin(1014, 1015), F.col("CLNT_CONSENT_TYP").isin(5002, 5003))
          .otherwise(F.col("CLNT_CONSENT_TYP") == 5002))
VALUE = (F.when(F.col("CLNT_CONSENT_TYP") == 5001, "yes")
          .when(F.col("CLNT_CONSENT_TYP") == 5002, "explicit_no")
          .when(F.col("CLNT_CONSENT_TYP") == 5003, "blank").otherwise("other"))

# One row per client: their first unsubscribe.
_w = W.partitionBy("CLNT_NO").orderBy(F.col("unsub_tm").asc(), F.col("TREATMENT_ID").asc())
unsub = (ub.withColumn("rn", F.row_number().over(_w)).filter("rn = 1")
           .select("CLNT_NO", "unsub_tm", F.expr("substring(TREATMENT_ID, 8, 3)").alias("unsub_mne")))
unsub.cache()

# One row per client and switch: their standing position.
_ws = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
standing = (cpc.withColumn("rn", F.row_number().over(_ws)).filter("rn = 1")
               .withColumn("value", VALUE).withColumn("is_no", IS_NO)
               .withColumn("yr", F.year("CHG_TMSTMP")))
standing.cache()

N_UNSUB = unsub.count()
display(Markdown("**Universe** · unsubscribers Jul 2025 – Jun 2026: **%s** · CPC preference rows: **%s**"
                 % (f"{N_UNSUB:,}", f"{cpc.count():,}")))

# ============================ 1. THE VENDOR SIDE ============================

# %% [1] U1 - email unsubscribes by month
u1 = T("U1 - email unsubscribes by month, Jul 2025 - Jun 2026",
       unsub.groupBy(F.date_format("unsub_tm", "yyyyMM").alias("month"))
            .agg(F.countDistinct("CLNT_NO").alias("clients_unsubscribed")).orderBy("month"))

# %% [2] U2 - which campaigns the unsubscribes came from
u2 = T("U2 - unsubscribes by source campaign, Jul 2025 - Jun 2026 (every campaign, no top-N)",
       unsub.withColumn("mne", F.when(F.trim(F.col("unsub_mne")) == "", F.lit("(untagged)"))
                                .otherwise(F.col("unsub_mne")))
            .groupBy("mne").agg(F.countDistinct("CLNT_NO").alias("clients_unsubscribed"))
            .orderBy(F.col("clients_unsubscribed").desc()))

# ========================== 2. THE CONSENT RECORD ===========================

# %% [3] C1 - standing position of every client holding a record, per switch
c1 = T("C1 - clients at each standing position, per switch (last event per client and switch)",
       standing.filter(F.col("PREF_ID").isin(SWITCHES + [1006]))
               .groupBy("PREF_ID").pivot("value", ["explicit_no", "blank", "yes", "other"])
               .agg(F.countDistinct("CLNT_NO")).orderBy("PREF_ID"))

# %% [4] C2 - "No" under each switch's own rule, and how it splits
c2 = T("C2 - clients standing No under each switch's own rule, and how that No was recorded",
       standing.filter(F.col("PREF_ID").isin(SWITCHES + [1006])).filter("is_no")
               .groupBy("PREF_ID")
               .agg(F.countDistinct("CLNT_NO").alias("standing_no_under_rule"),
                    F.countDistinct(F.when(F.col("value") == "explicit_no", F.col("CLNT_NO"))).alias("of_which_explicit"),
                    F.countDistinct(F.when(F.col("value") == "blank", F.col("CLNT_NO"))).alias("of_which_blank"))
               .orderBy("PREF_ID"))

# %% [5] C3 - which system wrote each standing position
_g = (standing.filter(F.col("PREF_ID").isin(SWITCHES + [1006]))
              .groupBy("PREF_ID", "value", "APP_SYS_CD").agg(F.countDistinct("CLNT_NO").alias("clients")))
c3 = T("C3 - who wrote the standing position, per switch and value",
       top_writer(_g, ["PREF_ID", "value"]).orderBy("PREF_ID", "value"))

# %% [6] C4 - when the standing blank on 1014 was written
_g = (standing.filter("PREF_ID = 1014 AND value = 'blank'")
              .groupBy("yr", "APP_SYS_CD").agg(F.countDistinct("CLNT_NO").alias("clients")))
c4 = T("C4 - year the standing blank on 1014 was written, and by which system",
       top_writer(_g, ["yr"]).orderBy("yr"))

# ==================== 3. THE UNSUBSCRIBERS' CONSENT STATE ===================

# %% [7] X1 - standing position of the 319,733 unsubscribers, per switch
_rows = []
for p in SWITCHES + [1006]:
    st = standing.filter("PREF_ID = " + str(p)).select("CLNT_NO", "value", "is_no")
    jj = unsub.select("CLNT_NO").join(st, "CLNT_NO", "left")
    _rows.append((p,
                  "blank=No" if p in (1014, 1015) else "blank=Yes",
                  N_UNSUB,
                  jj.filter("value = 'explicit_no'").count(),
                  jj.filter("value = 'blank'").count(),
                  jj.filter("value = 'yes'").count(),
                  jj.filter("value IS NULL").count(),
                  jj.filter("is_no").count()))
x1 = T("X1 - standing position of the unsubscribers on each switch (no_row = no consent record at all)",
       pd.DataFrame(_rows, columns=["PREF_ID", "blank_rule", "unsubscribers", "explicit_no", "blank",
                                    "yes", "no_row", "standing_no_under_rule"]))

# %% [8] X2 - was that standing position written before or after they unsubscribed
x2 = T("X2 - unsubscribers standing No: was the position written before or after their unsubscribe",
       standing.filter(F.col("PREF_ID").isin(SWITCHES)).filter("is_no")
               .join(unsub.select("CLNT_NO", "unsub_tm"), "CLNT_NO", "inner")
               .withColumn("when_written", F.when(F.col("CHG_TMSTMP") < F.col("unsub_tm"), "before_unsub")
                                            .otherwise("after_unsub"))
               .groupBy("PREF_ID", "value", "when_written")
               .agg(F.countDistinct("CLNT_NO").alias("clients"))
               .orderBy("PREF_ID", "value", "when_written"))

# =========================== 4. THE BRIDGE TEST =============================
# For every unsubscriber, how long until a "No" appears on each switch. z_never is the denominator:
# clients for whom one never appears. A hand-off between the systems clusters at a_0-1d.

_after = (cpc.filter(F.col("PREF_ID").isin(SWITCHES)).withColumn("is_no", IS_NO).filter("is_no")
             .join(unsub.select("CLNT_NO", "unsub_tm"), "CLNT_NO", "inner")
             .filter("CHG_TMSTMP > unsub_tm")
             .groupBy("CLNT_NO", "PREF_ID").agg(F.min("CHG_TMSTMP").alias("first_no_tm"),
                                                F.min("unsub_tm").alias("unsub_tm"))
             .withColumn("days", F.datediff("first_no_tm", "unsub_tm")))
_after.cache()

BUCKET = (lambda c: F.when(c <= 1, "a_0-1d").when(c <= 7, "b_2-7d").when(c <= 30, "c_8-30d")
                     .when(c <= 90, "d_31-90d").when(c <= 180, "e_91-180d").otherwise("f_180d+"))

_grid = unsub.select("CLNT_NO", "unsub_tm").crossJoin(
            spark.createDataFrame([(p,) for p in SWITCHES], ["PREF_ID"]))
_lag = (_grid.join(_after.select("CLNT_NO", "PREF_ID", "days"), ["CLNT_NO", "PREF_ID"], "left")
             .withColumn("lag_bucket", F.when(F.col("days").isNull(), F.lit("z_never"))
                                        .otherwise(BUCKET(F.col("days")))))

# %% [9] B1 - the bridge test, all unsubscribers
b1 = T("B1 - days from the unsubscribe until a No appears on each switch (z_never = it never does)",
       _lag.groupBy("lag_bucket").pivot("PREF_ID", SWITCHES)
           .agg(F.countDistinct("CLNT_NO")).orderBy("lag_bucket"))

# %% [10] B2 - the same test with a full 90 days of follow-up
b2 = T("B2 - same test, unsubscribes before 1 Apr 2026 only, so every client has >= 90 days of follow-up",
       _lag.filter("unsub_tm < DATE '2026-04-01'")
           .groupBy("lag_bucket").pivot("PREF_ID", SWITCHES)
           .agg(F.countDistinct("CLNT_NO")).orderBy("lag_bucket"))

# %% [11] B3 - when a No did appear, what kind was it and who wrote it
_wf = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").asc())
_first = (cpc.filter(F.col("PREF_ID").isin(SWITCHES)).withColumn("is_no", IS_NO).filter("is_no")
             .join(unsub.select("CLNT_NO", "unsub_tm"), "CLNT_NO", "inner")
             .filter("CHG_TMSTMP > unsub_tm")
             .withColumn("rk", F.row_number().over(_wf)).filter("rk = 1")
             .withColumn("value", VALUE)
             .withColumn("lag_bucket", BUCKET(F.datediff("CHG_TMSTMP", "unsub_tm"))))
_g = _first.groupBy("PREF_ID", "value", "lag_bucket", "APP_SYS_CD").agg(F.countDistinct("CLNT_NO").alias("clients"))
b3 = T("B3 - when a No did appear after the unsubscribe: explicit or blank, at what lag, written by whom",
       top_writer(_g, ["PREF_ID", "value", "lag_bucket"]).orderBy("PREF_ID", "value", "lag_bucket"))

# ============================== 5. DELIVERY =================================

# %% [12] D1 - named campaign email received Apr-Jun 2026, by standing position at 1 Apr
_asof = (cpc.filter("CHG_TMSTMP < DATE '2026-04-01'").filter(F.col("PREF_ID").isin(SWITCHES))
            .withColumn("rn", F.row_number().over(_ws)).filter("rn = 1")
            .withColumn("value", VALUE).withColumn("is_no", IS_NO))
d1 = T("D1 - clients who received named campaign email Apr-Jun 2026, by standing position at 1 Apr",
       _asof.join(recn, "CLNT_NO", "left")
            .groupBy("PREF_ID")
            .agg(F.countDistinct(F.when(F.col("is_no"), F.col("CLNT_NO"))).alias("standing_no"),
                 F.countDistinct(F.when(F.col("is_no") & (F.col("got_named") == 1), F.col("CLNT_NO"))).alias("no_got_email"),
                 F.countDistinct(F.when(F.col("value") == "yes", F.col("CLNT_NO"))).alias("standing_yes"),
                 F.countDistinct(F.when((F.col("value") == "yes") & (F.col("got_named") == 1), F.col("CLNT_NO"))).alias("yes_got_email"))
            .orderBy("PREF_ID"))

# %% [13] D2 - unsubscribers who received named campaign email in the following quarter
_pre = unsub.filter("unsub_tm < DATE '2026-04-01'")
d2 = T("D2 - clients who unsubscribed before Apr 2026 and received named campaign email Apr-Jun",
       pd.DataFrame([("unsubscribed before Apr 2026", _pre.count()),
                     ("of those, received named campaign email Apr-Jun",
                      _pre.join(recn.select("CLNT_NO"), "CLNT_NO", "inner").count())],
                    columns=["measure", "clients"]))

# ============================ 6. THE DECK FEED ==============================

# %% [14] DECK - every headline figure in one table. Photograph this cell.
_c2 = c2.set_index("PREF_ID"); _x1 = x1.set_index("PREF_ID")
_b1 = b1.set_index("lag_bucket")
_deck = [
    ("Clients who unsubscribed from email", N_UNSUB),
    ("Monthly average unsubscribes", int(round(u1["clients_unsubscribed"].mean()))),
    ("1014 - clients standing No under the rule", int(_c2.loc[1014, "standing_no_under_rule"])),
    ("1014 - of which recorded as an explicit No", int(_c2.loc[1014, "of_which_explicit"])),
    ("1014 - of which recorded as a blank", int(_c2.loc[1014, "of_which_blank"])),
    ("1002 - clients standing explicit No", int(_c2.loc[1002, "standing_no_under_rule"])),
    ("1012 - clients standing explicit No", int(_c2.loc[1012, "standing_no_under_rule"])),
    ("Unsubscribers standing No on 1014", int(_x1.loc[1014, "standing_no_under_rule"])),
    ("Unsubscribers with no 1014 record at all", int(_x1.loc[1014, "no_row"])),
    ("Unsubscribers where a No appeared within 1 day", int(_b1.loc["a_0-1d", "1014"]) if "a_0-1d" in _b1.index else 0),
    ("Unsubscribers where a No never appeared on 1014", int(_b1.loc["z_never", "1014"])),
]
deck = T("DECK FEED - every headline figure the slides use", pd.DataFrame(_deck, columns=["figure", "clients"]))
