# Which switch does campaign selection actually read? HDFS only, no Teradata.
#
# THE CONTRADICTION THIS RESOLVES (from 17-C T3, both figures verified):
#   1014 = No got email 61.4%.  1014 = Yes got email 32.3%.
# 1014 is documented as the parameter campaign decisioning reads to select clients. If it gates
# selection, that result is backwards. Three explanations survive and this pack separates them:
#   (a) decisioning does not read 1014 as documented - the switch is inert and the 3.6M "No" is a
#       phantom that costs nothing;
#   (b) it does read it and something downstream overrides - program consent, a separate suppression
#       list, or a different key;
#   (c) 1014 = Yes is a small selected population that gets less mail for unrelated reasons.
#
# METHOD: hold each switch constant and vary the others. Whichever switch moves email receipt is the
# one selection responds to. If none of the three moves it, none of them is the key and the answer is
# outside CPC. No causal claim is made - this is a descriptive contrast on observational data.
#
# Standing = last event per (client, switch) BEFORE 1 Apr 2026. Receipt = named campaign mail Apr-Jun.

# %% [0] Setup
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql import functions as F, Window as W

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
pd.set_option("display.max_rows", 60)
pd.set_option("display.width", 170)

def T(label, df):
    if hasattr(df, "toPandas"):
        for f in df.schema.fields:
            if f.dataType.typeName() in ("timestamp", "date", "timestamp_ntz"):
                df = df.withColumn(f.name, F.date_format(F.col(f.name), "yyyy-MM-dd"))
        out = df.toPandas()
    else:
        out = df
    display(Markdown("**" + label + "**  ·  " + str(len(out)) + " rows"))
    display(out)
    return out

BASE = "hdfs:///user/427966379/unsub_cpc/"
ASOF = "2026-04-01"
cpc  = spark.read.option("recursiveFileLookup", "true").parquet(BASE + "cpc_pref")
recn = spark.read.parquet(BASE + "q2_recipients_named/*").distinct().withColumn("got", F.lit(1))

IS_NO = (F.when(F.col("PREF_ID").isin(1014, 1015), F.col("CLNT_CONSENT_TYP").isin(5002, 5003))
          .otherwise(F.col("CLNT_CONSENT_TYP") == 5002))

# Standing position per client per switch as at ASOF, collapsed to No / not-No under each switch's rule.
_w = W.partitionBy("CLNT_NO", "PREF_ID").orderBy(F.col("CHG_TMSTMP").desc())
st = (cpc.filter("CHG_TMSTMP < DATE '%s'" % ASOF)
         .filter(F.col("PREF_ID").isin(1002, 1012, 1014))
         .withColumn("rn", F.row_number().over(_w)).filter("rn = 1")
         .withColumn("pos", F.when(IS_NO, "No").otherwise("not_No"))
         .select("CLNT_NO", "PREF_ID", "pos"))

# One row per client, one column per switch. 'absent' = no record on that switch, kept as its own
# level rather than folded into either side - a client with no row is not a No (see LOCKED FACTS 4a).
cube = (st.groupBy("CLNT_NO").pivot("PREF_ID", [1002, 1012, 1014]).agg(F.first("pos"))
          .withColumnRenamed("1002", "s1002").withColumnRenamed("1012", "s1012")
          .withColumnRenamed("1014", "s1014")
          .fillna("absent", ["s1002", "s1012", "s1014"])
          .join(recn.select("CLNT_NO", "got"), "CLNT_NO", "left").fillna(0, ["got"]))
cube.cache()

# %% [1] Q1 - the full cube. Every combination of the three switches x whether they got named mail.
q1 = T("Q1 - clients by standing position on all three switches, and how many received named campaign mail",
       cube.groupBy("s1002", "s1012", "s1014")
           .agg(F.countDistinct("CLNT_NO").alias("clients"),
                F.sum("got").alias("got_named_email"))
           .withColumn("pct_got", F.round(100 * F.col("got_named_email") / F.col("clients"), 1))
           .orderBy(F.col("clients").desc()))

# %% [2] Q2 - hold two switches, vary the third. The switch whose rows move is the one receipt responds to.
_rows = []
for tgt, keep in [("s1014", ["s1002", "s1012"]), ("s1002", ["s1012", "s1014"]), ("s1012", ["s1002", "s1014"])]:
    d = (cube.filter((F.col(keep[0]) == "not_No") & (F.col(keep[1]) == "not_No"))
             .groupBy(F.col(tgt).alias("position"))
             .agg(F.countDistinct("CLNT_NO").alias("clients"), F.sum("got").alias("got_named_email")))
    for r in d.collect():
        _rows.append((tgt, "%s = not_No AND %s = not_No" % (keep[0], keep[1]), r["position"],
                      r["clients"], r["got_named_email"],
                      round(100.0 * r["got_named_email"] / r["clients"], 1) if r["clients"] else None))
q2 = T("Q2 - one switch varied with the other two held at not_No: does receipt move?",
       pd.DataFrame(_rows, columns=["switch_varied", "others_held_at", "position", "clients",
                                    "got_named_email", "pct_got"]))

# %% [3] Q3 - the clean single-flag cohorts: clients who are No on exactly one of the three.
# If a switch gates selection, its exclusive-No cohort should show the lowest receipt rate.
_n = (F.when(F.col("s1002") == "No", 1).otherwise(0) + F.when(F.col("s1012") == "No", 1).otherwise(0)
      + F.when(F.col("s1014") == "No", 1).otherwise(0))
q3 = T("Q3 - clients standing No on exactly one switch, and their named-mail receipt rate",
       cube.withColumn("n_no", _n).filter("n_no = 1")
           .withColumn("only_no_on", F.when(F.col("s1002") == "No", "1002")
                                      .when(F.col("s1012") == "No", "1012").otherwise("1014"))
           .groupBy("only_no_on")
           .agg(F.countDistinct("CLNT_NO").alias("clients"), F.sum("got").alias("got_named_email"))
           .withColumn("pct_got", F.round(100 * F.col("got_named_email") / F.col("clients"), 1))
           .orderBy("only_no_on"))

# %% [4] Q4 - on 1014 only, does an EXPLICIT No behave differently from a blank? If decisioning reads
# the raw value rather than the rule, explicit and blank will separate. If it reads the rule, they will not.
st14 = (cpc.filter("CHG_TMSTMP < DATE '%s' AND PREF_ID = 1014" % ASOF)
           .withColumn("rn", F.row_number().over(_w)).filter("rn = 1")
           .withColumn("v", F.when(F.col("CLNT_CONSENT_TYP") == 5002, "explicit_no")
                             .when(F.col("CLNT_CONSENT_TYP") == 5003, "blank")
                             .when(F.col("CLNT_CONSENT_TYP") == 5001, "yes").otherwise("other"))
           .select("CLNT_NO", "v")
           .join(recn.select("CLNT_NO", "got"), "CLNT_NO", "left").fillna(0, ["got"]))
q4 = T("Q4 - 1014 standing value vs named-mail receipt: does an explicit No behave like a blank?",
       st14.groupBy("v").agg(F.countDistinct("CLNT_NO").alias("clients"), F.sum("got").alias("got_named_email"))
           .withColumn("pct_got", F.round(100 * F.col("got_named_email") / F.col("clients"), 1))
           .orderBy(F.col("clients").desc()))

# %% [5] Q5 - how to read the result
display(Markdown("""
**Reading Q2 and Q3**

- A switch that gates selection shows a **large drop** in `pct_got` between `No` and `not_No` while the
  other two are held constant, and its exclusive-No cohort in Q3 has the lowest rate.
- A switch that is **inert** shows roughly the same `pct_got` either way. If that is 1014, then the
  3,638,798 clients standing No on it are not being excluded from anything, and the population is a
  bookkeeping artefact rather than lost reach.
- If **none** of the three moves receipt, selection is not reading any of them and the key is outside
  CPC - which makes the suppression-list question the only one worth asking of the owners.
- **Q4 separates rule from raw value.** If explicit No and blank show different rates, whatever reads
  1014 reads the raw code and does not apply the blank rule. That would matter more than any single
  number in this study.

These are contrasts on observational data. Cohorts differ in tenure, product mix and program membership,
so a gap here is a description of what the records show, not a measured effect of the switch.
"""))
