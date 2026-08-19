# %% [markdown]
# # 45 — monthly series since 2024-01: vendor feedback (dispo 1/4 x MNE) + CPC 1012 standing
#
# Feeds the deck's monthly-unsub comparison: vendor unsubs by MNE (-> LOB rollup at slice
# time) against the CPC 1012 No+blank line, same time axis. Assumes a live `spark` session.
# Data lands on HDFS idempotently - a killed session resumes where it stopped.

# %% [0] connections + HDFS helpers - prompts ONCE per kernel, later cells reuse EDW
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass
import pandas as pd

if "EDW" not in globals():
    EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com",
                              user=input("Teradata username: "),
                              password=getpass.getpass("Teradata password: "),
                              logmech="LDAP")

jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
print("EDW connection + HDFS helpers ready")

# %% [1] VENDOR MONTHLY since 2024-01 - month x MNE x disposition (1 = send, 4 = unsub),
# events + distinct clients. One bounded Teradata aggregate per month (bite discipline -
# a single 31-month scan of EVENT is a TDWM kill risk), each bite landed idempotently.
# MNE = raw SUBSTR(TREATMENT_ID, 8, 3), no recoding - LOB rollup happens at slice time.
VFB_BASE = "/user/427966379/unsub_cpc/vendor_monthly_mne/"
VFB_MONTHS = pd.date_range("2024-01-01", "2026-07-01", freq="MS").strftime("%Y-%m-%d").tolist()

for m0 in VFB_MONTHS:
    m1 = (pd.Timestamp(m0) + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    target = f"{VFB_BASE}month={m0[:7]}/"
    if fs.exists(jvm.org.apache.hadoop.fs.Path(target + "_SUCCESS")):
        print(f"{m0[:7]} already landed - skipping")
        continue
    bite = pd.read_sql(f"""
        SELECT SUBSTR(TREATMENT_ID, 8, 3)            AS mne,
               disposition_cd                        AS dispo,
               COUNT(*)                              AS n_events,
               COUNT(DISTINCT consumer_id_hashed)    AS n_ids
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd IN (1, 4)
          AND disposition_dt_tm >= DATE '{m0}'
          AND disposition_dt_tm <  DATE '{m1}'
        GROUP BY 1, 2
    """, EDW)
    bite.insert(0, "month", m0[:7])
    spark.createDataFrame(bite).write.mode("overwrite").parquet(target)
    print(f"{m0[:7]}: landed {len(bite)} mne x dispo rows")

vfb = spark.read.parquet(VFB_BASE).toPandas().sort_values(["month", "dispo", "mne"])
print("Vendor feedback monthly x MNE x disposition (1 = send, 4 = unsub), 2024-01 -> 2026-07:")
display(vfb)

# monthly totals view - the headline series (MNE detail stays in vfb for the LOB rollup)
vfb_tot = (vfb.pivot_table(index="month", columns="dispo", values="n_ids", aggfunc="sum")
              .rename(columns={1: "clients_sent", 4: "clients_unsub"}).reset_index())
print("Monthly totals (distinct ids per disposition):")
display(vfb_tot)

# %% [2] CPC MONTHLY since 2024-01 - standing 1012 counts per month-end by consent value
# (5002 explicit No + 5003 blank; 5001 lands free). One server-side aggregate, no
# attribution - CPC carries no MNE.
cpc_m = pd.read_sql("""
    SELECT MTH_END_DT, CLNT_CONSENT_TYP,
           CAST(COUNT(*) AS BIGINT) AS n_clients
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012
      AND MTH_END_DT >= DATE '2024-01-31'
    GROUP BY 1, 2
    ORDER BY 1, 2
""", EDW)
cpc_piv = (cpc_m.pivot_table(index="MTH_END_DT", columns="CLNT_CONSENT_TYP",
                             values="n_clients", aggfunc="sum")
                .rename(columns={5001: "n_5001_yes", 5002: "n_5002_no", 5003: "n_5003_blank"})
                .reset_index())
print("CPC 1012 standing per month-end (2024-01 -> latest), by consent value:")
display(cpc_piv)
