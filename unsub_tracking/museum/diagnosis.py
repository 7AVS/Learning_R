# %% [0] SIZING DIAGNOSTIC - counts + timed calibrations that decide dump-raw vs join-server-side per table.
# Windows (Andre 2026-07-24): CPC + VENDOR EVENT + VENDOR MASTER >= 2024-01-01; TACTIC >= 2025-06-01.
# Run top to bottom, screenshot the [4] summary. Pure teradatasql - no Spark, no jar needed.
# Old env-diagnosis content retired 2026-07-24 (recoverable in git history).
get_ipython().system("./environment/bin/python -m pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")

# %% [1] Connect + helpers
import getpass, time, warnings
import pandas as pd
import teradatasql

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")
EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username, password=password, logmech="LDAP")

cur = EDW.cursor()
cur.execute("SELECT USER, SESSION, CURRENT_TIMESTAMP")
print("EDW round-trip returned:", cur.fetchall())
cur.close()

def edw_pd(sql):
    return pd.read_sql(sql, EDW)

def cal(label, sql):
    t0 = time.time(); pdf = edw_pd(sql); dt = max(time.time() - t0, 0.001)
    rps = len(pdf) / dt
    bpr = float(pdf.memory_usage(deep=True).sum()) / max(len(pdf), 1)
    print("CAL %-16s %7d rows %6.1f s -> %8.0f rows/s %6.0f B/row" % (label, len(pdf), dt, rps, bpr))
    return rps, bpr

def cnt(label, sql):
    t0 = time.time(); df = edw_pd(sql); dt = time.time() - t0
    print("CNT %-34s %6.1f s" % (label, dt)); print(df.to_string(index=False)); print()
    return df

t_all0 = time.time()

# %% [2] CALIBRATION - timed 100k pull per table measures the real pipe (Teradata -> socket -> pandas)
cal_cpc = cal("cpc_pref", "SELECT TOP 100000 CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD FROM DDWV01.CPC_RB_PREF_LOG WHERE PREF_ID IN (1002, 1012, 1014, 1006)")
cal_evt = cal("event", "SELECT TOP 100000 consumer_id_hashed, TREATMENT_ID, disposition_cd, disposition_dt_tm FROM DTZV01.VENDOR_FEEDBACK_EVENT WHERE disposition_dt_tm >= DATE '2026-06-01'")
cal_mst = cal("master", "SELECT TOP 100000 CLNT_NO, consumer_id_hashed, TREATMENT_ID, load_tm FROM DTZV01.VENDOR_FEEDBACK_MASTER WHERE load_tm >= DATE '2026-06-01'")
cal_tacn = cal("tactic_narrow", "SELECT TOP 100000 CLNT_NO, TACTIC_ID, TST_GRP_CD, TREATMT_STRT_DT FROM DG6V01.TACTIC_EVNT_IP_AR_HIST WHERE TREATMT_STRT_DT >= DATE '2026-06-01'")
cal_tacw = cal("tactic_wide", "SELECT TOP 100000 CLNT_NO, TACTIC_ID, TST_GRP_CD, TREATMT_STRT_DT, TACTIC_DECISN_VRB_INFO FROM DG6V01.TACTIC_EVNT_IP_AR_HIST WHERE TREATMT_STRT_DT >= DATE '2026-06-01'")

# %% [3] COUNTS - chunked statements (TDWM-sized bites), progressive prints so a mid-cell kill keeps earlier answers
c_cpc = cnt("cpc 4-switch: total vs 2024up", """
SELECT COUNT(*) AS all_rows,
       SUM(CASE WHEN CHG_TMSTMP >= DATE '2024-01-01' THEN 1 ELSE 0 END) AS rows_2024up
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014, 1006)""")

evt_rows = 0
for y0, y1 in [("2024-01-01", "2025-01-01"), ("2025-01-01", "2026-01-01"), ("2026-01-01", "2026-08-01")]:
    d = cnt("event %s..%s by disposition" % (y0, y1), """
SELECT disposition_cd, COUNT(*) AS rows_ct
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '%s' AND disposition_dt_tm < DATE '%s'
GROUP BY 1 ORDER BY 1""" % (y0, y1))
    evt_rows += int(d.iloc[:, 1].sum())

mst_rows = 0
mst_bounds = ["2024-01-01", "2024-07-01", "2025-01-01", "2025-07-01", "2026-01-01", "2026-07-01", "2026-08-01"]
for a, b in zip(mst_bounds[:-1], mst_bounds[1:]):
    d = cnt("master %s..%s" % (a, b), "SELECT COUNT(*) AS rows_ct FROM DTZV01.VENDOR_FEEDBACK_MASTER WHERE load_tm >= DATE '%s' AND load_tm < DATE '%s'" % (a, b))
    mst_rows += int(d.iloc[0, 0])

tac_rows = 0
for a, b in [("2025-06-01", "2026-01-01"), ("2026-01-01", "2026-08-01")]:
    d = cnt("tactic %s..%s by month" % (a, b), """
SELECT EXTRACT(YEAR FROM TREATMT_STRT_DT) AS yr, EXTRACT(MONTH FROM TREATMT_STRT_DT) AS mn, COUNT(*) AS rows_ct
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE TREATMT_STRT_DT >= DATE '%s' AND TREATMT_STRT_DT < DATE '%s'
GROUP BY 1, 2 ORDER BY 1, 2""" % (a, b))
    tac_rows += int(d.iloc[:, 2].sum())

# %% [4] SUMMARY - screenshot this (est_pull_min = transfer only; server scan/spool + TDWM queue on top)
summ = pd.DataFrame(
    [(name, rows, round(rows / rps / 60.0, 1), round(rows * bpr / 1e9, 2))
     for name, rows, (rps, bpr) in [
         ("cpc_pref 2024up (4 switches)", int(c_cpc.iloc[0, 1]), cal_cpc),
         ("event 2024up (all dispositions)", evt_rows, cal_evt),
         ("master 2024up", mst_rows, cal_mst),
         ("tactic 2025-06up NARROW cols", tac_rows, cal_tacn),
         ("tactic 2025-06up WIDE (+VRB_INFO)", tac_rows, cal_tacw)]],
    columns=["dataset", "rows", "est_pull_min", "est_pandas_GB"])
print(summ.to_string(index=False))
print("est_pull_min = transfer only (server scan/spool + TDWM queue on top); columns as in CAL pulls")
print("diagnostic total: %.1f min" % ((time.time() - t_all0) / 60.0))
