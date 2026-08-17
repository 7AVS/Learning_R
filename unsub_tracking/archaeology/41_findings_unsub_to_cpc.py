# %% [markdown]
# # 41 — What we learned: the email unsubscribe → CPC pipeline (evidence notebook)
#
# Every finding below is backed by a query that re-runs live against the source tables —
# reproducible and auditable. Frame: 18 months (CHG_TMSTMP >= 2025-02-01) unless a cell
# says otherwise. Switch-offs = explicit `CLNT_CONSENT_TYP = 5002` writes on
# `DDWV01.CPC_RB_PREF`.
#
# ## The documented process (source: "RB-Client CPC Personal Unsubscribe", Digital
# ## Messaging team space + the "RBC Clients Unsubscribe Blueprint" doc, catalogued in
# ## `unsub_tracking/sfmc_unsub_blueprint_notes.md`)
#
# 1. Every promotional email footer carries an unsubscribe link with the client's hashed
#    SRF (SubscriberKey), the send's Job ID (`jid`), and at most ONE optional LOB
#    preference code (`cpc=`).
# 2. The landing page offers TWO options: the campaign's dedicated LOB switch (only if
#    the link carried a code), and — mandatory on every page per CASL — "unsubscribe from
#    promotional emails from RBC Royal Bank" = **1012, Communication - Email**.
# 3. Submit → processing page → SFMC data extension (RB_CPC_PROD) → **FTP backfeed at
#    3:30 AM Mon-Sat / 9:00 AM Sun** → lands in the CPC table stamped APP_SYS_CD 7020.
#    (The document does NOT name the EDW table it feeds — the 7020 writes are how we
#    located the landing point empirically.)
# 4. Send-time suppression applies AUTO-SUPPRESSION LISTS in every sender profile:
#    CPC1012 (clients standing 1012=5002, rebuilt daily), the LOB CPC lists, CPCEMO,
#    and EMO_SuppressionList — the shared-email-address list (if one holder of an email
#    address opts out, everyone on that address is suppressed) which never writes CPC
#    at all. Suppression is therefore BROADER than consent: CPC understates who is
#    actually unmailable.
# 5. The consent codes: 5001 Yes / 5002 No / 5003 blank ("no feedback received or
#    unable to confirm agreement").
#
# The findings [F1]-[F8] test this documented picture against the data and measure what
# it implies for attribution.

# %% [0] connect + proof round-trip
try:
    import teradatasql
except ImportError:
    get_ipython().system("pip install teradatasql -i https://artifactory.fg.rbc.com/artifactory/api/pypi/pypi-remote/simple --trusted-host artifactory.fg.rbc.com")
    import teradatasql
import getpass
import pandas as pd
import matplotlib.pyplot as plt

username = input("Enter your username: ")
password = getpass.getpass("Enter your password: ")

EDW = teradatasql.connect(host="Teradata-dns-sysa.fg.rbc.com", user=username,
                          password=password, logmech="LDAP")

def edw_pd(sql, chunksize=1_000_000):
    cur = EDW.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    parts = []
    while True:
        rows = cur.fetchmany(chunksize)
        if not rows:
            break
        parts.append(pd.DataFrame(rows, columns=cols))
    cur.close()
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)

display(edw_pd("SELECT USER AS usr, SESSION AS sess, CURRENT_TIMESTAMP AS ts"))
pd.set_option("display.max_colwidth", 80)

# %% [D] Decodes (preferences: Borealis rule + SFMC page + OnePATH schema; writers: EDW dictionary)
PREF_DESC = {
    1001: "Entity Level Marketing - DI", 1002: "Entity Level Marketing - RBC",
    1004: "Accounts & Packages", 1006: "Credit Cards", 1007: "Banking - Direct Mail",
    1008: "Banking - Telephone", 1009: "Banking - RBC Online", 1010: "Creditor Insurance",
    1012: "Banking - E-Mail (CASL)", 1013: "Banking - Face to Face",
    1014: "Info Share for Marketing - Banking", 1015: "Info Share for Marketing - Service",
    1016: "Entity Level Marketing - Credit Bureau",
    1023: "Investments - Registered", 1024: "Investments - Non-Registered",
    1025: "Loans & Lines of Credit", 1026: "Mortgages",
    1036: "Info Share for Marketing - Online Personalization",
    1044: "Travel Health Insurance", 1045: "E-Newsletter - Banking",
    1046: "E-Newsletter - Rewards", 1048: "Banking - ATM", 1057: "Info Share for Marketing - DI",
}
SYS_DESC = {
    7001: "Sales Platform (branch staff)", 7003: "Royal Direct / Client View (contact centre)",
    7004: "Online Banking", 7006: "RBC Banking (STaR UI, batch/purge)", 7016: "RBC.COM",
    7020: "Exact Target (email ESP - the unsubscribe page)",
    7027: "D&H", 7033: "?? not in dictionary", 7053: "?? not in dictionary",
    7999: "Default Application System", 99999: "batch update (SRF consolidation)",
}

# %% [F1] FINDING 1 — email is one door among many: who writes 1012 switch-offs
f1 = edw_pd("""
SELECT APP_SYS_CD, COUNT(*) AS n_1012_switch_offs
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1 ORDER BY 2 DESC
""")
f1.insert(1, "system", [SYS_DESC.get(s, "other") for s in f1["APP_SYS_CD"]])
f1["share_pct"] = (100 * f1["n_1012_switch_offs"] / f1["n_1012_switch_offs"].sum()).round(1)
print("Who writes 1012 (email consent) to No. The unsubscribe page (7020) is ~a quarter;")
print("branch and phone staff write the majority - email metrics never see those:")
display(f1)

# %% [F2] FINDING 2 — the email pipe is exactly one batch: hour-of-day of 7020 writes
f2 = edw_pd("""
SELECT EXTRACT(HOUR FROM CHG_TMSTMP) AS hour_of_write, COUNT(*) AS n_writes
FROM DDWV01.CPC_RB_PREF
WHERE APP_SYS_CD = 7020 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1 ORDER BY 1
""")
f2["share_pct"] = (100 * f2["n_writes"] / f2["n_writes"].sum()).round(1)
print("Nearly all 7020 writes land in one early-morning hour - the documented 3:30 AM FTP")
print("backfeed. One automation, one pipe; no second email-side writer exists:")
display(f2)

# %% [F3] FINDING 3 — the page's footprint matches its documentation exactly
ON_PAGE = {1012: "mandatory option", 1004: "LOB option", 1006: "LOB option",
           1010: "LOB option", 1023: "LOB option", 1024: "LOB option", 1025: "LOB option",
           1026: "LOB option", 1044: "LOB option", 1045: "LOB option", 1046: "LOB option",
           1002: "legacy (old form only)"}
f3 = edw_pd("""
SELECT PREF_ID, COUNT(*) AS n_writes
FROM DDWV01.CPC_RB_PREF
WHERE APP_SYS_CD = 7020 AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1 ORDER BY 2 DESC
""")
f3.insert(1, "switch", [PREF_DESC.get(p, "??") for p in f3["PREF_ID"]])
f3.insert(2, "on_the_page_as", [ON_PAGE.get(p, "NOT on the documented page") for p in f3["PREF_ID"]])
f3["share_pct"] = (100 * f3["n_writes"] / f3["n_writes"].sum()).round(1)
print("Every switch the email pipe has ever written is on the documented page - nothing")
print("else. ~70% take the mandatory global option; the Rewards newsletter dominates the")
print("dedicated options:")
display(f3)

# %% [F4] FINDING 4 — the email channel acts alone: no mixing with other systems
f4 = edw_pd("""
SELECT CASE WHEN n_other = 0 THEN '0 other-system switches within +/-1 day (stands alone)'
            ELSE                  '1+ other-system switches nearby (coincidence-level)' END
         AS around_each_email_unsub,
       COUNT(*) AS n_esp_writes
FROM (
    SELECT a.CLNT_NO, a.PREF_ID, a.CHG_TMSTMP, COUNT(b.PREF_ID) AS n_other
    FROM DDWV01.CPC_RB_PREF a
    LEFT JOIN DDWV01.CPC_RB_PREF b
      ON  b.CLNT_NO = a.CLNT_NO AND b.APP_SYS_CD <> 7020 AND b.CLNT_CONSENT_TYP = 5002
      AND CAST(b.CHG_TMSTMP AS DATE) BETWEEN CAST(a.CHG_TMSTMP AS DATE) - 1
                                         AND CAST(a.CHG_TMSTMP AS DATE) + 1
    WHERE a.APP_SYS_CD = 7020 AND a.CLNT_CONSENT_TYP = 5002
      AND a.CHG_TMSTMP >= DATE '2025-02-01'
    GROUP BY 1, 2, 3
) t GROUP BY 1 ORDER BY 1
""")
f4["share_pct"] = (100 * f4["n_esp_writes"] / f4["n_esp_writes"].sum()).round(2)
print("For every email-pipe write: did any OTHER system write consent for the same client")
print("within +/-1 day? ~99.9% stand alone - a 7020 write IS a client's unsubscribe")
print("submission, not a fragment of some larger multi-system process:")
display(f4)

# %% [F5] FINDING 5 — the attribution mask: 70% of email unsubs carry no campaign identity
# No query needed beyond [F3]: 1012 is the mandatory option on EVERY promotional email,
# so a 1012 write identifies no campaign. Only the ~30% who choose the dedicated option
# self-identify (and that is dominated by one program). The send's Job ID (jid) IS in the
# unsubscribe URL and reaches the SFMC data extension - but no extract of it has been
# found in EDW (dictionary sweep, pack 40). Deterministic send-level attribution exists
# only inside the Digital Messaging platform.
print("See [F3]: 1012 = 70.9% of the email pipe's writes, campaign-anonymous by design.")
print("jid (the exact send) reaches SFMC but, as far as swept, never reaches EDW.")

# %% [F6] FINDING 6 — campaign history CAN see email unsubs, at week precision
f6 = edw_pd("""
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2025-02-01'
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
      ON  t.CLNT_NO = f.CLNT_NO AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-02-01'
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT CASE WHEN em.email_dt IS NULL              THEN '6_no_email_decision_found'
            WHEN f.flip_dt - em.email_dt <= 1     THEN '1_same_or_next_day'
            WHEN f.flip_dt - em.email_dt <= 7     THEN '2_within_week'
            WHEN f.flip_dt - em.email_dt <= 30    THEN '3_within_month'
            ELSE                                       '4_over_30_days' END AS last_campaign_email_decision,
       COUNT(*) AS n_clients
FROM flips f LEFT JOIN last_email em ON em.CLNT_NO = f.CLNT_NO
GROUP BY 1 ORDER BY 1
""")
f6["share_pct"] = (100 * f6["n_clients"] / f6["n_clients"].sum()).round(1)
print("Proven email unsubs vs campaign (tactic) history: ~98% have a prior email decision,")
print("~3/4 within a week - so LAST-SEND attribution is viable at WEEK precision, never")
print("send precision (decisions are not deliveries; several campaigns can share a week).")
print("The small no-decision group = mail streams outside campaign decisioning (newsletters):")
display(f6)

# %% [F7] FINDING 7 — the vendor unsub feed misses most real email unsubs (3-month slice)
# The one feed that LOOKS like unsub events (VENDOR_FEEDBACK disposition 4) fails on
# ground truth: most proven email unsubs have NO disposition-4 anywhere. The capture
# mechanism behind disposition-4 is undocumented; open question for Digital Messaging.
f7 = edw_pd("""
WITH esp_flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 AND APP_SYS_CD = 7020
      AND CHG_TMSTMP >= DATE '2026-05-01'                 -- 3-month live slice
),
unsubs AS (
    SELECT DISTINCT m.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
      ON  m.consumer_id_hashed = e.consumer_id_hashed AND m.TREATMENT_ID = e.TREATMENT_ID
    WHERE e.disposition_cd = 4 AND e.disposition_dt_tm >= DATE '2026-04-01'
      AND m.load_tm >= DATE '2026-03-01'
)
SELECT CASE WHEN u.CLNT_NO IS NOT NULL THEN 'vendor disposition-4 exists before the flip'
            ELSE                            'NO disposition-4 found (capture miss)' END AS vendor_feed,
       COUNT(*) AS n_proven_email_unsubs
FROM esp_flips f
LEFT JOIN (
    SELECT DISTINCT f2.CLNT_NO
    FROM esp_flips f2 JOIN unsubs u2
      ON u2.CLNT_NO = f2.CLNT_NO AND u2.unsub_dt <= f2.flip_dt
) u ON u.CLNT_NO = f.CLNT_NO
GROUP BY 1 ORDER BY 1
""")
f7["share_pct"] = (100 * f7["n_proven_email_unsubs"] / f7["n_proven_email_unsubs"].sum()).round(1)
print("Every flip here IS an email unsubscription (written by the page's pipe), so every")
print("'no disposition-4' is a pure capture miss by the vendor feed:")
display(f7)

# %% [F8] FINDING 8 — suppression is broader than consent (no query possible from EDW)
# Documented, not measurable from our side:
#  - Every sender profile applies auto-suppression lists ON TOP of consent: CPC1012
#    (standing 1012=5002, rebuilt daily from RB_CPC_PROD via RB Email v2 hash-match),
#    the LOB CPC lists, CPCEMO (daily kill file), and EMO_SuppressionList.
#  - EMO_SuppressionList suppresses by SHARED EMAIL ADDRESS: one joint holder opts out,
#    every client on that address stops receiving mail - with NO CPC write for the others.
#  - Three "Service" sender profiles carry no suppression at all (legal exemption).
# Consequence: the CPC table understates the truly unmailable population by the EMO
# layer (size unknown - no EDW extract found), and reach computed from consent alone
# overstates deliverable audience.
print("Documented in unsub_tracking/sfmc_unsub_blueprint_notes.md - no EDW dataset found")
print("for the EMO shared-address layer; its size is an open question for Digital Messaging.")

# %% [markdown]
# ## Conclusions
#
# | Question | Status |
# |---|---|
# | Is a 7020 write a real client email unsubscription? | **Yes — proven** (single pipe [F2], page-only footprint [F3], acts alone [F4]) |
# | Channel attribution (email vs branch vs phone)? | **Clean** — the writer code is the attribution [F1] |
# | Which campaign caused an email unsub? | **~30% self-identified** (dedicated option, [F3]); **70% masked** (mandatory 1012, [F5]); last-send heuristic works at week precision [F6] |
# | Can the vendor unsub feed (disposition-4) attribute unsubs? | **No** — misses most real email unsubs on ground truth [F7] |
# | Does CPC capture everyone who is unmailable? | **No** — the EMO shared-address suppression layer never touches CPC [F8] |
#
# ## Open asks (each one sentence, each unblocks a step)
# 1. **Digital Messaging:** does RB_CPC_PROD retain `jid` per opt-out, and can we get an
#    extract? (→ deterministic send-level attribution for the masked 70%.)
# 2. **Digital Messaging:** which event emits VENDOR_FEEDBACK disposition-4 — footer
#    click, page submit, or mail-app one-click? (→ explains [F7].)
# 3. **Digital Messaging / CPC team:** is there any EDW extract of EMO_SuppressionList?
#    (→ sizes the invisible suppression layer.)
