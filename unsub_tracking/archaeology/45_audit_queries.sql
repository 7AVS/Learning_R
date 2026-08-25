-- ============================================================================
-- 45_audit_queries.sql — standalone audit surface for the contactable-base deck
-- Every query stands on its own (Teradata SQL, run in Teradata Studio / BTEQ).
-- These are THE queries behind the deck outputs - no caching, no Python; runtime
-- may be long, correctness over speed.
--
-- UNIVERSE RULE (Andre 2026-08-21): all queries scope to active personal
-- clients - CLNT_TYP_CD=1 on CPC_RB_PREF_MTHLY, AND every CPC/event read is
-- merged to RB_CLNT_DLY CLNT_STS='A' at the EQUIVALENT DATE (month-end grain):
-- a month-end standing count uses that month-end's status snapshot, an event
-- uses its own month's month-end, the waterfall checks BOTH anchors. Never one
-- fixed snapshot across a time series.
--
-- THE LOCKED EVENT+MASTER MERGE (used identically wherever vendor data appears):
--   * join on BOTH keys: consumer_id_hashed AND TREATMENT_ID
--   * MASTER reduced to DISTINCT (consumer_id_hashed, TREATMENT_ID, CLNT_NO),
--     CLNT_NO IS NOT NULL (MASTER is not 1:1 - raw join inflates ~11%)
--   * EVENT side shape-guarded to 10-char dated treatment ids
--     (CHARACTER_LENGTH = 10, numeric 7-prefix) - DEFAULT/CABVRSN1 excluded by rule
--   * MASTER scan anchored by SUBSTR(TREATMENT_ID,1,7) = deployment date (YYYYDDD
--     julian): '2023274' = 2023-10-01 (3 months before the frame - an unsub
--     references the SEND's master row), '2026212' = 2026-07-31
--
-- Time axis (decision 2026-08-19): DISPOSITION-4 date - an unsub counts in the
-- month the client clicked, never the campaign deployment month.
--
-- UCP queries: see the APPENDIX at the bottom - ucp4 exists ONLY as parquet on
-- the lake (not in any warehouse/Starburst), so those statements run as Spark
-- SQL. The SQL semantics are identical; only the engine differs, and each is
-- labeled. The one line of Python involved registers the parquet as a view -
-- plumbing, not logic.
-- ============================================================================


-- coverage check (RUN ONCE BEFORE Q1/Q2/Q3 - now load-bearing): the date-matched
-- joins need EVERY month-end 2024-01-31 .. 2026-07-31 present in RB_CLNT_DLY.
-- If the daily table keeps only a rolling window, history joins silently empty out.
-- SELECT SNAP_DT, COUNT(*) AS n
-- FROM DDWV01.RB_CLNT_DLY
-- WHERE SNAP_DT >= DATE '2024-01-31' AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
-- GROUP BY 1 ORDER BY 1;   -- expect ~31 month-end rows, each in the tens of millions

-- [Q0] UNIVERSE CODES PROBE - informational. Only the CLNT_STS open/active
--      code ('A') feeds the status CTEs below (act in Q1/Q2, u_a/u_b in Q3);
--      CLNT_TYP plays no role there (personal-vs-non comes from CLNT_TYP_CD=1
--      on CPC_RB_PREF_MTHLY instead). If row_count <> distinct_clnt_count the
--      table is not client-grain at a snapshot.
--      Q0 run 2026-08-21: table is client-grain (row_count = distinct everywhere).
SELECT CLNT_TYP, CLNT_STS,
       COUNT(*) AS row_count,
       COUNT(DISTINCT CLNT_NO) AS distinct_clnt_count
FROM DDWV01.RB_CLNT_DLY
WHERE SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.RB_CLNT_DLY WHERE SNAP_DT >= DATE - 7)
GROUP BY CLNT_TYP, CLNT_STS
ORDER BY CLNT_TYP, CLNT_STS;


/* ============================================================================
[Q1] MONTHLY VENDOR ACTIVITY x MNE since 2024-01 - ONE table, sends and unsubs
     side by side (Andre 2026-08-20). clnt_no grain both measures:
       sent_clients  = distinct clients mailed by that MNE that month
       unsub_clients = distinct clients whose FIRST unsub of the month was that
                       MNE (multi-MNE clients count once -> unsub_clients sums
                       to distinct unsubscribers per month)
     One scan of EVENT (disp 1 and 4), one MASTER join (locked merge).
============================================================================ */
WITH act AS (
    -- ACTIVE spine, date-matched (Andre 2026-08-21): one row per month-end x
    -- active client. Each event below joins to the status snapshot of ITS OWN
    -- month - never one fixed snapshot across the series. Month-end-only rows
    -- kept via EXTRACT (SNAP_DT+1 = day 1 <=> SNAP_DT is a month-end); if this
    -- predicate defeats partition pruning and the scan crawls, swap it for an
    -- explicit SNAP_DT IN (...) list of month-ends.
    SELECT SNAP_DT, CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted - CHAR column
      AND SNAP_DT >= DATE '2024-01-31'
      AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
      AND CLNT_TYP = 1   -- PERSONAL only (Andre 2026-08-21: every query, every number).
                         -- Source: RB_CLNT_DLY type code; Q0 shows type 1 = 29.4M
                         -- (personal-scale) vs type 2 = 3.9M. CPC-side queries use
                         -- CLNT_TYP_CD = 1 on CPC_RB_PREF_MTHLY for the same cut.
),
base AS (
    SELECT m.CLNT_NO,
           e.disposition_cd,
           e.disposition_dt_tm AS dt,
           e.TREATMENT_ID,
           SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
           TRIM(EXTRACT(YEAR FROM e.disposition_dt_tm)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM e.disposition_dt_tm) < 10
                       THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM e.disposition_dt_tm))       AS evt_month
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2023274' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    -- active check at the event's own month-end: the client must be 'A' in the
    -- month the send/unsub happened, not merely today
    INNER JOIN act ON act.CLNT_NO = m.CLNT_NO
                  AND act.SNAP_DT = ADD_MONTHS(CAST(e.disposition_dt_tm AS DATE)
                                               - EXTRACT(DAY FROM e.disposition_dt_tm) + 1, 1) - 1
    WHERE e.disposition_cd IN (1, 4)
      AND e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
sends AS (
    SELECT evt_month, mne, CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS sent_clients
    FROM base
    WHERE disposition_cd = 1
    GROUP BY 1, 2
),
unsub_ranked AS (
    -- TWO ranks per unsub row (Andre 2026-08-24):
    --   rn_month  - first per client per MONTH  -> monthly view (a client counts
    --               once each month they unsub; repeats across months reappear)
    --   rn_window - first per client EVER       -> unique view (client counts
    --               once in the whole window, in the month + MNE of their first
    --               unsub; window sum = unique unsubscribing clients)
    SELECT evt_month, CLNT_NO, mne,
           ROW_NUMBER() OVER (PARTITION BY CLNT_NO, evt_month
                              ORDER BY dt ASC, mne ASC, TREATMENT_ID ASC) AS rn_month,
           ROW_NUMBER() OVER (PARTITION BY CLNT_NO
                              ORDER BY dt ASC, mne ASC, TREATMENT_ID ASC) AS rn_window
    FROM base
    WHERE disposition_cd = 4
),
unsubs AS (
    SELECT evt_month, mne,
           CAST(SUM(CASE WHEN rn_month  = 1 THEN 1 ELSE 0 END) AS BIGINT) AS unsub_clients,
           CAST(SUM(CASE WHEN rn_window = 1 THEN 1 ELSE 0 END) AS BIGINT) AS first_unsub_clients
    FROM unsub_ranked
    GROUP BY 1, 2
)
SELECT COALESCE(s.evt_month, un.evt_month) AS evt_month,
       COALESCE(s.mne, un.mne)             AS mne,
       COALESCE(s.sent_clients, 0)         AS sent_clients,
       COALESCE(un.unsub_clients, 0)       AS unsub_clients,
       COALESCE(un.first_unsub_clients, 0) AS first_unsub_clients
FROM sends s
FULL OUTER JOIN unsubs un
  ON un.evt_month = s.evt_month AND un.mne = s.mne
ORDER BY 1, 2;


/* ============================================================================
[Q2] CPC 1012 MONTHLY x WRITER - one table, stock AND flow side by side
     (Q4 merged in, Andre 2026-08-20: same grain = same query).
     Grain: month x APP_SYS_CD. Columns:
       n_5001_yes / n_5002_no / n_5003_blank = STANDING at that month-end,
         decomposed by the system that last wrote each client's row
       write columns = NEW writes to explicit No DURING that month by that
         system (flow; from the write timestamp on the standing mirror -
         survivor caveat: a No later overwritten by re-consent drops out, ~0.1%)
============================================================================ */
WITH act AS (
    -- ACTIVE spine, date-matched (Andre 2026-08-21): one row per month-end x
    -- active client. Standing joins at its OWN MTH_END_DT; writes join at the
    -- month-end of the write's month - never one fixed snapshot across the
    -- series. Swap the EXTRACT month-end predicate for an explicit
    -- SNAP_DT IN (...) list if partition pruning suffers.
    SELECT SNAP_DT, CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted - CHAR column
      AND SNAP_DT >= DATE '2024-01-31'
      AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
      AND CLNT_TYP = 1   -- PERSONAL only (Andre 2026-08-21: every query, every number).
                         -- Source: RB_CLNT_DLY type code; Q0 shows type 1 = 29.4M
                         -- (personal-scale) vs type 2 = 3.9M. CPC-side queries use
                         -- CLNT_TYP_CD = 1 on CPC_RB_PREF_MTHLY for the same cut.
),
standing AS (
    SELECT TRIM(EXTRACT(YEAR FROM MTH_END_DT)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM MTH_END_DT) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM MTH_END_DT))                AS mth,
           APP_SYS_CD,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS BIGINT) AS n_5001_yes,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS BIGINT) AS n_5002_no,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS BIGINT) AS n_5003_blank,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5004 THEN 1 ELSE 0 END) AS BIGINT) AS n_5004_yes_credit_bureau,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP NOT IN (5001, 5002, 5003, 5004)
                         THEN 1 ELSE 0 END) AS BIGINT)                              AS n_other_value
    FROM DDWV01.CPC_RB_PREF_MTHLY p
    -- active check at the SAME month-end the standing is measured at
    INNER JOIN act ON act.CLNT_NO = p.CLNT_NO
                  AND act.SNAP_DT = p.MTH_END_DT
    WHERE p.PREF_ID = 1012
      AND p.MTH_END_DT >= DATE '2024-01-31'
      AND p.CLNT_TYP_CD = 1
    GROUP BY 1, 2
),
writes AS (
    -- ALL writes that month by target value (not just 5002 - Andre 2026-08-20:
    -- yes-writes expose bulk subscribe events, blank-writes expose resolutions)
    SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10
                       THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS mth,
           APP_SYS_CD,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_yes,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_no,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_blank,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5004 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_5004,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP NOT IN (5001, 5002, 5003, 5004)
                         THEN 1 ELSE 0 END) AS BIGINT)                              AS n_writes_to_other
    FROM DDWV01.CPC_RB_PREF w
    -- active check at the month-end of the WRITE's month (RB_CLNT_DLY is the
    -- status source; write-day precision would need daily snapshots verified)
    INNER JOIN act ON act.CLNT_NO = w.CLNT_NO
                  AND act.SNAP_DT = ADD_MONTHS(CAST(w.CHG_TMSTMP AS DATE)
                                               - EXTRACT(DAY FROM w.CHG_TMSTMP) + 1, 1) - 1
    WHERE w.PREF_ID = 1012
      AND w.CHG_TMSTMP >= DATE '2024-01-01'
      AND w.CHG_TMSTMP <  DATE '2026-08-01'  -- file cutoff; also keeps the partial
                                             -- current month from silently dropping
                                             -- on a not-yet-existing month-end snapshot
    GROUP BY 1, 2
)
SELECT COALESCE(s.mth, w.mth)                 AS mth,
       COALESCE(s.APP_SYS_CD, w.APP_SYS_CD)   AS app_sys_cd,
       COALESCE(s.n_5001_yes, 0)              AS n_5001_yes,
       COALESCE(s.n_5002_no, 0)               AS n_5002_no,
       COALESCE(s.n_5003_blank, 0)            AS n_5003_blank,
       COALESCE(s.n_5004_yes_credit_bureau, 0) AS n_5004_yes_credit_bureau,
       COALESCE(s.n_other_value, 0)           AS n_other_value,
       COALESCE(w.n_writes_to_yes, 0)         AS n_writes_to_yes,
       COALESCE(w.n_writes_to_no, 0)          AS n_writes_to_no,
       COALESCE(w.n_writes_to_blank, 0)       AS n_writes_to_blank,
       COALESCE(w.n_writes_to_5004, 0)        AS n_writes_to_5004,
       COALESCE(w.n_writes_to_other, 0)       AS n_writes_to_other
FROM standing s
FULL OUTER JOIN writes w
  ON w.mth = s.mth AND w.APP_SYS_CD = s.APP_SYS_CD
ORDER BY 1, 2;


/* ============================================================================
[Q3] THE WATERFALL - Aug-2024 -> Jul-2026, status checked at BOTH anchors
     (Andre 2026-08-21: the 2026-08-20 version pinned universe to ONE snapshot
     at the END anchor only - a client closed in Aug-24 but reopened by Jul-26
     was silently counted as always-active, and vice versa. u_a/u_b below
     check CLNT_STS at EACH side's own date, matching that side's CPC anchor.)

     Identity:
       start_5001_aug24 + new_5001
         - (seg_inactive + seg_email_cpc + seg_overlap + seg_ac_branch + left_other)
         = end_5001_jul26                                  (the "official" end bar)
       end_contactable = end_5001_jul26 - seg_email_sf_open
         (a Salesforce-unsub client still reads as cons=5001/active in CPC -
          "in the book" but not truly contactable; subtracted as an
          adjustment INSIDE the end bar, not counted as a departure)

     Departure segments (all require start = 5001 AND active_a = 1);
     precedence order below, mutually exclusive by construction so summing
     them independently (rather than a CASE chain) is safe - see comments
     on seg_ac_branch for the one place that needed an explicit NULL rule:
       1. seg_inactive   - left the active book; takes precedence over any
                            consent movement - a consent row that closes
                            mechanically when a client goes inactive must not
                            inflate the unsubscribe segments
       2. seg_email_cpc  - closed to 5002 by the CPC/email backfeed (7020),
                            no Salesforce record
       3. seg_overlap    - closed to 5002 AND has a Salesforce unsub record
                            (any writer)
       4. seg_ac_branch  - closed to 5002 by a writer other than 7020, no
                            Salesforce record
       5. left_other     - still active in Jul-26 but landed on a consent
                            value that is neither 5001 nor 5002 (incl. NULL)

     COVERAGE WARNING: RB_CLNT_DLY coverage at DATE '2024-08-31' is
     UNVERIFIED - run the MIN/MAX probe at the top of this file first. If the
     table doesn't reach back that far, u_a returns zero rows and
     start_5001_aug24 collapses to 0 - that's the loud-failure signal.
============================================================================ */
WITH u_a AS (
    -- Universe at the START anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Aug-24 side (`a` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
u_b AS (
    -- Universe at the END anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Jul-26 side (`b` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
v AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-09-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
j AS (
    -- One row per client appearing in `a` and/or `b`. active_a/active_b are
    -- looked up per-anchor against u_a/u_b (no single inner-joined universe
    -- anymore - a client's book status can differ across the two dates).
    -- writer_b is APP_SYS_CD from `b` - the system that wrote the Jul-26 row.
    SELECT COALESCE(a.CLNT_NO, b.CLNT_NO)                        AS CLNT_NO,
           a.cons_a,
           b.cons_b,
           b.APP_SYS_CD                                          AS writer_b,
           CASE WHEN ua.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_a,
           CASE WHEN ub.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_b,
           CASE WHEN v.CLNT_NO  IS NOT NULL THEN 1 ELSE 0 END    AS vendor_unsub
    FROM a
    FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN u_a ua ON ua.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN u_b ub ON ub.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN v       ON v.CLNT_NO  = COALESCE(a.CLNT_NO, b.CLNT_NO)
)
SELECT
    -- start bar: cons=5001 AND active, at the Aug-24 anchor
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS start_5001_aug24,

    -- additions: wasn't (5001 AND active) at the start, but is at the end -
    -- covers both newly-consented clients and previously-inactive clients
    -- who came back active while already sitting on 5001. Written as an
    -- explicit disjunction (not NOT(...)) so a NULL cons_a (client absent
    -- from `a` entirely) reads as "not in book", not as unknown/excluded.
    CAST(SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001 OR active_a = 0)
                   AND cons_b = 5001 AND active_b = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS new_5001,

    -- 1. left the active book - checked first, takes precedence over consent
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_inactive,

    -- 2. closed by CPC/email backfeed (7020), no Salesforce record
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002
                   AND writer_b = 7020 AND vendor_unsub = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_cpc,

    -- 3. closed AND has a Salesforce unsub record, any writer
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_overlap,

    -- 4. closed by a writer other than 7020, no Salesforce record.
    -- COALESCE(writer_b, -1) so a NULL writer_b lands here (not silently
    -- dropped) - this keeps segs 2/3/4 a full partition of the
    -- active_b=1, cons_b=5002 space regardless of writer nullability.
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002
                   AND COALESCE(writer_b, -1) <> 7020 AND vendor_unsub = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_ac_branch,

    -- 5. still active, but landed on a consent value that's neither
    -- 5001 nor 5002 (blank/other/NULL) - no consent value silently vanishes
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1
                   AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS left_other,

    -- inside end_5001_jul26 (still 5001 AND active on both sides) but carries
    -- a Salesforce unsub record - a contactable adjustment, NOT a departure
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_sf_open,

    -- end bar (official): cons=5001 AND active, at the Jul-26 anchor
    CAST(SUM(CASE WHEN cons_b = 5001 AND active_b = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS end_5001_jul26,

    -- end bar (contactable): official end minus the still-5001/active
    -- clients that carry a Salesforce unsub record
    CAST((SUM(CASE WHEN cons_b = 5001 AND active_b = 1 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                     AND cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                    THEN 1 ELSE 0 END)) AS BIGINT)                           AS end_contactable
FROM j;


/* ============================================================================
[Q4] SF-OPEN BLIND-SPOT SPLIT BY MNE - what did those 420,561 actually unsub from?
     (Andre 2026-08-21. Q3's seg_email_sf_open counts a Salesforce disposition-4
     from ANY list against the 1012 book. A Loyalty-newsletter-only unsub
     legitimately keeps 1012 open - it is NOT a capture failure. This query
     splits the blind-spot clients by the MNE (positions 8-10 of TREATMENT_ID)
     of their FIRST unsub in the window - same first-unsub rule as Q1. Andre
     maps MNE -> LOB in Excel (LOB MANUAL): Loyalty-side MNEs ~ the Rewards
     e-newsletter (1046 list) = legit other-list unsubs; banking-side MNEs =
     the true 1012 capture failure.
     MNE is the campaign family of the SEND the unsub referenced - a proxy for
     the list unsubbed from, not an exact preference id.
     CHECK: SUM(n_clients) must equal Q3's seg_email_sf_open (420,561 on the
     2026-08-21 run) - same population, one row per client.
============================================================================ */
WITH u_a AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'
),
u_b AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
vm AS (
    -- one row per client: MNE of the FIRST disposition-4 in the window
    -- (locked EVENT+MASTER merge, identical filters to Q3's v)
    SELECT CLNT_NO, mne
    FROM (
        SELECT m.CLNT_NO,
               SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
               ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                                  ORDER BY e.disposition_dt_tm ASC,
                                           e.TREATMENT_ID ASC) AS rn
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-09-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ) t
    WHERE rn = 1
)
SELECT vm.mne,
       CAST(COUNT(*) AS BIGINT) AS n_clients
FROM a
INNER JOIN b   ON b.CLNT_NO   = a.CLNT_NO
INNER JOIN u_a ON u_a.CLNT_NO = a.CLNT_NO
INNER JOIN u_b ON u_b.CLNT_NO = a.CLNT_NO
INNER JOIN vm  ON vm.CLNT_NO  = a.CLNT_NO
WHERE a.cons_a = 5001
  AND b.cons_b = 5001
GROUP BY vm.mne
ORDER BY n_clients DESC;


/* ============================================================================
[Q5] DID THE BLIND-SPOT CLIENTS EVER SEE THE PREFERENCE PAGE? (Andre 2026-08-21)
     Population: the same 420,561 as Q4 (5001+active at both anchors, SF unsub
     on record, 1012 never moved). For each client, look for ANY CPC opt-out
     write (CLNT_CONSENT_TYP = 5002, ANY PREF_ID, ANY writer) in the window
     [first unsub date - 1 day, + 14 days].
     Reading:
       * wrote another gate (e.g. 1046) and not 1012 -> they DID reach the page
         and CHOSE a different gate - correctly still 1012-emailable (case 1)
       * no write on any gate -> they never saw the page (one-click/native list
         unsub) - intent unknowable from CPC (the black box)
       * wrote 1012 in-window -> write exists but standing reverted/late - edge
     Split by sending family (Loyalty sends vs all other MNEs) to cross the
     Q4 attribution with actual page behavior. LOYALTY MNE IN-LIST IS EDITABLE -
     extend from mapping Mne.csv when it lands; 'VRE' confirmed Loyalty by
     Andre 2026-08-21, VME/VRG presumed same family.
     CHECK: n_clients sums to Q3's seg_email_sf_open (420,561 on 2026-08-21 run).
============================================================================ */
WITH u_a AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'
),
u_b AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
vm AS (
    -- first SF unsub per client: its date AND its sending campaign MNE
    SELECT CLNT_NO, unsub_dt, mne
    FROM (
        SELECT m.CLNT_NO,
               CAST(e.disposition_dt_tm AS DATE)  AS unsub_dt,
               SUBSTR(e.TREATMENT_ID, 8, 3)       AS mne,
               ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                                  ORDER BY e.disposition_dt_tm ASC,
                                           e.TREATMENT_ID ASC) AS rn
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-09-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ) t
    WHERE rn = 1
),
cohort AS (
    SELECT vm.CLNT_NO, vm.unsub_dt, vm.mne
    FROM a
    INNER JOIN b   ON b.CLNT_NO   = a.CLNT_NO
    INNER JOIN u_a ON u_a.CLNT_NO = a.CLNT_NO
    INNER JOIN u_b ON u_b.CLNT_NO = a.CLNT_NO
    INNER JOIN vm  ON vm.CLNT_NO  = a.CLNT_NO
    WHERE a.cons_a = 5001
      AND b.cons_b = 5001
),
wr AS (
    -- one row per cohort client who has at least one opt-out write (any gate)
    -- within [-1, +14] days of their first unsub; did any of them hit 1012?
    SELECT c.CLNT_NO,
           MAX(CASE WHEN w.PREF_ID = 1012 THEN 1 ELSE 0 END) AS wrote_1012,
           MAX(CASE WHEN w.PREF_ID <> 1012 THEN 1 ELSE 0 END) AS wrote_other
    FROM cohort c
    INNER JOIN DDWV01.CPC_RB_PREF w
      ON  w.CLNT_NO = c.CLNT_NO
      AND w.CLNT_CONSENT_TYP = 5002
      AND CAST(w.CHG_TMSTMP AS DATE) >= c.unsub_dt - 1
      AND CAST(w.CHG_TMSTMP AS DATE) <= c.unsub_dt + 14
    WHERE w.CHG_TMSTMP >= DATE '2024-08-01'   -- scan floor; window starts 2024-09
    GROUP BY c.CLNT_NO
)
SELECT CASE WHEN c.mne IN ('VRE', 'VME', 'VRG')   -- EDIT ME: extend from mapping Mne.csv
            THEN 'Loyalty send' ELSE 'Other send' END          AS send_family,
       CASE WHEN wr.CLNT_NO IS NULL               THEN '1. no write on ANY gate - never saw the page'
            WHEN wr.wrote_1012 = 1                THEN '2. wrote 1012 in-window - late/reverted edge'
            ELSE                                       '3. wrote OTHER gate only - chose a different gate on the page'
       END                                                     AS page_outcome,
       CAST(COUNT(*) AS BIGINT)                                AS n_clients,
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,1)) AS pct
FROM cohort c
LEFT JOIN wr ON wr.CLNT_NO = c.CLNT_NO
GROUP BY 1, 2
ORDER BY 1, 2;


/* ============================================================================
[Q3b] WATERFALL v3 - gray bar without the Aug-24 precondition (2026-08-25)
     Q3 v2's seg_email_sf_open required the client to be 5001 AND active at
     BOTH the Aug-24 AND Jul-26 anchors before a Salesforce unsub counted
     against it. That start-book precondition means a client who ENTERED the
     5001-active book in-window (new joiner, or 5002/5003/NULL -> 5001, or
     inactive -> active) and then unsubbed in Salesforce was counted in
     new_5001 and never subtracted - end_contactable overstated, gray bar
     understated. v3 makes seg_email_sf_open an END-state-only property
     (5001 AND active at Jul-26 AND a Salesforce unsub on record, no
     precondition on the Aug-24 side) and splits it into its two components:
       seg_email_sf_open_startbook - the v2 definition (must reproduce v2's
                                      420,561 on the 2026-08-21 run)
       seg_email_sf_open_entered   - the clients v2 missed (subset of new_5001)
     Q3 v2 stays as shipped, unchanged above; v3 is additive, run separately.
     Same CTEs as Q3 (u_a, u_b, a, b, v, j), verbatim - only the SELECT
     differs (seg_email_sf_open, seg_email_sf_open_startbook,
     seg_email_sf_open_entered, and end_contactable).
============================================================================ */
WITH u_a AS (
    -- Universe at the START anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Aug-24 side (`a` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
u_b AS (
    -- Universe at the END anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Jul-26 side (`b` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
v AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-09-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
j AS (
    -- One row per client appearing in `a` and/or `b`. active_a/active_b are
    -- looked up per-anchor against u_a/u_b (no single inner-joined universe
    -- anymore - a client's book status can differ across the two dates).
    -- writer_b is APP_SYS_CD from `b` - the system that wrote the Jul-26 row.
    SELECT COALESCE(a.CLNT_NO, b.CLNT_NO)                        AS CLNT_NO,
           a.cons_a,
           b.cons_b,
           b.APP_SYS_CD                                          AS writer_b,
           CASE WHEN ua.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_a,
           CASE WHEN ub.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_b,
           CASE WHEN v.CLNT_NO  IS NOT NULL THEN 1 ELSE 0 END    AS vendor_unsub
    FROM a
    FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN u_a ua ON ua.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN u_b ub ON ub.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN v       ON v.CLNT_NO  = COALESCE(a.CLNT_NO, b.CLNT_NO)
)
SELECT
    -- start bar: cons=5001 AND active, at the Aug-24 anchor
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS start_5001_aug24,

    -- additions: wasn't (5001 AND active) at the start, but is at the end -
    -- covers both newly-consented clients and previously-inactive clients
    -- who came back active while already sitting on 5001. Written as an
    -- explicit disjunction (not NOT(...)) so a NULL cons_a (client absent
    -- from `a` entirely) reads as "not in book", not as unknown/excluded.
    CAST(SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001 OR active_a = 0)
                   AND cons_b = 5001 AND active_b = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS new_5001,

    -- 1. left the active book - checked first, takes precedence over consent
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_inactive,

    -- 2. closed by CPC/email backfeed (7020), no Salesforce record
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002
                   AND writer_b = 7020 AND vendor_unsub = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_cpc,

    -- 3. closed AND has a Salesforce unsub record, any writer
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_overlap,

    -- 4. closed by a writer other than 7020, no Salesforce record.
    -- COALESCE(writer_b, -1) so a NULL writer_b lands here (not silently
    -- dropped) - this keeps segs 2/3/4 a full partition of the
    -- active_b=1, cons_b=5002 space regardless of writer nullability.
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1 AND cons_b = 5002
                   AND COALESCE(writer_b, -1) <> 7020 AND vendor_unsub = 0
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_ac_branch,

    -- 5. still active, but landed on a consent value that's neither
    -- 5001 nor 5002 (blank/other/NULL) - no consent value silently vanishes
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND active_b = 1
                   AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS left_other,

    -- inside end_5001_jul26 (5001 AND active at the Jul-26 anchor) but carries
    -- a Salesforce unsub record - a contactable adjustment, NOT a departure.
    -- v3 (2026-08-25): the start-book precondition (cons_a = 5001 AND
    -- active_a = 1) is GONE. v2 required it, so a client who ENTERED the
    -- 5001-active book in-window (new joiner, or 5002/5003/NULL -> 5001, or
    -- inactive -> active) and then unsubbed in Salesforce was counted in
    -- new_5001 and never subtracted: end_contactable overstated, gray bar
    -- understated. The gray bar is a property of the END state only.
    CAST(SUM(CASE WHEN cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_sf_open,

    -- decomposition (a): the v2 definition - start-book clients still 5001
    -- AND active at Jul-26 with a Salesforce unsub. Must reproduce the v2
    -- number (420,561 on the 2026-08-21 run).
    CAST(SUM(CASE WHEN cons_a = 5001 AND active_a = 1
                   AND cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_sf_open_startbook,

    -- decomposition (b): the clients v2 missed - not (5001 AND active) at
    -- Aug-24 (same explicit disjunction as new_5001, NULL-safe), 5001 AND
    -- active at Jul-26, Salesforce unsub. A subset of new_5001.
    -- ASSERT: seg_email_sf_open_startbook + seg_email_sf_open_entered
    --         = seg_email_sf_open (the two conditions partition cons_a/active_a).
    CAST(SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001 OR active_a = 0)
                   AND cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS seg_email_sf_open_entered,

    -- end bar (official): cons=5001 AND active, at the Jul-26 anchor
    CAST(SUM(CASE WHEN cons_b = 5001 AND active_b = 1
                  THEN 1 ELSE 0 END) AS BIGINT)                              AS end_5001_jul26,

    -- end bar (contactable): official end minus ALL 5001/active-at-Jul-26
    -- clients that carry a Salesforce unsub record (= the v3 seg_email_sf_open,
    -- no start-book precondition - same expression, re-derived inline)
    CAST((SUM(CASE WHEN cons_b = 5001 AND active_b = 1 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN cons_b = 5001 AND active_b = 1 AND vendor_unsub = 1
                    THEN 1 ELSE 0 END)) AS BIGINT)                           AS end_contactable
FROM j;


/* ============================================================================
[Q3c] Q1 <-> Q3 BRIDGE - where do the Salesforce unsubbers sit in the waterfall?
     (2026-08-25.) Q1 first_unsub_clients (live rerun, Aug-24..Jul-26) came in
     at ~494K unique clients; Q3 v2 showed gray bar 420,561 + seg_overlap
     8,604 = 429,165. This query takes the SAME client-level CTE Q3 uses
     (cons_a, active_a, cons_b, active_b, vendor_unsub), keeps vendor_unsub = 1
     only, and buckets every client by its two-anchor state so the partition
     is complete (G is the catch-all).
     Reading:
       SUM(clients) over all buckets = Q1-style unique vendor unsubbers that
                                       are visible to the Q3 universe
       A + B                        = Q3b v3 seg_email_sf_open (gray bar)
       A                            = seg_email_sf_open_startbook (v2 gray bar)
       B                            = seg_email_sf_open_entered
       D                            = seg_overlap
       A + D                        = v2 gray bar + seg_overlap (the 429,165)
       C, E, F, G                   = the rest of the Q1 - Q3 gap
     LABEL NOTE (exact, all four map 1:1 to Q3/Q3b columns):
       A = seg_email_sf_open_startbook, B = seg_email_sf_open_entered,
       D = seg_overlap, A + B = seg_email_sf_open (v3, from Q3b).
       C and E are SF-unsub SUBSETS of seg_inactive / left_other (not equal
       to them - those columns also include non-SF-unsub clients).
       G MUST BE 0 - the CASE below is a full A-F partition of the space; a
       non-zero G means a bucket's condition is wrong, investigate before
       trusting the bridge.
     WINDOW WARNING: the bridge is NOT expected to close to Q1's 494K, and
     moving VENDOR_FLOOR alone will not close it. Three structural differences
     between Q1's first_unsub_clients leg and Q3c's `v`:
       1. Q1 scans disposition_dt_tm from DATE '2024-01-01' and keeps each
          client's FIRST-EVER unsub (ROW_NUMBER PARTITION BY CLNT_NO); clients
          whose first unsub fell Jan-Jul 2024 are excluded from Q1's 494K but
          Q3c counts ANY disposition-4 in-window (no first-ever rule).
       2. Q1's MASTER TREATMENT_ID prefix range is '2023274'..'2026212';
          Q3c's is '2024153'..'2026212' - unsubs against Oct-23..May-24 sends
          are in Q1, never in Q3c.
       3. Q1 gates active status at each event's own month-end; Q3c gates at
          the two fixed anchors (Aug-24, Jul-26).
     Read the bridge as BUCKET SUMS reconciling to Q3/Q3b columns (A, A+B, A+D
     are exact), not as a reconciliation to Q1's total. VENDOR_FLOOR in `v` is
     left as a marked literal (Q3/Q3b use 2024-09-01, post-anchor unsubs only).
     Output: <= 7 rows, bucket + clients.
============================================================================ */
WITH u_a AS (
    -- Universe at the START anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Aug-24 side (`a` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2024-08-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
u_b AS (
    -- Universe at the END anchor: CLNT_STS is CHAR, 'A' = active; status
    -- date matches the CPC anchor MTH_END_DT on the Jul-26 side (`b` below).
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31'
      AND CLNT_STS = 'A'   -- Q0 2026-08-21: A=14.86M, I=14.07M, null=463K (CLNT_TYP=1); quoted — CHAR column
),
a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
      AND CLNT_TYP_CD = 1
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
      AND CLNT_TYP_CD = 1
),
v AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-09-01'   -- <<< VENDOR_FLOOR: Q3/Q3b use 2024-09-01; set DATE '2024-08-01' to include Aug-24 (the first month of the Q1 rerun sum); Q1 itself scans from 2024-01-01 with a first-ever rule, so this will still not close to 494K - see WINDOW WARNING.
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
j AS (
    -- One row per client appearing in `a` and/or `b`. active_a/active_b are
    -- looked up per-anchor against u_a/u_b (no single inner-joined universe
    -- anymore - a client's book status can differ across the two dates).
    -- writer_b is APP_SYS_CD from `b` - the system that wrote the Jul-26 row.
    SELECT COALESCE(a.CLNT_NO, b.CLNT_NO)                        AS CLNT_NO,
           a.cons_a,
           b.cons_b,
           b.APP_SYS_CD                                          AS writer_b,
           CASE WHEN ua.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_a,
           CASE WHEN ub.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END    AS active_b,
           CASE WHEN v.CLNT_NO  IS NOT NULL THEN 1 ELSE 0 END    AS vendor_unsub
    FROM a
    FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN u_a ua ON ua.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN u_b ub ON ub.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
    LEFT JOIN v       ON v.CLNT_NO  = COALESCE(a.CLNT_NO, b.CLNT_NO)
)
SELECT
    CASE
        WHEN cons_a = 5001 AND active_a = 1 AND cons_b = 5001 AND active_b = 1
            THEN CAST('A start-book, still 5001 active (gray bar startbook)' AS VARCHAR(80))
        WHEN (cons_a IS NULL OR cons_a <> 5001 OR active_a = 0)
             AND cons_b = 5001 AND active_b = 1
            THEN 'B entered book in-window, 5001 active at Jul-26 (gray bar entered)'
        WHEN cons_a = 5001 AND active_a = 1 AND active_b = 0
            THEN 'C start-book, went inactive by Jul-26 (seg_inactive, SF-unsub subset)'
        WHEN cons_a = 5001 AND active_a = 1 AND active_b = 1 AND cons_b = 5002
            THEN 'D start-book, closed 5002 by Jul-26 (seg_overlap)'
        WHEN cons_a = 5001 AND active_a = 1 AND active_b = 1
             AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
            THEN 'E start-book, other consent at Jul-26 (left_other, SF-unsub subset)'
        WHEN (cons_a IS NULL OR cons_a <> 5001 OR active_a = 0)
             AND (cons_b IS NULL OR cons_b <> 5001 OR active_b = 0)
            THEN 'F never 5001-active at either anchor (invisible to waterfall)'
        -- G is a guard: A-F partition the space, so G should be 0 rows / 0 clients
        ELSE 'G unreachable guard - expect 0'
    END                                                                     AS bucket,
    CAST(COUNT(*) AS BIGINT)                                                AS clients
FROM j
WHERE vendor_unsub = 1
GROUP BY 1
ORDER BY 1;


-- ============================================================================
-- APPENDIX: UCP QUERIES (Spark SQL over ucp4 parquet - UCP's only home;
-- not runnable in Teradata/Starburst. View registration, the single Python line:
--   spark.read.parquet("/prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE=<m>/")
--        .createOrReplaceTempView("ucp_<m>")
-- Everything below is the logic itself.)
-- ============================================================================

/* ----------------------------------------------------------------------------
[A1] UCP MONTHLY FLOW - clients whose CPC_EM_ELIGIBLE flag flipped month over
     month (run per consecutive month-end pair m0, m1 since 2024-01).
---------------------------------------------------------------------------- */
-- WITH m0 AS (SELECT CLNT_NO, CAST(TRIM(CAST(CPC_EM_ELIGIBLE AS STRING)) = '1' AS INT) AS e0
--             FROM ucp_m0),
--      m1 AS (SELECT CLNT_NO, CAST(TRIM(CAST(CPC_EM_ELIGIBLE AS STRING)) = '1' AS INT) AS e1
--             FROM ucp_m1)
-- SELECT SUM(CASE WHEN m0.e0 = 1 AND m1.e1 = 0 THEN 1 ELSE 0 END)          AS lost_consent,
--        SUM(CASE WHEN m0.e0 = 0 AND m1.e1 = 1 THEN 1 ELSE 0 END)          AS opted_in,
--        SUM(CASE WHEN m0.e0 = 1 AND m1.CLNT_NO IS NULL THEN 1 ELSE 0 END) AS attrition
-- FROM m0 FULL OUTER JOIN m1 ON m0.CLNT_NO = m1.CLNT_NO;

/* ----------------------------------------------------------------------------
[A2] POPULATION PROFILE - the Aug-24 start-bar population (CPC 1012 = 5001,
     landed client list from [Q5]'s `a` CTE) x UCP client type x open products.
     Decides whether the waterfall universe gets the personal-active filter.
---------------------------------------------------------------------------- */
-- SELECT CASE WHEN u.CLNT_NO IS NULL THEN 'not in UCP'
--             ELSE CAST(u.CLNT_TYP AS STRING) END                    AS client_type,
--        CASE WHEN u.CLNT_NO IS NULL      THEN 'not in UCP'
--             WHEN u.OPN_PROD_CNT IS NULL THEN 'null products'
--             WHEN u.OPN_PROD_CNT = 0     THEN '0 products'
--             ELSE                             '1+ products' END     AS open_products,
--        COUNT(*) AS n_clients
-- FROM cpc5001_aug24 c
-- LEFT JOIN ucp_2024_08_31 u ON c.CLNT_NO = u.CLNT_NO
-- GROUP BY 1, 2;


/* ============================================================================
[Q6] SF-VS-CPC DISAGREEMENT, FEB-2026 - client-level evidence rows where
     Salesforce says "unsubscribed" (disposition_cd=4) but CPC 1012 still
     reads open (5001) or the client has no 1012 row at all (2026-08-25).
     Personal-active universe, same u_b / CLNT_TYP pattern as Q1's act CTE
     and Q3's u_a/u_b (Andre 2026-08-21 rule - CLNT_TYP=1 on RB_CLNT_DLY AND
     CLNT_TYP_CD=1 on the CPC read). MASTER range widened to Q1's
     '2023274'..'2026212' (not Q3's narrower '2024153' start) - a Feb-2026
     click can reference an older triggered treatment; narrowing to Q3's
     range risks silently dropping real disagreement rows from this evidence
     file. email_addr lives on MASTER only (EVENT has no email column - see
     schemas/vendor_feedback_tables_schema.md); MASTER grain is NOT verified
     1:1 (same doc), so the DISTINCT below can still fan out on email_addr if
     a (consumer_id_hashed, TREATMENT_ID) pair carries >1 email row - check
     row counts before trusting a 1:1 read.

     Q6a = OUTPUT A: one row per (client, SF unsub event) in Feb-2026, with
     the client's latest CPC 1012 position attached. cons_1012_latest is the
     CLNT_CONSENT_TYP at that client's MAX(MTH_END_DT) on
     DDWV01.CPC_RB_PREF_MTHLY (task-specified source for Output A). No row
     cap - full population returned, Andre samples/filters downstream.
     disagreement_type: 'CPC_1012_OPEN' = CPC still shows 5001 despite the SF
     unsub; 'NOT_IN_CPC' = client has no PREF_ID=1012 row on the monthly
     table at all (LEFT JOIN miss).
============================================================================ */
WITH u_feb AS (
    -- personal-active universe at Feb-2026 month-end (Q1's act-CTE pattern,
    -- single month here since the whole population is Feb-2026 dispositions)
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-02-28'
      AND CLNT_STS = 'A'
      AND CLNT_TYP = 1
),
sf_feb AS (
    -- Salesforce disposition-4 (unsubscribed) events in Feb-2026, locked
    -- EVENT+MASTER merge (identical filters to Q1/Q3's v/base), email_addr
    -- carried through from MASTER
    SELECT m.CLNT_NO,
           m.consumer_id_hashed,
           m.email_addr,
           e.TREATMENT_ID,
           SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
           e.disposition_cd,
           e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO, email_addr
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2023274' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    INNER JOIN u_feb ON u_feb.CLNT_NO = m.CLNT_NO
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-02-01'
      AND e.disposition_dt_tm <  DATE '2026-03-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
cpc1012_latest AS (
    -- latest CPC 1012 position per client (max MTH_END_DT), Q3's a/b filter
    -- pattern (PREF_ID=1012, CLNT_TYP_CD=1); ROW_NUMBER + outer WHERE rn=1,
    -- same idiom as Q1/Q4/Q5's rn CTEs (repo convention avoids QUALIFY)
    SELECT CLNT_NO, cons_1012_latest, writer_1012_latest, mth_end_1012_latest
    FROM (
        SELECT CLNT_NO,
               CLNT_CONSENT_TYP AS cons_1012_latest,
               APP_SYS_CD       AS writer_1012_latest,
               MTH_END_DT       AS mth_end_1012_latest,
               ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY MTH_END_DT DESC) AS rn
        FROM DDWV01.CPC_RB_PREF_MTHLY
        WHERE PREF_ID = 1012
          AND CLNT_TYP_CD = 1
    ) t
    WHERE rn = 1
)
SELECT sf.CLNT_NO,
       sf.consumer_id_hashed,
       sf.email_addr,
       sf.TREATMENT_ID,
       sf.mne,
       sf.disposition_cd,
       sf.disposition_dt_tm,
       TRUNC(sf.disposition_dt_tm, 'MM')     AS cohort_month,
       c.cons_1012_latest,
       c.writer_1012_latest,
       c.mth_end_1012_latest,
       CASE WHEN c.cons_1012_latest = 5001 THEN 'CPC_1012_OPEN'
            WHEN c.cons_1012_latest IS NULL THEN 'NOT_IN_CPC'
       END                                   AS disagreement_type
FROM sf_feb sf
LEFT JOIN cpc1012_latest c ON c.CLNT_NO = sf.CLNT_NO
WHERE c.cons_1012_latest = 5001 OR c.cons_1012_latest IS NULL
ORDER BY sf.disposition_dt_tm;


/* ----------------------------------------------------------------------------
[Q6b] OUTPUT B - long format, ALL PREF_ID gates for the SAME disagreement
      clients from Q6a (same u_feb/sf_feb/cpc1012_latest CTEs, repeated
      verbatim - repo convention across Q3/Q3b/Q3c). One row per
      (CLNT_NO, PREF_ID) at each client's LATEST position. No row cap.

      TABLE CHOICE: uses DDWV01.CPC_RB_PREF, not CPC_RB_PREF_MTHLY - Q5
      already reads CPC_RB_PREF for write-level detail (CHG_TMSTMP), and
      the schema doc (schemas/cpc_rb_pref_log_schema.md) names it the
      CURRENT-STATE snapshot: one row per (CLNT_NO, PREF_ID) already at its
      latest value, no need to hunt for a max MTH_END_DT, and it is more
      current than the monthly table between month-ends. Output column is
      latest_chg_tmstmp (from CHG_TMSTMP), not MTH_END_DT - there is no
      MTH_END_DT on this table. consumer_id_hashed is not a CPC column (CPC
      keys on CLNT_NO only per the schema doc) - carried in from the SF
      event side, one pick per client (latest event) via ROW_NUMBER, and
      present on every output row.
---------------------------------------------------------------------------- */
WITH u_feb AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-02-28'
      AND CLNT_STS = 'A'
      AND CLNT_TYP = 1
),
sf_feb AS (
    SELECT m.CLNT_NO,
           m.consumer_id_hashed,
           e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2023274' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    INNER JOIN u_feb ON u_feb.CLNT_NO = m.CLNT_NO
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-02-01'
      AND e.disposition_dt_tm <  DATE '2026-03-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
cpc1012_latest AS (
    SELECT CLNT_NO, cons_1012_latest
    FROM (
        SELECT CLNT_NO,
               CLNT_CONSENT_TYP AS cons_1012_latest,
               ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY MTH_END_DT DESC) AS rn
        FROM DDWV01.CPC_RB_PREF_MTHLY
        WHERE PREF_ID = 1012
          AND CLNT_TYP_CD = 1
    ) t
    WHERE rn = 1
),
disagree_clients AS (
    -- same population as Q6a, deduped to one CLNT_NO row with a
    -- representative consumer_id_hashed (most recent SF event that month)
    SELECT CLNT_NO, consumer_id_hashed
    FROM (
        SELECT sf.CLNT_NO, sf.consumer_id_hashed,
               ROW_NUMBER() OVER (PARTITION BY sf.CLNT_NO
                                  ORDER BY sf.disposition_dt_tm DESC) AS rn
        FROM sf_feb sf
        LEFT JOIN cpc1012_latest c ON c.CLNT_NO = sf.CLNT_NO
        WHERE c.cons_1012_latest = 5001 OR c.cons_1012_latest IS NULL
    ) t
    WHERE rn = 1
),
gate_latest AS (
    -- CURRENT-STATE row per (CLNT_NO, PREF_ID) already latest by
    -- construction; ROW_NUMBER guard kept only in case of duplicate writes
    -- landing at the identical CHG_TMSTMP
    SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, APP_SYS_CD, CHG_TMSTMP
    FROM (
        SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, APP_SYS_CD, CHG_TMSTMP,
               ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID
                                  ORDER BY CHG_TMSTMP DESC) AS rn
        FROM DDWV01.CPC_RB_PREF
    ) t
    WHERE rn = 1
)
SELECT dc.CLNT_NO,
       dc.consumer_id_hashed,
       g.PREF_ID,
       g.CLNT_CONSENT_TYP,
       g.APP_SYS_CD,
       g.CHG_TMSTMP AS latest_chg_tmstmp
FROM disagree_clients dc
INNER JOIN gate_latest g ON g.CLNT_NO = dc.CLNT_NO
ORDER BY dc.CLNT_NO, g.PREF_ID;

/* ---------------------------------------------------------------------------
[Q6c] EVIDENCE FILE, SIMPLE VERSION (2026-08-25, replaces Q6a/Q6b for the share-out)
      Feb-2026 Salesforce unsubs (disposition_cd = 4), personal-active at
      2026-02-28, then EVERY CPC_RB_PREF row for those clients - all gates,
      no 1012 isolation, no disagreement filter. Andre picks the sample.
      One row per (SF unsub event x CPC pref row). Clients with NO CPC row
      at all still appear once, with CPC columns NULL.
      Why lighter than Q6a: MASTER scan narrowed to sends from Nov-2025 on
      (an unsub in Feb-26 rides a send from the prior ~90 days; widen the
      prefix floor if the Feb count looks short). No SAMPLE / TOP.
--------------------------------------------------------------------------- */
WITH u_feb AS (
    SELECT DISTINCT CLNT_NO
    FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-02-28' AND CLNT_STS = 'A' AND CLNT_TYP = 1
),
sf AS (
    -- Salesforce side: the unsub click, with hash id, email, treatment id
    SELECT m.CLNT_NO, m.consumer_id_hashed, m.email_addr, m.TREATMENT_ID,
           SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
           e.disposition_cd, e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO, email_addr
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2025305' AND '2026059'   -- Nov-25 .. Feb-26 sends
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    INNER JOIN u_feb ON u_feb.CLNT_NO = m.CLNT_NO
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-02-01'
      AND e.disposition_dt_tm <  DATE '2026-03-01'
)
SELECT sf.CLNT_NO,
       sf.consumer_id_hashed,
       sf.email_addr,
       sf.TREATMENT_ID,
       sf.mne,
       sf.disposition_cd,
       sf.disposition_dt_tm,
       -- CPC side: every gate the client has, current position + who wrote it + when
       p.PREF_ID,
       p.CLNT_CONSENT_TYP,
       p.APP_SYS_CD,
       p.CHG_TMSTMP,
       CASE WHEN p.CLNT_NO IS NULL THEN 'NOT_IN_CPC' ELSE 'IN_CPC' END AS cpc_presence
FROM sf
LEFT JOIN DDWV01.CPC_RB_PREF p ON p.CLNT_NO = sf.CLNT_NO
ORDER BY sf.CLNT_NO, sf.disposition_dt_tm, p.PREF_ID;
