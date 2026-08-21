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
    SELECT evt_month, CLNT_NO, mne,
           ROW_NUMBER() OVER (PARTITION BY CLNT_NO, evt_month
                              ORDER BY dt ASC, mne ASC, TREATMENT_ID ASC) AS rn
    FROM base
    WHERE disposition_cd = 4
),
unsubs AS (
    SELECT evt_month, mne, CAST(COUNT(*) AS BIGINT) AS unsub_clients
    FROM unsub_ranked
    WHERE rn = 1
    GROUP BY 1, 2
)
SELECT COALESCE(s.evt_month, un.evt_month) AS evt_month,
       COALESCE(s.mne, un.mne)             AS mne,
       COALESCE(s.sent_clients, 0)         AS sent_clients,
       COALESCE(un.unsub_clients, 0)       AS unsub_clients
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
