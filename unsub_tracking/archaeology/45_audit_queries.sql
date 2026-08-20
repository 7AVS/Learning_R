-- ============================================================================
-- 45_audit_queries.sql — standalone audit surface for the contactable-base deck
-- Every query stands on its own (Teradata SQL, run in Teradata Studio / BTEQ).
-- These are THE queries behind the deck outputs - no caching, no Python; runtime
-- may be long, correctness over speed.
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


/* ============================================================================
[Q1] MONTHLY VENDOR ACTIVITY x MNE since 2024-01 - ONE table, sends and unsubs
     side by side (Andre 2026-08-20). clnt_no grain both measures:
       sent_clients  = distinct clients mailed by that MNE that month
       unsub_clients = distinct clients whose FIRST unsub of the month was that
                       MNE (multi-MNE clients count once -> unsub_clients sums
                       to distinct unsubscribers per month)
     One scan of EVENT (disp 1 and 4), one MASTER join (locked merge).
============================================================================ */
WITH base AS (
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
SELECT COALESCE(s.evt_month, u.evt_month)  AS evt_month,
       COALESCE(s.mne, u.mne)              AS mne,
       COALESCE(s.sent_clients, 0)         AS sent_clients,
       COALESCE(u.unsub_clients, 0)        AS unsub_clients
FROM sends s
FULL OUTER JOIN unsubs u
  ON u.evt_month = s.evt_month AND u.mne = s.mne
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
WITH standing AS (
    SELECT TRIM(EXTRACT(YEAR FROM MTH_END_DT)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM MTH_END_DT) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM MTH_END_DT))                AS mth,
           APP_SYS_CD,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 1 ELSE 0 END) AS BIGINT) AS n_5001_yes,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS BIGINT) AS n_5002_no,
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS BIGINT) AS n_5003_blank
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012
      AND MTH_END_DT >= DATE '2024-01-31'
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
           CAST(SUM(CASE WHEN CLNT_CONSENT_TYP = 5003 THEN 1 ELSE 0 END) AS BIGINT) AS n_writes_to_blank
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CHG_TMSTMP >= DATE '2024-01-01'
    GROUP BY 1, 2
)
SELECT COALESCE(s.mth, w.mth)                 AS mth,
       COALESCE(s.APP_SYS_CD, w.APP_SYS_CD)   AS app_sys_cd,
       COALESCE(s.n_5001_yes, 0)              AS n_5001_yes,
       COALESCE(s.n_5002_no, 0)               AS n_5002_no,
       COALESCE(s.n_5003_blank, 0)            AS n_5003_blank,
       COALESCE(w.n_writes_to_yes, 0)         AS n_writes_to_yes,
       COALESCE(w.n_writes_to_no, 0)          AS n_writes_to_no,
       COALESCE(w.n_writes_to_blank, 0)       AS n_writes_to_blank
FROM standing s
FULL OUTER JOIN writes w
  ON w.mth = s.mth AND w.APP_SYS_CD = s.APP_SYS_CD
ORDER BY 1, 2;


/* ============================================================================
[Q3] THE SUBSCRIBERS WATERFALL, Aug-24 -> Jul-26 - eight numbers, one statement.
     Start = pure CPC (1012 = 5001 at the anchor, no filters). The unsub bar's
     four segments = closing writer x Salesforce record. END official = the CPC
     book at Jul-26; END contactable = official minus the E-mail-Salesforce
     segment (clients with a Salesforce unsub the consent book never recorded).
     Identity: start + new - (seg_email_cpc + seg_overlap + seg_ac_branch)
               - left_other = end_official.
============================================================================ */
WITH a AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_a
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2024-08-31'
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD AS writer_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31'
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
    SELECT COALESCE(a.CLNT_NO, b.CLNT_NO) AS clnt_no, a.cons_a, b.cons_b, b.writer_b,
           CASE WHEN v.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS vendor_unsub
    FROM a
    FULL OUTER JOIN b ON a.CLNT_NO = b.CLNT_NO
    LEFT JOIN v ON v.CLNT_NO = COALESCE(a.CLNT_NO, b.CLNT_NO)
)
SELECT CAST(SUM(CASE WHEN cons_a = 5001 THEN 1 ELSE 0 END) AS BIGINT)                       AS start_5001_aug24,
       CAST(SUM(CASE WHEN (cons_a IS NULL OR cons_a <> 5001) AND cons_b = 5001
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS new_5001,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND writer_b = 7020
                      AND vendor_unsub = 0 THEN 1 ELSE 0 END) AS BIGINT)                    AS seg_email_cpc,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND vendor_unsub = 1
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS seg_overlap,
       CAST(SUM(CASE WHEN cons_b = 5001 AND vendor_unsub = 1 THEN 1 ELSE 0 END) AS BIGINT)  AS seg_email_sf_open,
       CAST(SUM(CASE WHEN cons_a = 5001 AND cons_b = 5002 AND writer_b <> 7020
                      AND vendor_unsub = 0 THEN 1 ELSE 0 END) AS BIGINT)                    AS seg_ac_branch,
       CAST(SUM(CASE WHEN cons_a = 5001 AND (cons_b IS NULL OR cons_b NOT IN (5001, 5002))
                     THEN 1 ELSE 0 END) AS BIGINT)                                          AS left_other,
       CAST(SUM(CASE WHEN cons_b = 5001 THEN 1 ELSE 0 END) AS BIGINT)                       AS end_5001_jul26
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
