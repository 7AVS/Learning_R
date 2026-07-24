-- volatile tables vt_unsub_base + vt_unsub_first + vt_q2_sends persist per session; DROP all three at end
-- rerun after failure: run all three DROPs first
-- run ONE statement at a time
-- Universe: NBA campaign email (SFMC vendor feed); CPC opt-out = explicit No on switches 1002 (entity DNS), 1012 (banking email), 1014 (marketing sharing) - the 3 email-relevant of ~40 codes.

-- SETUP pass 1/2: unsub base - trailing-12-month resolved unsub events, MASTER load_tm chunk 1 (2025-06-01 to 2026-01-01)
CREATE VOLATILE TABLE vt_unsub_base AS (
    SELECT DISTINCT
        m.CLNT_NO,
        e.disposition_dt_tm AS unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-07-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2025-06-01'
      AND m.load_tm           <  DATE '2026-01-01'
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

-- SETUP pass 2/2: same table, MASTER load_tm chunk 2 (2026-01-01 to 2026-08-01)
INSERT INTO vt_unsub_base
    SELECT DISTINCT
        m.CLNT_NO,
        e.disposition_dt_tm AS unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-07-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2026-01-01'
      AND m.load_tm           <  DATE '2026-08-01';

COLLECT STATISTICS ON vt_unsub_base COLUMN (CLNT_NO);


-- SETUP dedup: first unsub per client - runs AFTER both passes are loaded, derived from vt_unsub_base (no further MASTER access)
CREATE VOLATILE TABLE vt_unsub_first AS (
    WITH ranked AS (
        SELECT
            CLNT_NO,
            unsub_tm,
            ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY unsub_tm ASC) AS rn
        FROM vt_unsub_base
    )
    SELECT CLNT_NO, unsub_tm
    FROM ranked
    WHERE rn = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_first COLUMN (CLNT_NO);


-- SETUP 3/3: Q2 send resolution - EVENT disp=1 Apr-Jun 2026 joined to MASTER load_tm Mar-Aug 2026, built once so Evidence 4 doesn't re-evaluate the join per reference
CREATE VOLATILE TABLE vt_q2_sends AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2026-04-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2026-03-01'
      AND m.load_tm           <  DATE '2026-08-01'
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_q2_sends COLUMN (CLNT_NO);


-- EVIDENCE 1: two consent worlds, monthly volumes (email unsubs outnumber CPC opt-outs ~35x)
-- window: Jul 2025 - Jun 2026 for both series
SELECT
    CAST('email_unsub' AS VARCHAR(30)) AS consent_world,
    EXTRACT(YEAR FROM unsub_tm) * 100 + EXTRACT(MONTH FROM unsub_tm) AS month_yyyymm,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS clients
FROM vt_unsub_first
GROUP BY 1, 2
UNION ALL
SELECT
    'cpc_optout',
    EXTRACT(YEAR FROM CHG_TMSTMP) * 100 + EXTRACT(MONTH FROM CHG_TMSTMP),
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014)
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-07-01'
  AND CHG_TMSTMP <  DATE '2026-07-01'
GROUP BY 1, 2
ORDER BY 1, 2;


-- EVIDENCE 2: the blind gate (~99.6% of unsubscribers have no explicit CPC opt-out)
-- unsubs: Jul 2025 - Jun 2026; consent standing: latest answer ever (any time, before or after the unsub - deliberately generous to CPC); before/after split vs each client's first unsub
WITH cpc_latest AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        CHG_TMSTMP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
    -- consent standing = latest answer ever recorded (full history; small table - deliberate exception to the 2024 scan floor)
),
cpc_optout_detail AS (
    SELECT CLNT_NO, PREF_ID, CHG_TMSTMP
    FROM cpc_latest
    WHERE rn = 1 AND CLNT_CONSENT_TYP = 5002
),
cpc_optout AS (
    SELECT DISTINCT CLNT_NO
    FROM cpc_optout_detail
),
-- multi-switch clients: a client can hold a qualifying (latest-state) opt-out on more than one switch;
-- count them ONCE using the EARLIEST qualifying CHG_TMSTMP for the before/after test below
cpc_optout_earliest AS (
    SELECT CLNT_NO, MIN(CHG_TMSTMP) AS optout_chg_tmstmp
    FROM cpc_optout_detail
    GROUP BY CLNT_NO
),
flagged AS (
    SELECT
        u.CLNT_NO,
        CASE WHEN co.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS has_cpc_optout,
        CASE WHEN ce.optout_chg_tmstmp IS NOT NULL AND ce.optout_chg_tmstmp <  u.unsub_tm THEN 1 ELSE 0 END AS optout_before_unsub,
        CASE WHEN ce.optout_chg_tmstmp IS NOT NULL AND ce.optout_chg_tmstmp >= u.unsub_tm THEN 1 ELSE 0 END AS optout_after_unsub
    FROM vt_unsub_first u
    LEFT JOIN cpc_optout co ON co.CLNT_NO = u.CLNT_NO
    LEFT JOIN cpc_optout_earliest ce ON ce.CLNT_NO = u.CLNT_NO
)
SELECT CAST('unsub_clients_total' AS VARCHAR(30)) AS metric, CAST(COUNT(*) AS BIGINT) AS clients FROM flagged
UNION ALL
SELECT 'with_explicit_cpc_optout', CAST(SUM(has_cpc_optout) AS BIGINT) FROM flagged
UNION ALL
SELECT 'without_explicit_cpc_optout', CAST(SUM(1 - has_cpc_optout) AS BIGINT) FROM flagged
UNION ALL
SELECT 'optout_recorded_before_unsub', CAST(SUM(optout_before_unsub) AS BIGINT) FROM flagged
UNION ALL
SELECT 'optout_recorded_after_unsub', CAST(SUM(optout_after_unsub) AS BIGINT) FROM flagged
ORDER BY 1;


-- EVIDENCE 3: no bridge (CPC opt-outs are not triggered by email unsubs; only trace = SFMC on 1012, ~15/yr)
-- window: CPC flips since Jul 2025, matched against any prior unsub (no cap on how far back the unsub can be)
WITH cpc_flips AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        APP_SYS_CD,
        CHG_TMSTMP
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-07-01'
),
nearest_prior AS (
    SELECT
        f.PREF_ID,
        f.APP_SYS_CD,
        u.CLNT_NO AS matched_unsub,
        ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO, f.PREF_ID, f.CHG_TMSTMP ORDER BY u.unsub_tm DESC) AS rn
    FROM cpc_flips f
    LEFT JOIN vt_unsub_first u
        ON  u.CLNT_NO  = f.CLNT_NO
        AND u.unsub_tm <  f.CHG_TMSTMP
)
SELECT
    PREF_ID,
    APP_SYS_CD,
    CASE WHEN matched_unsub IS NOT NULL THEN 'Y' ELSE 'N' END AS had_prior_unsub,
    CAST(COUNT(*) AS BIGINT) AS flips
FROM nearest_prior
WHERE rn = 1
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC;


-- EVIDENCE 4: the leaking gate (clients with explicit email opt-outs still receive campaign email)
-- window: opted out before Apr 1 2026 -> did they get campaign email Apr-Jun 2026?
WITH cpc_gate AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
      AND CHG_TMSTMP < DATE '2026-04-01'
    -- standing as of Apr 1, 2026 = latest answer ever recorded before that date
),
gate_flags AS (
    SELECT
        CLNT_NO,
        MAX(CASE WHEN PREF_ID = 1002 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1002,
        MAX(CASE WHEN PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1012,
        MAX(CASE WHEN PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1014
    FROM cpc_gate
    WHERE rn = 1
    GROUP BY 1
),
gate_long AS (
    SELECT CLNT_NO, 1002 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM gate_flags WHERE out_1002 = 1
    UNION ALL
    SELECT CLNT_NO, 1012 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM gate_flags WHERE out_1012 = 1
    UNION ALL
    SELECT CLNT_NO, 1014 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM gate_flags WHERE out_1014 = 1
)
SELECT
    CAST(PREF_ID AS VARCHAR(30)) AS pref_id,
    CASE WHEN flag_count = 1 THEN 'only_this_flag' ELSE 'multi_flag' END AS exclusivity,
    CAST(COUNT(*) AS BIGINT) AS optout_clients,
    CAST(SUM(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS got_email_apr_jun
FROM gate_long g
LEFT JOIN vt_q2_sends s ON s.CLNT_NO = g.CLNT_NO
GROUP BY 1, 2
UNION ALL
SELECT
    'ALL_SWITCHES',
    'any_flag',
    CAST(COUNT(*) AS BIGINT),
    CAST(SUM(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
FROM (SELECT DISTINCT CLNT_NO FROM gate_long) g
LEFT JOIN vt_q2_sends s ON s.CLNT_NO = g.CLNT_NO
ORDER BY 1, 2;


DROP TABLE vt_q2_sends;
DROP TABLE vt_unsub_first;
DROP TABLE vt_unsub_base;
