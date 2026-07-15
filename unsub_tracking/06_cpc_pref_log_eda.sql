-- =============================================================================
-- CPC x Unsub — TWO decision questions, two screenshot-sized outputs
-- =============================================================================
-- DDWV01.CPC_RB_PREF_LOG: change log, client x PREF_ID x CHG_TMSTMP.
-- Full schema + PREF_ID catalog + consent semantics: schemas/cpc_rb_pref_log_schema.md
-- (91.4M rows; consent 5001=Yes 5002=No 5003=blank; blank=YES except 1014/1015=NO).
--
-- Output contract: each statement returns <=10 rows. Nothing here is an extract.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- D1: WHICH preference code does an email unsub flip?  (<=10 rows)
-- ---------------------------------------------------------------------------
-- Decision: the PREF_ID(s) at the top = the code(s) email unsubs write; that
-- code becomes the anchor for "population lost" and the CPC-side unsub metric.
-- (No Banking-E-Mail code exists in the catalog — this is the empirical answer.)
-- ---------------------------------------------------------------------------

WITH unsub_clients AS (
    SELECT
        m.CLNT_NO,
        MIN(e.disposition_dt_tm) AS first_unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1
)
SELECT TOP 10
    c.PREF_ID,
    c.CLNT_CONSENT_TYP,
    COUNT(DISTINCT c.CLNT_NO)  AS distinct_clients
FROM unsub_clients u
INNER JOIN DDWV01.CPC_RB_PREF_LOG c
    ON  c.CLNT_NO = u.CLNT_NO
    AND c.CHG_TMSTMP >= u.first_unsub_tm
    AND c.CHG_TMSTMP <  u.first_unsub_tm + INTERVAL '7' DAY
GROUP BY 1, 2
ORDER BY distinct_clients DESC;


-- ---------------------------------------------------------------------------
-- D2: what share of unsub clients show ANY CPC change within 7 days?  (1 row)
-- ---------------------------------------------------------------------------
-- Decision: the linkage rate. High -> CPC validates our unsub counts (the
-- "double counting" objection dies: distinct clients, first unsub, confirmed
-- against the source of truth). Low -> unsubs mostly don't reach CPC and the
-- two systems measure different things — population-lost must then be CPC-only.
-- ---------------------------------------------------------------------------

WITH unsub_clients AS (
    SELECT
        m.CLNT_NO,
        MIN(e.disposition_dt_tm) AS first_unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1
),
linked AS (
    SELECT u.CLNT_NO
    FROM unsub_clients u
    WHERE EXISTS (
        SELECT 1
        FROM DDWV01.CPC_RB_PREF_LOG c
        WHERE c.CLNT_NO = u.CLNT_NO
          AND c.CHG_TMSTMP >= u.first_unsub_tm
          AND c.CHG_TMSTMP <  u.first_unsub_tm + INTERVAL '7' DAY
    )
)
SELECT
    t.unsub_clients_total,
    l.unsub_clients_with_cpc_change_7d
FROM (SELECT CAST(COUNT(*) AS BIGINT) AS unsub_clients_total FROM unsub_clients) t
CROSS JOIN (SELECT CAST(COUNT(*) AS BIGINT) AS unsub_clients_with_cpc_change_7d FROM linked) l;
