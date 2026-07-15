-- =============================================================================
-- Reachability loss — overlap of the four exit mechanisms (one cross-tab)
-- =============================================================================
-- Question: do email unsubs and CPC opt-outs overlap, and by how much?
-- Output: <=16 rows — every combination of the four flags, client counts.
--   is_unsub : vendor email unsub since 2024-01-01 (first-unsub population)
--   out_1002 : latest CPC state = 5002 for 1002 (entity do-not-solicit)
--   out_1012 : latest CPC state = 5002 for 1012 (channel; Mobile vs E-Mail
--              label unresolved — see schemas/cpc_rb_pref_log_schema.md)
--   out_1014 : latest CPC state = 5002 for 1014 (share-for-marketing;
--              NOTE blank/absent also behaves as NO for 1014 — this flag counts
--              EXPLICIT No only)
-- Note: unsub flag is 2024+ events; CPC flags are current stock (all-time
-- latest state) — a state-vs-flow mix, fine for overlap reading.
-- Total lost to ANY mechanism = sum of all rows except the all-zeros row.
-- ENGINE: Teradata-direct.
-- =============================================================================

WITH unsub AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
),
cpc_latest AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID
                           ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
),
cpc_flags AS (
    SELECT
        CLNT_NO,
        MAX(CASE WHEN PREF_ID = 1002 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1002,
        MAX(CASE WHEN PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1012,
        MAX(CASE WHEN PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1014
    FROM cpc_latest
    WHERE rn = 1
    GROUP BY 1
),
combined AS (
    SELECT
        COALESCE(u.CLNT_NO, c.CLNT_NO)  AS CLNT_NO,
        CASE WHEN u.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS is_unsub,
        COALESCE(c.out_1002, 0)         AS out_1002,
        COALESCE(c.out_1012, 0)         AS out_1012,
        COALESCE(c.out_1014, 0)         AS out_1014
    FROM unsub u
    FULL OUTER JOIN cpc_flags c
        ON c.CLNT_NO = u.CLNT_NO
)
SELECT
    is_unsub,
    out_1002,
    out_1012,
    out_1014,
    CAST(COUNT(*) AS BIGINT)            AS clients
FROM combined
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC;
