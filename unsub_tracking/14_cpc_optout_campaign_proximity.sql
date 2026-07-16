-- =============================================================================
-- CPC opt-out <- campaign proximity (backward attribution, with base rate)
-- =============================================================================
-- Question: do campaign emails PRECEDE client-initiated formal opt-outs?
-- Mirror of D2 (which looked unsub -> CPC forward and found nothing): here we
-- start from CPC opt-outs and look BACK for an email-decisioned touch.
-- DESCRIPTIVE attribution ("preceded by", not "caused by") — and it only means
-- anything vs the base rate (P3): we email millions monthly, so raw proximity
-- overstates. The attributable share = P1's rate MINUS the base rate.
-- Population: client-initiated opt-outs only (human channels; machine writers
-- excluded), PREF_ID 1002/1014, 2024+ ex-HSBC.
-- Outputs: P1 = 1 row, P2 <= 15 rows, P3 = 1 row.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- P1: opt-outs with an EM-decisioned touch within 30 days before (1 row)
WITH optouts AS (
    SELECT CLNT_NO, CHG_TMSTMP
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1014)
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2024-01-01'
      AND NOT (CHG_TMSTMP >= DATE '2024-03-01' AND CHG_TMSTMP < DATE '2024-04-01')
      AND APP_SYS_CD IN (7001, 7003, 7004, 7016)   -- client-initiated channels
),
touched AS (
    SELECT o.CLNT_NO, o.CHG_TMSTMP
    FROM optouts o
    WHERE EXISTS (
        SELECT 1
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
        WHERE t.CLNT_NO = o.CLNT_NO
          AND t.TREATMT_STRT_DT >= CAST(o.CHG_TMSTMP AS DATE) - 30
          AND t.TREATMT_STRT_DT <  CAST(o.CHG_TMSTMP AS DATE)
          AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
               OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    )
)
SELECT
    o.optout_events,
    o.distinct_clients,
    t.touched_events
FROM (SELECT CAST(COUNT(*) AS BIGINT) AS optout_events,
             COUNT(DISTINCT CLNT_NO)  AS distinct_clients
      FROM optouts) o
CROSS JOIN
     (SELECT CAST(COUNT(*) AS BIGINT) AS touched_events
      FROM touched) t;


-- P2: touched opt-outs — which MNE was the most recent EM touch (<= 15 rows)
WITH optouts AS (
    SELECT CLNT_NO, CHG_TMSTMP
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1014)
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2024-01-01'
      AND NOT (CHG_TMSTMP >= DATE '2024-03-01' AND CHG_TMSTMP < DATE '2024-04-01')
      AND APP_SYS_CD IN (7001, 7003, 7004, 7016)
),
last_touch AS (
    SELECT
        o.CLNT_NO,
        o.CHG_TMSTMP,
        SUBSTR(t.TACTIC_ID, 8, 3) AS touch_mne,
        ROW_NUMBER() OVER (PARTITION BY o.CLNT_NO, o.CHG_TMSTMP
                           ORDER BY t.TREATMT_STRT_DT DESC) AS rn
    FROM optouts o
    INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON  t.CLNT_NO = o.CLNT_NO
        AND t.TREATMT_STRT_DT >= CAST(o.CHG_TMSTMP AS DATE) - 30
        AND t.TREATMT_STRT_DT <  CAST(o.CHG_TMSTMP AS DATE)
    WHERE (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
)
SELECT TOP 15
    touch_mne,
    CAST(COUNT(*) AS BIGINT)  AS optouts_last_touched_by
FROM last_touch
WHERE rn = 1
GROUP BY 1
ORDER BY optouts_last_touched_by DESC;


-- P3: BASE RATE — share of clients EM-decisioned in a typical 30-day window
-- (1 row; compare P1's touched share against this before claiming anything)
SELECT
    COUNT(DISTINCT t.CLNT_NO) AS clients_em_decisioned_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.TREATMT_STRT_DT >= DATE '2026-06-01'
  AND t.TREATMT_STRT_DT <  DATE '2026-07-01'
  AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
       OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' );
