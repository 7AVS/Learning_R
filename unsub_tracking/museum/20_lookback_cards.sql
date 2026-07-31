-- 20: 12-MONTH LOOKBACK — cards-first, regulatory-excluded, with same-month OVERLAP
-- ENGINE: Teradata-direct. Adapted from archaeology/19_unsub_journey_lookback.sql v7 (never run).
-- Always run top to bottom. Pre-clean DROPs below: 'does not exist' on a fresh session is harmless.
--
-- WHAT CHANGED vs pack 19 v7, and why:
--   1. WINDOW aligned to the museum (2026-03-01..2026-06-01). v7 had Apr..Jul; July 2026 is
--      incomplete and the mismatch meant nothing here could reconcile to L7.
--   2. already_out EXCLUDED from the stayer spine (VT2b). v7 removed only in-window unsubs, so
--      clients who had already opted out before the window sat in the denominator. They were never
--      at risk. Leaving them in biases the unsub rate DOWN.
--   3. REGULATORY excluded from every measured count (22 mnemonics, ACTION_TYPE='Regulatory',
--      canon in RUN_2026-07-30_REGULATORY.md). Kept as its OWN column, not silently dropped —
--      lookback_contacts_reg is how we prove the exclusion mattered.
--   4. CARDS columns added ALONGSIDE bank-wide, on the same row. The query is NOT filtered to cards.
--   5. OVERLAP added for brief 3c: distinct campaigns landing in the SAME month, max across the
--      12 months. lookback_mnes is breadth over a year; that is not the same question.
--   6. Restructured to ONE pass over the 12mo event history (VT5), then cheap rollups. v7 aborted
--      on CPU twice. Also: ZERO COUNT(DISTINCT) in any GROUP BY over sends — that is what hung [5b].
--
-- WEIGHTING (do not forget): stayers are 1-in-10 sampled (MOD 10). Leavers are a census.
--   unsub rate = leavers / (leavers + 10 * stayers).  Never weight the leaver side.

DROP TABLE vt_lookback_client;
DROP TABLE vt_lookback_grain;
DROP TABLE vt_unsub_journey_pop;
DROP TABLE vt_baseline_spine;
DROP TABLE vt_prior_unsub;
DROP TABLE vt_unsub_cohort;
DROP TABLE vt_unsub_events;
DROP TABLE vt_params;

-- editable: THE ONLY PLACE to set the spotlight window. Matches unsub_value_museum.py WIN_START/WIN_END.
CREATE VOLATILE TABLE vt_params AS (
    SELECT DATE '2026-03-01' AS window_start,
           DATE '2026-06-01' AS window_end,
           DATE '2024-01-01' AS hist_floor      -- hard rule: no scan reaches below 2024-01-01
) WITH DATA PRIMARY INDEX (window_start) ON COMMIT PRESERVE ROWS;


-- VT1: unsub events, window-bounded (spotlight only)
CREATE VOLATILE TABLE vt_unsub_events AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    CROSS JOIN vt_params vp
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= vp.window_start
      AND e.disposition_dt_tm <  vp.window_end
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_events COLUMN (consumer_id_hashed, TREATMENT_ID);


-- VT2: leaver cohort — resolve CLNT_NO, index_dt = earliest unsub in window, all addresses kept
CREATE VOLATILE TABLE vt_unsub_cohort AS (
    WITH resolved AS (
        SELECT
            m.CLNT_NO,
            u.consumer_id_hashed,
            u.disposition_dt_tm,
            ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY u.disposition_dt_tm ASC) AS rn
        FROM vt_unsub_events u
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
            ON  m.consumer_id_hashed = u.consumer_id_hashed
            AND m.TREATMENT_ID       = u.TREATMENT_ID
        CROSS JOIN vt_params vp
        WHERE m.load_tm >= ADD_MONTHS(vp.window_start, -1)
          AND m.load_tm <  ADD_MONTHS(vp.window_end,  1)
    ),
    client_index AS (
        SELECT CLNT_NO, disposition_dt_tm AS index_dt
        FROM resolved
        WHERE rn = 1
    )
    SELECT DISTINCT r.CLNT_NO, r.consumer_id_hashed, ci.index_dt
    FROM resolved r
    INNER JOIN client_index ci ON ci.CLNT_NO = r.CLNT_NO
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_cohort COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_unsub_cohort COLUMN (consumer_id_hashed);


-- VT2b: ALREADY-OUT — opted out BEFORE the window opens. Museum's third bucket.
-- These clients get mailed anyway (the suppression leak), but they were never at risk of leaving
-- during the window. Museum excludes them from stayers; v7 did not. This is the fix.
CREATE VOLATILE TABLE vt_prior_unsub AS (
    SELECT DISTINCT e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    CROSS JOIN vt_params vp
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= vp.hist_floor
      AND e.disposition_dt_tm <  vp.window_start
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_prior_unsub COLUMN (consumer_id_hashed);


-- VT3: stayer spine — mailed in window, per-MONTH cohorts, 1-in-10 sliced,
--      excluding both in-window leavers (VT2) and already-out (VT2b)
CREATE VOLATILE TABLE vt_baseline_spine AS (
    WITH baseline_sends AS (
        SELECT
            m.CLNT_NO,
            e.consumer_id_hashed,
            e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
            ON  m.consumer_id_hashed = e.consumer_id_hashed
            AND m.TREATMENT_ID       = e.TREATMENT_ID
        CROSS JOIN vt_params vp
        WHERE e.disposition_cd = 1
          AND e.disposition_dt_tm >= vp.window_start
          AND e.disposition_dt_tm <  vp.window_end
          AND m.load_tm           >= ADD_MONTHS(vp.window_start, -1)
          AND m.load_tm           <  ADD_MONTHS(vp.window_end,  1)
          AND MOD(m.CLNT_NO, 10) = 0                    -- editable: slice modulus. WEIGHT = 10.
    ),
    client_month_index AS (   -- one row per CLNT_NO per month mailed: index_dt = LAST send that month
        SELECT
            CLNT_NO,
            EXTRACT(YEAR FROM disposition_dt_tm) * 100 + EXTRACT(MONTH FROM disposition_dt_tm) AS send_month,
            MAX(disposition_dt_tm) AS index_dt
        FROM baseline_sends
        GROUP BY 1, 2
    )
    SELECT DISTINCT b.CLNT_NO, b.consumer_id_hashed, ci.index_dt
    FROM baseline_sends b
    INNER JOIN client_month_index ci ON ci.CLNT_NO = b.CLNT_NO
    WHERE NOT EXISTS (SELECT 1 FROM vt_unsub_cohort u WHERE u.CLNT_NO = b.CLNT_NO)
      AND NOT EXISTS (SELECT 1 FROM vt_prior_unsub  q WHERE q.consumer_id_hashed = b.consumer_id_hashed)
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_baseline_spine COLUMN (CLNT_NO);


-- VT4: population spine — leavers UNION ALL stayers
CREATE VOLATILE TABLE vt_unsub_journey_pop AS (
    SELECT
        consumer_id_hashed, CLNT_NO,
        CAST('unsub' AS VARCHAR(10)) AS cohort_group,
        EXTRACT(YEAR FROM index_dt) * 100 + EXTRACT(MONTH FROM index_dt) AS cohort_month,
        index_dt
    FROM vt_unsub_cohort
    UNION ALL
    SELECT
        consumer_id_hashed, CLNT_NO,
        CAST('stayed' AS VARCHAR(10)) AS cohort_group,
        EXTRACT(YEAR FROM index_dt) * 100 + EXTRACT(MONTH FROM index_dt) AS cohort_month,
        index_dt
    FROM vt_baseline_spine
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_journey_pop COLUMN (consumer_id_hashed);
COLLECT STATISTICS ON vt_unsub_journey_pop COLUMN (CLNT_NO);


-- VT5: THE ONE PASS over 12 months of event history.
-- Grain: client x cohort_month x send_month x mne x disposition_cd. Flags computed once, here.
-- Every downstream number comes off this table. Nothing else reads VENDOR_FEEDBACK_EVENT again.
CREATE VOLATILE TABLE vt_lookback_grain AS (
    SELECT
        p.CLNT_NO,
        p.cohort_group,
        p.cohort_month,
        EXTRACT(YEAR FROM s.disposition_dt_tm) * 100 + EXTRACT(MONTH FROM s.disposition_dt_tm) AS send_month,
        SUBSTR(s.TREATMENT_ID, 8, 3) AS mne,
        s.disposition_cd,
        -- regulatory: ACTION_TYPE='Regulatory', 22 mnemonics, RUN_2026-07-30_REGULATORY.md
        CASE WHEN SUBSTR(s.TREATMENT_ID, 8, 3) IN (
                 'AFD','BPU','BUK','CFR','EOE','FNE','FSA','FSO','FXR','GAF','HFC',
                 'HPN','IOO','NST','OTC','PUK','ROP','TWI','VMF','VOA','ZDC','ZHX')
             THEN 1 ELSE 0 END AS is_reg,
        -- cards: 12 mnemonics, given by Andre 2026-07-31. MVP is NOT cards (Borealis orchestration).
        CASE WHEN SUBSTR(s.TREATMENT_ID, 8, 3) IN (
                 'AUH','CEC','CLI','CRO','CRV','MET','PCD','PCL','PCQ','VBA','VBU','VIF')
             THEN 1 ELSE 0 END AS is_cards,
        COUNT(DISTINCT s.TREATMENT_ID) AS n_treatments   -- dedupes a client's multiple addresses
    FROM vt_unsub_journey_pop p
    CROSS JOIN vt_params vp
    INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT s
        ON  s.consumer_id_hashed = p.consumer_id_hashed
        AND s.disposition_cd IN (1, 2, 3)                              -- 1=sent 2=opened 3=clicked
        AND s.disposition_dt_tm >= ADD_MONTHS(CAST(p.index_dt AS DATE), -12)
        AND s.disposition_dt_tm <  p.index_dt
    WHERE s.disposition_dt_tm >= ADD_MONTHS(vp.window_start, -12)      -- static bound, helps pruning
      AND s.disposition_dt_tm <  vp.window_end
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_lookback_grain COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_lookback_grain COLUMN (CLNT_NO, cohort_group, cohort_month);


-- VT6: per-client rollup. 12mo totals + same-month overlap, marketing / regulatory / cards.
-- LEFT JOIN back to the population so zero-contact clients survive into the contacts_0 band.
CREATE VOLATILE TABLE vt_lookback_client AS (
    WITH per_mne AS (          -- one row per client x cohort_month x mne  (no COUNT(DISTINCT) needed later)
        SELECT
            CLNT_NO, cohort_group, cohort_month, mne, is_reg, is_cards,
            SUM(CASE WHEN disposition_cd = 1 THEN n_treatments ELSE 0 END) AS sends,
            SUM(CASE WHEN disposition_cd = 2 THEN n_treatments ELSE 0 END) AS opens,
            SUM(CASE WHEN disposition_cd = 3 THEN n_treatments ELSE 0 END) AS clicks
        FROM vt_lookback_grain
        GROUP BY 1, 2, 3, 4, 5, 6
    ),
    totals AS (
        SELECT
            CLNT_NO, cohort_group, cohort_month,
            SUM(CASE WHEN is_reg = 0 THEN sends  ELSE 0 END)                  AS lookback_contacts,
            SUM(CASE WHEN is_reg = 0 AND sends > 0 THEN 1 ELSE 0 END)         AS lookback_mnes,
            SUM(CASE WHEN is_reg = 0 THEN opens  ELSE 0 END)                  AS lookback_opens,
            SUM(CASE WHEN is_reg = 0 THEN clicks ELSE 0 END)                  AS lookback_clicks,
            SUM(CASE WHEN is_reg = 1 THEN sends  ELSE 0 END)                  AS lookback_contacts_reg,
            SUM(CASE WHEN is_cards = 1 THEN sends  ELSE 0 END)                AS lookback_contacts_cards,
            SUM(CASE WHEN is_cards = 1 AND sends > 0 THEN 1 ELSE 0 END)       AS lookback_mnes_cards,
            SUM(CASE WHEN is_cards = 1 THEN opens  ELSE 0 END)                AS lookback_opens_cards,
            SUM(CASE WHEN is_cards = 1 THEN clicks ELSE 0 END)                AS lookback_clicks_cards
        FROM per_mne
        GROUP BY 1, 2, 3
    ),
    per_month AS (             -- distinct campaigns landing in the SAME month = the 3c question
        SELECT
            CLNT_NO, cohort_group, cohort_month, send_month,
            SUM(CASE WHEN disposition_cd = 1 AND is_reg   = 0 THEN 1 ELSE 0 END) AS mnes_in_month,
            SUM(CASE WHEN disposition_cd = 1 AND is_cards = 1 THEN 1 ELSE 0 END) AS cards_mnes_in_month
        FROM vt_lookback_grain
        GROUP BY 1, 2, 3, 4
    ),
    ov AS (
        SELECT
            CLNT_NO, cohort_group, cohort_month,
            MAX(mnes_in_month)                          AS max_mnes_month,
            CAST(AVG(mnes_in_month) AS DECIMAL(10,1))   AS avg_mnes_month,
            MAX(cards_mnes_in_month)                    AS max_cards_mnes_month
        FROM per_month
        WHERE mnes_in_month > 0 OR cards_mnes_in_month > 0    -- months with no send do not dilute the mean
        GROUP BY 1, 2, 3
    )
    SELECT
        p.CLNT_NO, p.cohort_group, p.cohort_month,
        COALESCE(t.lookback_contacts,        0) AS lookback_contacts,
        COALESCE(t.lookback_mnes,            0) AS lookback_mnes,
        COALESCE(t.lookback_opens,           0) AS lookback_opens,
        COALESCE(t.lookback_clicks,          0) AS lookback_clicks,
        COALESCE(t.lookback_contacts_reg,    0) AS lookback_contacts_reg,
        COALESCE(t.lookback_contacts_cards,  0) AS lookback_contacts_cards,
        COALESCE(t.lookback_mnes_cards,      0) AS lookback_mnes_cards,
        COALESCE(t.lookback_opens_cards,     0) AS lookback_opens_cards,
        COALESCE(t.lookback_clicks_cards,    0) AS lookback_clicks_cards,
        COALESCE(o.max_mnes_month,           0) AS max_mnes_month,
        COALESCE(o.avg_mnes_month,           0) AS avg_mnes_month,
        COALESCE(o.max_cards_mnes_month,     0) AS max_cards_mnes_month
    FROM (SELECT DISTINCT CLNT_NO, cohort_group, cohort_month FROM vt_unsub_journey_pop) p
    LEFT JOIN totals t
        ON t.CLNT_NO = p.CLNT_NO AND t.cohort_group = p.cohort_group AND t.cohort_month = p.cohort_month
    LEFT JOIN ov o
        ON o.CLNT_NO = p.CLNT_NO AND o.cohort_group = p.cohort_group AND o.cohort_month = p.cohort_month
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_lookback_client COLUMN (cohort_group, cohort_month);


-- ============================================================================
-- R1: MAIN TABLE — 12mo contact history, engagement classified BEFORE the window
-- ~18 rows (2 cohort_group x 3 engagement x 3 cohort_month).
-- engagement uses the LOOKBACK, not the unsubscribe email. That is the whole point: unsubscribing
-- requires opening and clicking, so classifying off the unsub email is circular.
-- ============================================================================
SELECT
    cohort_group,
    CASE WHEN lookback_clicks > 0 THEN 'clicked'
         WHEN lookback_opens  > 0 THEN 'opened'
         ELSE 'dark' END                                AS engagement,
    cohort_month,
    COUNT(*)                                            AS n_clients,
    CAST(AVG(lookback_contacts)       AS DECIMAL(10,1)) AS avg_contacts,
    CAST(AVG(lookback_mnes)           AS DECIMAL(10,1)) AS avg_mnes,
    CAST(AVG(lookback_contacts_cards) AS DECIMAL(10,1)) AS avg_contacts_cards,
    CAST(AVG(lookback_mnes_cards)     AS DECIMAL(10,1)) AS avg_mnes_cards,
    CAST(AVG(lookback_contacts_reg)   AS DECIMAL(10,1)) AS avg_contacts_reg,
    CAST(AVG(max_mnes_month)          AS DECIMAL(10,1)) AS avg_max_mnes_month,
    -- editable: contact bands, BANK-WIDE marketing (regulatory excluded)
    SUM(CASE WHEN lookback_contacts = 0               THEN 1 ELSE 0 END) AS c_0,
    SUM(CASE WHEN lookback_contacts BETWEEN 1  AND 2  THEN 1 ELSE 0 END) AS c_1_2,
    SUM(CASE WHEN lookback_contacts BETWEEN 3  AND 6  THEN 1 ELSE 0 END) AS c_3_6,
    SUM(CASE WHEN lookback_contacts BETWEEN 7  AND 12 THEN 1 ELSE 0 END) AS c_7_12,
    SUM(CASE WHEN lookback_contacts BETWEEN 13 AND 24 THEN 1 ELSE 0 END) AS c_13_24,
    SUM(CASE WHEN lookback_contacts >= 25             THEN 1 ELSE 0 END) AS c_25p,
    -- editable: contact bands, CARDS ONLY
    SUM(CASE WHEN lookback_contacts_cards = 0              THEN 1 ELSE 0 END) AS cc_0,
    SUM(CASE WHEN lookback_contacts_cards BETWEEN 1 AND 2  THEN 1 ELSE 0 END) AS cc_1_2,
    SUM(CASE WHEN lookback_contacts_cards BETWEEN 3 AND 6  THEN 1 ELSE 0 END) AS cc_3_6,
    SUM(CASE WHEN lookback_contacts_cards BETWEEN 7 AND 12 THEN 1 ELSE 0 END) AS cc_7_12,
    SUM(CASE WHEN lookback_contacts_cards >= 13            THEN 1 ELSE 0 END) AS cc_13p
FROM vt_lookback_client
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- ============================================================================
-- R2: OVERLAP — brief 3c. Does the unsub rate rise when more campaigns land in the SAME month?
-- ~12 rows. Read the two n_ columns as a rate: unsub / (unsub + 10 * stayed).
-- max_mnes_month = the busiest single month in the client's prior year.
-- ============================================================================
SELECT
    CASE WHEN max_mnes_month = 0 THEN '0'
         WHEN max_mnes_month = 1 THEN '1'
         WHEN max_mnes_month = 2 THEN '2'
         WHEN max_mnes_month = 3 THEN '3'
         WHEN max_mnes_month BETWEEN 4 AND 5 THEN '4-5'
         ELSE '6+' END                                          AS max_mnes_month_band,
    SUM(CASE WHEN cohort_group = 'unsub'  THEN 1 ELSE 0 END)     AS n_unsub,
    SUM(CASE WHEN cohort_group = 'stayed' THEN 1 ELSE 0 END)     AS n_stayed_sampled,
    CAST(AVG(lookback_contacts)       AS DECIMAL(10,1))          AS avg_contacts,
    CAST(AVG(lookback_contacts_cards) AS DECIMAL(10,1))          AS avg_contacts_cards
FROM vt_lookback_client
GROUP BY 1
ORDER BY 1;


-- ============================================================================
-- R3: OVERLAP, CARDS LENS — same question, cards campaigns only
-- ============================================================================
SELECT
    CASE WHEN max_cards_mnes_month = 0 THEN '0'
         WHEN max_cards_mnes_month = 1 THEN '1'
         WHEN max_cards_mnes_month = 2 THEN '2'
         ELSE '3+' END                                          AS max_cards_mnes_month_band,
    SUM(CASE WHEN cohort_group = 'unsub'  THEN 1 ELSE 0 END)     AS n_unsub,
    SUM(CASE WHEN cohort_group = 'stayed' THEN 1 ELSE 0 END)     AS n_stayed_sampled,
    CAST(AVG(lookback_contacts_cards) AS DECIMAL(10,1))          AS avg_contacts_cards,
    CAST(AVG(lookback_mnes_cards)     AS DECIMAL(10,1))          AS avg_mnes_cards
FROM vt_lookback_client
GROUP BY 1
ORDER BY 1;


-- ============================================================================
-- R4: RECONCILIATION — run this FIRST when results land. If it does not tie, nothing else counts.
-- Expect leavers close to the museum's L7 62,658. Stayers here are the 1-in-10 sample: expect
-- ~907,000 BEFORE the already_out exclusion, LOWER after it. A stayer count that did not move
-- means VT2b matched nothing and the exclusion silently did nothing.
-- ============================================================================
SELECT
    cohort_group,
    COUNT(*)                                                     AS n_client_months,
    COUNT(DISTINCT CLNT_NO)                                      AS n_clients,
    SUM(CASE WHEN lookback_contacts     = 0 THEN 1 ELSE 0 END)   AS n_zero_marketing,
    SUM(CASE WHEN lookback_contacts_reg > 0 THEN 1 ELSE 0 END)   AS n_any_regulatory,
    SUM(CASE WHEN lookback_contacts = 0 AND lookback_contacts_reg > 0
             THEN 1 ELSE 0 END)                                  AS n_regulatory_only
FROM vt_lookback_client
GROUP BY 1
ORDER BY 1;


DROP TABLE vt_lookback_client;
DROP TABLE vt_lookback_grain;
DROP TABLE vt_unsub_journey_pop;
DROP TABLE vt_baseline_spine;
DROP TABLE vt_prior_unsub;
DROP TABLE vt_unsub_cohort;
DROP TABLE vt_unsub_events;
DROP TABLE vt_params;
