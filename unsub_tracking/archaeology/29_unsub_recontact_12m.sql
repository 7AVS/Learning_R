-- 29_unsub_recontact_12m.sql — July-2025 unsub cohort: forward 12mo recontact (FLIP of 19's lookback into a forward window)
-- Date: 2026-08-03. ENGINE: Teradata-direct.
-- Question: clients who unsubscribed from email in July 2025 — in the 12 months after, did they get emailed
-- again, how fast, by whom, how much?
-- COHORT: disposition_cd=4 (unsub) events, disposition_dt_tm in [2025-07-01, 2025-08-01). Anchor grain =
-- (CLNT_NO, unsub_mne); index_dt = first unsub per anchor in the month. Only shape-valid TREATMENT_IDs anchor
-- a cohort row (an unsub without a valid MNE can't anchor a same-MNE question) — excluded events are counted,
-- not silently dropped (R3a).
-- DAY-0 EXCLUSION: the forward window starts at index_dt + 1, NOT index_dt. A send on the SAME day as the
-- unsub is very often the email that TRIGGERED the unsubscribe (client receives -> clicks unsubscribe same
-- day), not a recontact after it. Counting it as recontact inflates pct_recontacted, the SAME class, and the
-- fastest timing bucket in the wrong direction. R3d sizes the excluded day-0 volume so it's visible, not hidden.
-- WINDOW: cohort = July 2025 unsubs; the forward window is a closed 12mo span of days 1..365(/366) after each
-- anchor's index_dt, closing 2026-07-31/08-01, right at the data edge as of this run date (2026-08-03) — R3c
-- is the completeness check that proves the feed actually reaches that far.
-- SPOOL NOTES: EVENT is ~400M rows and MASTER is large + not 1:1 — spool exhaustion is the failure mode this
-- pack is built to avoid, not correctness. Heaviest steps, rough magnitude, in order of risk:
--   1. vt_sent_evt (disp=1, 13mo span) is THE BIG ONE if left unscoped — EVENT-wide sends over 13mo could be a
--      large fraction of the 400M rows. It is semi-joined to vt_cohort_addr (the July cohort's own
--      consumer_id_hashed set, ~low hundreds of thousands) INSIDE the CREATE, so spool only ever holds cohort
--      sends, not all-of-RBC's sends.
--   2. vt_sent_resolved_raw — MASTER join keyed off vt_sent_evt's small (consumer_id_hashed,TREATMENT_ID) set,
--      still 2-pass load_tm-chunked (pack-22 idiom) as a second layer of defense since MASTER's non-1:1 rows
--      can multiply before the load_tm floor narrows them back down.
--   3. vt_unsub_evt / vt_unsub_resolved — small by construction (one month of disp=4 events); low risk.
-- If a spool error still occurs, shrink in this order: (a) date-chunk vt_sent_evt itself into 2-4 sequential
-- INSERTs by month (see OPTIONAL fallback at eof — same pattern as vt_sent_resolved_raw), (b) widen the
-- vt_sent_resolved_raw load_tm chunking from 2 passes to 4 with a MOD(CLNT_NO,4) slice per pass (also at eof),
-- (c) as a last resort, cut cohort scope — top-N MNEs by n_unsub_clients (from a first pass of R1) or a
-- MOD(CLNT_NO,N) slice on vt_cohort_clnt before it's used to build vt_cohort_addr / scope vt_sent_resolved_raw.
-- Pre-clean DROPs below: 'does not exist' errors on a fresh session are harmless. Always run top to bottom.

DROP TABLE vt_client_spine;
DROP TABLE vt_followup_agg;
DROP TABLE vt_followup_ranked;
DROP TABLE vt_followups;
DROP TABLE vt_sent_resolved;
DROP TABLE vt_sent_resolved_raw;
DROP TABLE vt_sent_evt;
DROP TABLE vt_cohort_addr;
DROP TABLE vt_cohort_clnt;
DROP TABLE vt_unsub_cohort;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_evt;
DROP TABLE vt_params;


-- editable: window_start/window_end are the ONLY place to move the cohort month (end is exclusive)
CREATE VOLATILE TABLE vt_params AS (
    SELECT DATE '2025-07-01' AS window_start, DATE '2025-08-01' AS window_end
) WITH DATA PRIMARY INDEX (window_start) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_params COLUMN (window_start);   -- gotcha 5: stats before CROSS JOIN vt_params (error 3149)


-- vt_unsub_evt: STAGE 1 — EVENT-first chunk, disp=4, cohort month only -- proves: small probe table before touching MASTER
CREATE VOLATILE TABLE vt_unsub_evt AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    CROSS JOIN vt_params vp
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= vp.window_start   -- cohort month start (inclusive)
      AND e.disposition_dt_tm <  vp.window_end     -- cohort month end (exclusive)
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_evt COLUMN (consumer_id_hashed, TREATMENT_ID);


-- vt_unsub_resolved: STAGE 2 — resolve CLNT_NO via MASTER (gotcha 1: CLNT_NO IS NOT NULL, dedup on output;
-- gotcha 2: join on consumer_id_hashed+TREATMENT_ID; gotcha 3: load_tm floored 3mo before earliest event date
-- used, i.e. window_start), plus the shape test + MNE extraction computed inline (THE RULE, no year-range filter)
-- -- proves: every window unsub event, resolved to a client, tagged valid/invalid before any cohort logic runs
CREATE VOLATILE TABLE vt_unsub_resolved AS (
    SELECT DISTINCT
        m.CLNT_NO,
        u.TREATMENT_ID,
        u.disposition_dt_tm,
        CASE WHEN CHARACTER_LENGTH(TRIM(u.TREATMENT_ID)) = 10
              AND SUBSTR(TRIM(u.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
             THEN 1 ELSE 0 END AS shape_valid,
        CASE WHEN CHARACTER_LENGTH(TRIM(u.TREATMENT_ID)) = 10
              AND SUBSTR(TRIM(u.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
             THEN SUBSTR(TRIM(u.TREATMENT_ID), 8, 3)
             ELSE NULL END AS unsub_mne
    FROM vt_unsub_evt u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
    CROSS JOIN vt_params vp
    WHERE m.CLNT_NO IS NOT NULL                          -- gotcha 1: 3.8% NULL CLNT_NO excluded explicitly
      AND m.load_tm >= ADD_MONTHS(vp.window_start, -3)   -- gotcha 3: floor >=3mo before earliest event date used
      AND m.load_tm <  ADD_MONTHS(vp.window_end, 1)
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_resolved COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_unsub_resolved COLUMN (CLNT_NO, unsub_mne);


-- vt_unsub_cohort: anchor grain (CLNT_NO, unsub_mne), index_dt = FIRST unsub in the month per anchor,
-- shape-invalid rows excluded here (they can't anchor a same-MNE question) -- proves: one row per qualifying
-- anchor, first-touch date only, ranked+filtered inside a volatile table (gotcha 5: no ROW_NUMBER in subqueries)
CREATE VOLATILE TABLE vt_unsub_cohort AS (
    WITH ranked AS (
        SELECT
            CLNT_NO,
            unsub_mne,
            disposition_dt_tm,
            ROW_NUMBER() OVER (PARTITION BY CLNT_NO, unsub_mne ORDER BY disposition_dt_tm ASC) AS rn
        FROM vt_unsub_resolved
        WHERE shape_valid = 1                            -- only shape-valid unsubs anchor a cohort row
    )
    SELECT
        CLNT_NO,
        unsub_mne,
        CAST(disposition_dt_tm AS DATE) AS index_dt       -- day-grain anchor date; see judgment-call note in report
    FROM ranked
    WHERE rn = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_cohort COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_unsub_cohort COLUMN (CLNT_NO, unsub_mne);


-- vt_cohort_clnt: distinct cohort CLNT_NO, tiny scoping table -- proves: lets the sent-side MASTER join restrict
-- to cohort clients only instead of resolving CLNT_NO for the whole client base
CREATE VOLATILE TABLE vt_cohort_clnt AS (
    SELECT DISTINCT CLNT_NO
    FROM vt_unsub_cohort
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cohort_clnt COLUMN (CLNT_NO);


-- vt_cohort_addr: distinct consumer_id_hashed seen on the July unsub events (small, ~low hundreds of thousands)
-- -- proves: the semi-join key that keeps the sends-side EVENT pull below from ever touching non-cohort
-- addresses. Deliberately built from the RAW unsub-event address set (vt_unsub_evt), not just shape-valid
-- cohort anchors — a few extra addresses here is harmless (vt_sent_resolved re-scopes to actual cohort CLNT_NO
-- via vt_cohort_clnt later), but missing an address would silently drop real recontact volume.
-- JUDGMENT CALL: this only catches sends to addresses the client had ALREADY used to unsubscribe. A send to a
-- brand-new address the client started using only AFTER July 2025 would be missed. Traded off deliberately for
-- spool safety (see header SPOOL NOTES) — flag to Andre if address churn within 12mo is a known concern.
CREATE VOLATILE TABLE vt_cohort_addr AS (
    SELECT DISTINCT consumer_id_hashed
    FROM vt_unsub_evt
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cohort_addr COLUMN (consumer_id_hashed);


-- vt_sent_evt: STAGE 1 — EVENT-first chunk, disp=1 (sent), bounded to the widest possible anchor window
-- (earliest possible index_dt = window_start, through latest possible index_dt + 12mo = window_end + 12mo) AND
-- semi-joined to vt_cohort_addr -- proves: THE spool-safety join (see header SPOOL NOTES #1) — this pull only
-- ever materializes sends to cohort addresses, never all-of-RBC's 13mo of disp=1 EVENT rows. Single INSERT is
-- judged safe here because the semi-join to a small (low-hundreds-of-thousands-row) table lets the optimizer
-- redistribute/hash-join instead of scanning+materializing the full disp=1 EVENT slice; see eof OPTIONAL for a
-- month-chunked fallback if it still spools.
CREATE VOLATILE TABLE vt_sent_evt AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN vt_cohort_addr ca ON ca.consumer_id_hashed = e.consumer_id_hashed   -- semi-join: cohort addresses only
    CROSS JOIN vt_params vp
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= vp.window_start                 -- earliest possible index_dt
      AND e.disposition_dt_tm <  ADD_MONTHS(vp.window_end, 12)   -- latest possible index_dt + 12mo
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent_evt COLUMN (consumer_id_hashed, TREATMENT_ID);


-- vt_sent_resolved_raw: STAGE 2 — resolve CLNT_NO via MASTER, joined ONLY to the (consumer_id_hashed,
-- TREATMENT_ID) pairs present in vt_sent_evt (gotcha 6 — never a bare/unconstrained MASTER scan), plus a
-- second scoping layer (vt_cohort_clnt INNER JOIN on CLNT_NO — belt #2, since vt_sent_evt is already
-- address-scoped to cohort addresses via belt #1), shape test + MNE computed inline. Still chunked into 2
-- sequential MASTER passes on load_tm (idiom from 22) as a third layer of defense: MASTER is not 1:1, so even
-- key-constrained, a given (consumer_id_hashed,TREATMENT_ID) can carry many load_tm-dated rows across the
-- ~17mo span before the floor/pad filters narrow it down — chunk to keep each pass's intermediate spool small.
-- NOTE: since MASTER is not 1:1 (gotcha 1), the same (CLNT_NO,TREATMENT_ID,send_dt) can resolve via a
-- MASTER row in pass 1 AND a different MASTER row (same key, different load_tm) in pass 2 — this raw table can
-- carry cross-pass duplicates; vt_sent_resolved below does the final dedup.
-- pass 1/2: load_tm window_start-3mo to window_end
CREATE VOLATILE TABLE vt_sent_resolved_raw AS (
    SELECT DISTINCT
        m.CLNT_NO,
        s.TREATMENT_ID,
        CAST(s.disposition_dt_tm AS DATE) AS send_dt,
        CASE WHEN CHARACTER_LENGTH(TRIM(s.TREATMENT_ID)) = 10
              AND SUBSTR(TRIM(s.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
             THEN 1 ELSE 0 END AS shape_valid,
        CASE WHEN CHARACTER_LENGTH(TRIM(s.TREATMENT_ID)) = 10
              AND SUBSTR(TRIM(s.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
             THEN SUBSTR(TRIM(s.TREATMENT_ID), 8, 3)
             ELSE NULL END AS send_mne
    FROM vt_sent_evt s
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = s.consumer_id_hashed
        AND m.TREATMENT_ID       = s.TREATMENT_ID
    INNER JOIN vt_cohort_clnt cc ON cc.CLNT_NO = m.CLNT_NO         -- scope to cohort clients only
    CROSS JOIN vt_params vp
    WHERE m.CLNT_NO IS NOT NULL                                    -- gotcha 1
      AND m.load_tm >= ADD_MONTHS(vp.window_start, -3)             -- gotcha 3: floor >=3mo before earliest event date
      AND m.load_tm <  vp.window_end
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

-- pass 2/2: load_tm window_end to window_end+13mo (event window + 1mo pad)
INSERT INTO vt_sent_resolved_raw
    SELECT DISTINCT
        m.CLNT_NO,
        s.TREATMENT_ID,
        CAST(s.disposition_dt_tm AS DATE) AS send_dt,
        CASE WHEN CHARACTER_LENGTH(TRIM(s.TREATMENT_ID)) = 10
              AND SUBSTR(TRIM(s.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
             THEN 1 ELSE 0 END AS shape_valid,
        CASE WHEN CHARACTER_LENGTH(TRIM(s.TREATMENT_ID)) = 10
              AND SUBSTR(TRIM(s.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
             THEN SUBSTR(TRIM(s.TREATMENT_ID), 8, 3)
             ELSE NULL END AS send_mne
    FROM vt_sent_evt s
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = s.consumer_id_hashed
        AND m.TREATMENT_ID       = s.TREATMENT_ID
    INNER JOIN vt_cohort_clnt cc ON cc.CLNT_NO = m.CLNT_NO
    CROSS JOIN vt_params vp
    WHERE m.CLNT_NO IS NOT NULL
      AND m.load_tm >= vp.window_end
      AND m.load_tm <  ADD_MONTHS(vp.window_end, 13);

COLLECT STATISTICS ON vt_sent_resolved_raw COLUMN (CLNT_NO);


-- vt_sent_resolved: final dedup across the 2 chunk passes -- proves: exact (CLNT_NO, TREATMENT_ID, send_dt)
-- grain per gotcha 4, no cross-pass double-count from MASTER's non-1:1 rows
CREATE VOLATILE TABLE vt_sent_resolved AS (
    SELECT DISTINCT
        CLNT_NO,
        TREATMENT_ID,
        send_dt,
        shape_valid,
        send_mne
    FROM vt_sent_resolved_raw
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent_resolved COLUMN (CLNT_NO);


-- vt_followups: cohort x sends, filtered to EACH anchor's own [index_dt+1, index_dt+12mo+1) window — day 0
-- (the unsub day itself) is EXCLUDED: a same-day send is very often the trigger for the unsubscribe, not a
-- recontact after it (see header). This is a closed 12mo span of days 1..365(/366) after index_dt. Send
-- classified against THAT anchor's unsub_mne (SAME beats REG_OPS beats OTHER beats DEFAULT, in that CASE
-- order) -- proves: every follow-up send in scope, one row per (anchor, send), correctly classified per anchor
-- not per client
CREATE VOLATILE TABLE vt_followups AS (
    SELECT
        c.CLNT_NO,
        c.unsub_mne,
        c.index_dt,
        s.TREATMENT_ID,
        s.send_dt,
        CASE
            WHEN s.shape_valid = 1 AND s.send_mne = c.unsub_mne THEN 'SAME'
            -- REG_OPS: the 10 cards regulatory/operational/fulfillment codes -- this list is cards-LOB only
            WHEN s.shape_valid = 1 AND s.send_mne IN
                ('AML','BAF','HCD','MEF','OTC','PON','RPF','WJA','WJF','WNH') THEN 'REG_OPS'
            WHEN s.shape_valid = 1 THEN 'OTHER'
            -- DEFAULT: fails the shape test (TREATMENT_ID='DEFAULT' or other non-dated residue) -- mixed
            -- service/regulatory/untagged-marketing, un-splittable
            ELSE 'DEFAULT'
        END AS send_class
    FROM vt_unsub_cohort c
    INNER JOIN vt_sent_resolved s ON s.CLNT_NO = c.CLNT_NO
    WHERE s.send_dt >= c.index_dt + 1                       -- day-0 exclusion: window starts the day AFTER the unsub
      AND s.send_dt <  ADD_MONTHS(c.index_dt, 12) + 1        -- shifted +1 day to keep the window a full 12mo span
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_followups COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_followups COLUMN (CLNT_NO, unsub_mne);


-- vt_followup_ranked: rank each anchor's follow-ups by send_dt (tie-break TREATMENT_ID) to pin down the FIRST
-- send's class -- proves: rn_first=1 identifies exactly one row per anchor for first-contact classification
CREATE VOLATILE TABLE vt_followup_ranked AS (
    SELECT
        CLNT_NO,
        unsub_mne,
        send_class,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, unsub_mne ORDER BY send_dt ASC, TREATMENT_ID ASC) AS rn_first
    FROM vt_followups
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_followup_ranked COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_followup_ranked COLUMN (CLNT_NO, unsub_mne);


-- vt_followup_agg: per-anchor counts by class + first/last send date + first-contact class -- proves: one row
-- per RECONTACTED anchor (INNER JOIN only produces anchors that had >=1 follow-up), ready to LEFT JOIN onto cohort
CREATE VOLATILE TABLE vt_followup_agg AS (
    WITH counts AS (
        SELECT
            CLNT_NO,
            unsub_mne,
            COUNT(*)                                            AS total_emails,
            SUM(CASE WHEN send_class = 'SAME'    THEN 1 ELSE 0 END) AS emails_same,
            SUM(CASE WHEN send_class = 'REG_OPS' THEN 1 ELSE 0 END) AS emails_reg_ops,
            SUM(CASE WHEN send_class = 'OTHER'   THEN 1 ELSE 0 END) AS emails_other,
            SUM(CASE WHEN send_class = 'DEFAULT' THEN 1 ELSE 0 END) AS emails_default,
            MIN(send_dt) AS first_send_dt,
            MAX(send_dt) AS last_send_dt
        FROM vt_followups
        GROUP BY 1, 2
    ),
    first_class AS (
        SELECT CLNT_NO, unsub_mne, send_class AS first_class
        FROM vt_followup_ranked
        WHERE rn_first = 1
    )
    SELECT
        cnt.CLNT_NO,
        cnt.unsub_mne,
        cnt.total_emails,
        cnt.emails_same,
        cnt.emails_reg_ops,
        cnt.emails_other,
        cnt.emails_default,
        cnt.first_send_dt,
        cnt.last_send_dt,
        fc.first_class
    FROM counts cnt
    INNER JOIN first_class fc ON fc.CLNT_NO = cnt.CLNT_NO AND fc.unsub_mne = cnt.unsub_mne
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_followup_agg COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_followup_agg COLUMN (CLNT_NO, unsub_mne);


-- vt_client_spine: FULL cohort LEFT JOIN follow-up aggregates -- proves: every anchor is represented exactly
-- once, recontacted=0 anchors keep NULL first/last dt and 0 email counts (never bucket), days_to_first/last are
-- simple DATE subtraction (day-grain, matches 22's gap_days idiom). Because vt_followups already excludes day 0,
-- days_to_first ranges 1..365(/366) by construction — never 0. PI stays CLNT_NO (high-cardinality) even though
-- R1 GROUP BYs on unsub_mne below — unsub_mne is low-cardinality (a few dozen MNE codes) and would skew rows
-- onto a handful of AMPs if used as PI; GROUP BY doesn't require PI alignment, it redistributes regardless.
CREATE VOLATILE TABLE vt_client_spine AS (
    SELECT
        c.CLNT_NO,
        c.unsub_mne,
        c.index_dt,
        COALESCE(a.total_emails, 0)   AS total_emails,
        COALESCE(a.emails_same, 0)    AS emails_same,
        COALESCE(a.emails_reg_ops, 0) AS emails_reg_ops,
        COALESCE(a.emails_other, 0)   AS emails_other,
        COALESCE(a.emails_default, 0) AS emails_default,
        a.first_send_dt,
        a.last_send_dt,
        a.first_class,
        CASE WHEN a.first_send_dt IS NOT NULL THEN a.first_send_dt - c.index_dt END AS days_to_first,
        CASE WHEN a.last_send_dt  IS NOT NULL THEN a.last_send_dt  - c.index_dt END AS days_to_last,
        CASE WHEN a.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS recontacted
    FROM vt_unsub_cohort c
    LEFT JOIN vt_followup_agg a ON a.CLNT_NO = c.CLNT_NO AND a.unsub_mne = c.unsub_mne
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_client_spine COLUMN (unsub_mne);
COLLECT STATISTICS ON vt_client_spine COLUMN (CLNT_NO);


-- ===== R1 — MNE-grain summary: one row per unsub_mne, ordered by n_unsub_clients DESC =====
-- R1 DECISION: for each MNE clients unsubscribed from in July 2025, did they get emailed again in the next
-- 12mo, how fast (first-contact timing bands, share of ALL anchors), by whom (SAME/REG_OPS/OTHER/DEFAULT mix),
-- and how much (median emails among the recontacted only). n_unsub_clients is anchor count (CLNT_NO,unsub_mne),
-- not distinct-client count — a client unsubscribing from 2 MNEs contributes 2 anchors/2 rows-worth; see R3b.
WITH mne_all AS (
    SELECT
        unsub_mne,
        COUNT(*)                                            AS n_unsub_clients,
        SUM(recontacted)                                    AS n_recontacted_12m,
        SUM(total_emails)                                   AS total_emails_12m,
        SUM(emails_same)                                    AS emails_same,
        SUM(emails_reg_ops)                                 AS emails_reg_ops,
        SUM(emails_other)                                   AS emails_other,
        SUM(emails_default)                                 AS emails_default,
        -- editable: first-contact timing bands, discrete, sum to 100% of ALL anchors incl. never; day 0 excluded
        -- from the follow-up window (see header) so bands start at day 1, not day 0
        SUM(CASE WHEN days_to_first BETWEEN 1  AND 7   THEN 1 ELSE 0 END) AS d1_7,
        SUM(CASE WHEN days_to_first BETWEEN 8  AND 14  THEN 1 ELSE 0 END) AS d8_14,
        SUM(CASE WHEN days_to_first BETWEEN 15 AND 30  THEN 1 ELSE 0 END) AS d15_30,
        SUM(CASE WHEN days_to_first BETWEEN 31 AND 90  THEN 1 ELSE 0 END) AS d31_90,
        SUM(CASE WHEN days_to_first BETWEEN 91 AND 180 THEN 1 ELSE 0 END) AS d91_180,
        SUM(CASE WHEN days_to_first >= 181              THEN 1 ELSE 0 END) AS d181_365,  -- catch-all through window edge (365/366)
        SUM(CASE WHEN recontacted = 0                   THEN 1 ELSE 0 END) AS never
    FROM vt_client_spine
    GROUP BY 1
),
mne_recontacted AS (
    -- medians and first-contact class shares computed over RECONTACTED anchors only (gotcha 7: no MEDIAN aggregate)
    SELECT
        unsub_mne,
        COUNT(*)                                                    AS n_recontacted_check,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_emails)   AS med_emails_per_client,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_first)  AS med_days_to_first,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_last)   AS med_days_to_last,
        SUM(CASE WHEN first_class = 'SAME'    THEN 1 ELSE 0 END)    AS first_same_n,
        SUM(CASE WHEN first_class = 'REG_OPS' THEN 1 ELSE 0 END)    AS first_reg_ops_n,
        SUM(CASE WHEN first_class = 'OTHER'   THEN 1 ELSE 0 END)    AS first_other_n,
        SUM(CASE WHEN first_class = 'DEFAULT' THEN 1 ELSE 0 END)    AS first_default_n
    FROM vt_client_spine
    WHERE recontacted = 1
    GROUP BY 1
)
SELECT
    a.unsub_mne,
    a.n_unsub_clients,
    a.n_recontacted_12m,
    CAST(100.0 * a.n_recontacted_12m / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS pct_recontacted,
    a.total_emails_12m,
    CAST(r.med_emails_per_client AS DECIMAL(10,1)) AS med_emails_per_client,
    CAST(100.0 * a.d1_7     / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d1_7,
    CAST(100.0 * a.d8_14    / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d8_14,
    CAST(100.0 * a.d15_30   / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d15_30,
    CAST(100.0 * a.d31_90   / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d31_90,
    CAST(100.0 * a.d91_180  / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d91_180,
    CAST(100.0 * a.d181_365 / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d181_365,
    CAST(100.0 * a.never    / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS never,
    a.emails_same,
    a.emails_reg_ops,
    a.emails_other,
    a.emails_default,
    CAST(100.0 * r.first_same_n    / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_same_pct,
    CAST(100.0 * r.first_reg_ops_n / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_reg_ops_pct,
    CAST(100.0 * r.first_other_n   / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_other_pct,
    CAST(100.0 * r.first_default_n / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_default_pct,
    CAST(r.med_days_to_first AS DECIMAL(10,1)) AS med_days_to_first,
    CAST(r.med_days_to_last  AS DECIMAL(10,1)) AS med_days_to_last
FROM mne_all a
LEFT JOIN mne_recontacted r ON r.unsub_mne = a.unsub_mne
ORDER BY a.n_unsub_clients DESC;


-- ===== R2 — overall row: all MNEs pooled, unsub_mne='ALL', same columns as R1 =====
-- R2 DECISION: the single-number version of R1 — same anchor-grain pooling, same denominators, no GROUP BY
WITH mne_all AS (
    SELECT
        CAST('ALL' AS VARCHAR(3)) AS unsub_mne,
        COUNT(*)                                            AS n_unsub_clients,
        SUM(recontacted)                                    AS n_recontacted_12m,
        SUM(total_emails)                                   AS total_emails_12m,
        SUM(emails_same)                                    AS emails_same,
        SUM(emails_reg_ops)                                 AS emails_reg_ops,
        SUM(emails_other)                                   AS emails_other,
        SUM(emails_default)                                 AS emails_default,
        SUM(CASE WHEN days_to_first BETWEEN 1  AND 7   THEN 1 ELSE 0 END) AS d1_7,
        SUM(CASE WHEN days_to_first BETWEEN 8  AND 14  THEN 1 ELSE 0 END) AS d8_14,
        SUM(CASE WHEN days_to_first BETWEEN 15 AND 30  THEN 1 ELSE 0 END) AS d15_30,
        SUM(CASE WHEN days_to_first BETWEEN 31 AND 90  THEN 1 ELSE 0 END) AS d31_90,
        SUM(CASE WHEN days_to_first BETWEEN 91 AND 180 THEN 1 ELSE 0 END) AS d91_180,
        SUM(CASE WHEN days_to_first >= 181              THEN 1 ELSE 0 END) AS d181_365,
        SUM(CASE WHEN recontacted = 0                   THEN 1 ELSE 0 END) AS never
    FROM vt_client_spine
),
mne_recontacted AS (
    SELECT
        CAST('ALL' AS VARCHAR(3)) AS unsub_mne,
        COUNT(*)                                                    AS n_recontacted_check,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_emails)   AS med_emails_per_client,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_first)  AS med_days_to_first,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_last)   AS med_days_to_last,
        SUM(CASE WHEN first_class = 'SAME'    THEN 1 ELSE 0 END)    AS first_same_n,
        SUM(CASE WHEN first_class = 'REG_OPS' THEN 1 ELSE 0 END)    AS first_reg_ops_n,
        SUM(CASE WHEN first_class = 'OTHER'   THEN 1 ELSE 0 END)    AS first_other_n,
        SUM(CASE WHEN first_class = 'DEFAULT' THEN 1 ELSE 0 END)    AS first_default_n
    FROM vt_client_spine
    WHERE recontacted = 1
)
SELECT
    a.unsub_mne,
    a.n_unsub_clients,
    a.n_recontacted_12m,
    CAST(100.0 * a.n_recontacted_12m / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS pct_recontacted,
    a.total_emails_12m,
    CAST(r.med_emails_per_client AS DECIMAL(10,1)) AS med_emails_per_client,
    CAST(100.0 * a.d1_7     / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d1_7,
    CAST(100.0 * a.d8_14    / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d8_14,
    CAST(100.0 * a.d15_30   / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d15_30,
    CAST(100.0 * a.d31_90   / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d31_90,
    CAST(100.0 * a.d91_180  / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d91_180,
    CAST(100.0 * a.d181_365 / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS d181_365,
    CAST(100.0 * a.never    / NULLIF(a.n_unsub_clients, 0) AS DECIMAL(5,1)) AS never,
    a.emails_same,
    a.emails_reg_ops,
    a.emails_other,
    a.emails_default,
    CAST(100.0 * r.first_same_n    / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_same_pct,
    CAST(100.0 * r.first_reg_ops_n / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_reg_ops_pct,
    CAST(100.0 * r.first_other_n   / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_other_pct,
    CAST(100.0 * r.first_default_n / NULLIF(r.n_recontacted_check, 0) AS DECIMAL(5,1)) AS first_default_pct,
    CAST(r.med_days_to_first AS DECIMAL(10,1)) AS med_days_to_first,
    CAST(r.med_days_to_last  AS DECIMAL(10,1)) AS med_days_to_last
FROM mne_all a
LEFT JOIN mne_recontacted r ON r.unsub_mne = a.unsub_mne;


-- ===== R3 — diagnostics =====

-- R3a(1) — cohort exclusions: shape-invalid unsub events in the window, counts -- proves: excluded volume is
-- visible, not silently dropped
SELECT
    CAST(COUNT(*) AS BIGINT)               AS n_excluded_events,
    COUNT(DISTINCT CLNT_NO)                AS n_excluded_clients
FROM vt_unsub_resolved
WHERE shape_valid = 0;

-- R3a(2) — top offending TREATMENT_ID values among shape-invalid unsub events
SELECT TOP 15
    TREATMENT_ID,
    CAST(COUNT(*) AS BIGINT) AS n_events
FROM vt_unsub_resolved
WHERE shape_valid = 0
GROUP BY 1
ORDER BY n_events DESC;

-- R3b — multi-MNE unsubscribers: distribution of distinct unsub_mne count per client in the cohort month --
-- proves: how much of R1/R2's anchor-vs-client gap (n_unsub_clients is anchors, not distinct clients) is real
WITH per_client AS (
    SELECT CLNT_NO, COUNT(DISTINCT unsub_mne) AS n_mnes
    FROM vt_unsub_cohort
    GROUP BY 1
)
SELECT
    CASE WHEN n_mnes = 1 THEN '1'
         WHEN n_mnes = 2 THEN '2'
         WHEN n_mnes = 3 THEN '3'
         ELSE '4+' END AS mne_count_band,
    CAST(COUNT(*) AS BIGINT) AS n_clients
FROM per_client
GROUP BY 1
ORDER BY 1;

-- R3c — window-edge check: max disposition_dt_tm and max load_tm seen in EVENT for disposition_cd=1 -- proves
-- whether the feed actually covers through 2026-07-31, the forward window's close date; if max_disposition_dt_tm
-- falls short of 2026-07-31, R1/R2's 'never' bucket and d181_365 band for late-July-2025 anchors are understated
-- by a truncated window, not a true absence of recontact. INTENTIONALLY touches the base table, not a volatile
-- table: it has to reflect the TRUE global feed edge, independent of this pack's cohort-address scoping (a
-- cohort-scoped volatile table could show a stale max even when the feed itself is current). Kept spool-safe by
-- a tight disposition_cd + ~7-week recent-only date floor instead of a full-history scan.
SELECT
    MAX(disposition_dt_tm) AS max_disposition_dt_tm,
    MAX(load_tm)            AS max_load_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 1
  AND disposition_dt_tm >= DATE '2026-06-15';   -- tight recent floor only (~7wk); full-history MAX would be an unfiltered scan (gotcha 5)

-- R3d — day-0 contamination: cohort anchors that had a SAME-DAY send (the volume the day-0 exclusion above
-- removed from the follow-up window), and how many of those day-0 sends were SAME-class -- proves the size of
-- the triggering-send contamination the fix avoids, so it's visible rather than silently absorbed into 'never'
-- or 'd1_7'
WITH day0 AS (
    SELECT
        c.CLNT_NO,
        c.unsub_mne,
        CASE
            WHEN s.shape_valid = 1 AND s.send_mne = c.unsub_mne THEN 'SAME'
            WHEN s.shape_valid = 1 AND s.send_mne IN
                ('AML','BAF','HCD','MEF','OTC','PON','RPF','WJA','WJF','WNH') THEN 'REG_OPS'
            WHEN s.shape_valid = 1 THEN 'OTHER'
            ELSE 'DEFAULT'
        END AS send_class
    FROM vt_unsub_cohort c
    INNER JOIN vt_sent_resolved s ON s.CLNT_NO = c.CLNT_NO
    WHERE s.send_dt = c.index_dt                       -- day 0 exactly: the unsub day itself
),
day0_anchor AS (
    SELECT CLNT_NO, unsub_mne, MAX(CASE WHEN send_class = 'SAME' THEN 1 ELSE 0 END) AS has_same
    FROM day0
    GROUP BY 1, 2
)
SELECT
    CAST(COUNT(*)       AS BIGINT) AS n_anchors_with_day0_send,
    CAST(SUM(has_same)  AS BIGINT) AS anchors_where_day0_send_was_same_class
FROM day0_anchor;


-- eof: drop volatile tables (children before parents)
DROP TABLE vt_client_spine;
DROP TABLE vt_followup_agg;
DROP TABLE vt_followup_ranked;
DROP TABLE vt_followups;
DROP TABLE vt_sent_resolved;
DROP TABLE vt_sent_resolved_raw;
DROP TABLE vt_sent_evt;
DROP TABLE vt_cohort_addr;
DROP TABLE vt_cohort_clnt;
DROP TABLE vt_unsub_cohort;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_evt;
DROP TABLE vt_params;

-- OPTIONAL fallback #1: if vt_sent_evt itself still spools despite the vt_cohort_addr semi-join, split its
-- single CREATE into 3 sequential date-chunk INSERTs across the 13mo span (pack-22 idiom, applied here to the
-- EVENT-side pull instead of the MASTER-side one) -- shrink step (a) from the header SPOOL NOTES:
-- CREATE VOLATILE TABLE vt_sent_evt AS ( ... same SELECT/JOIN ...
--     AND e.disposition_dt_tm >= vp.window_start
--     AND e.disposition_dt_tm <  ADD_MONTHS(vp.window_start, 5)   -- chunk 1/3: months 0-5
-- ) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;
-- INSERT INTO vt_sent_evt  ... same ...  AND e.disposition_dt_tm >= ADD_MONTHS(vp.window_start, 5)
--                                        AND e.disposition_dt_tm <  ADD_MONTHS(vp.window_start, 9)   -- chunk 2/3
-- INSERT INTO vt_sent_evt  ... same ...  AND e.disposition_dt_tm >= ADD_MONTHS(vp.window_start, 9)
--                                        AND e.disposition_dt_tm <  ADD_MONTHS(vp.window_end, 12)    -- chunk 3/3

-- OPTIONAL fallback #2: if CPU abort persists on vt_sent_resolved_raw (idiom from 19's tail comment) -- shrink
-- step (b) from the header SPOOL NOTES: split each of the 2 load_tm passes further by MOD(cc.CLNT_NO, 4) and
-- UNION ALL the 4 slices per pass:
-- ... same SELECT DISTINCT / JOIN / WHERE as vt_sent_resolved_raw pass 1 or 2 ...
--     AND MOD(cc.CLNT_NO, 4) = 0
-- UNION ALL  ... same ...  AND MOD(cc.CLNT_NO, 4) = 1
-- UNION ALL  ... same ...  AND MOD(cc.CLNT_NO, 4) = 2
-- UNION ALL  ... same ...  AND MOD(cc.CLNT_NO, 4) = 3
