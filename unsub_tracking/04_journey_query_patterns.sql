-- =============================================================================
-- Vendor Feedback — journey query patterns (disposition_cd usage)
-- =============================================================================
-- EVENT is a journey log: one send = multiple rows sharing
-- (consumer_id_hashed, treatment_id), one row per stage, each timestamped.
-- disposition_cd: 1=sent, 2=opened, 3=clicked, 4=unsubscribed, 5=hardbounce,
-- 6=complaint. Stages 1->2->3 are sequential; 4/5/6 are terminal-ish outcomes.
-- Never count raw rows for funnel questions — collapse the journey first (P1).
-- ENGINE: Teradata-direct. History floor 2024-01-01.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- P1: send-journey grain — one row per (consumer, treatment), stage flags + times
-- ---------------------------------------------------------------------------
-- The workhorse. MAX(CASE)=stage ever happened; MIN(CASE..dt_tm)=first time it
-- happened. Base for funnels, unsub-given-open, time-to-unsub.
-- ---------------------------------------------------------------------------

SELECT
    consumer_id_hashed,
    treatment_id,
    MAX(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS sent,
    MAX(CASE WHEN disposition_cd = 2 THEN 1 ELSE 0 END) AS opened,
    MAX(CASE WHEN disposition_cd = 3 THEN 1 ELSE 0 END) AS clicked,
    MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsubscribed,
    MAX(CASE WHEN disposition_cd = 5 THEN 1 ELSE 0 END) AS hardbounce,
    MAX(CASE WHEN disposition_cd = 6 THEN 1 ELSE 0 END) AS complaint,
    MIN(CASE WHEN disposition_cd = 1 THEN disposition_dt_tm END) AS sent_tm,
    MIN(CASE WHEN disposition_cd = 2 THEN disposition_dt_tm END) AS first_open_tm,
    MIN(CASE WHEN disposition_cd = 3 THEN disposition_dt_tm END) AS first_click_tm,
    MIN(CASE WHEN disposition_cd = 4 THEN disposition_dt_tm END) AS unsub_tm
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '2024-01-01'
GROUP BY 1, 2;


-- ---------------------------------------------------------------------------
-- P2: campaign funnel — sequential stage counts per MNE x month (counts only)
-- ---------------------------------------------------------------------------
-- Built ON TOP of the P1 grain. Expect sent >= opened >= clicked per row of
-- output; unsub reads conditionally against the earlier stages downstream.
-- ---------------------------------------------------------------------------

SELECT
    SUBSTR(treatment_id, 8, 3)      AS mne,
    EXTRACT(YEAR FROM sent_tm) * 100
      + EXTRACT(MONTH FROM sent_tm) AS send_month_yyyymm,
    CAST(COUNT(*) AS BIGINT)        AS journeys,
    SUM(sent)                       AS n_sent,
    SUM(opened)                     AS n_opened,
    SUM(clicked)                    AS n_clicked,
    SUM(unsubscribed)               AS n_unsub,
    SUM(hardbounce)                 AS n_hardbounce,
    SUM(complaint)                  AS n_complaint
FROM (
    SELECT
        consumer_id_hashed,
        treatment_id,
        MAX(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS sent,
        MAX(CASE WHEN disposition_cd = 2 THEN 1 ELSE 0 END) AS opened,
        MAX(CASE WHEN disposition_cd = 3 THEN 1 ELSE 0 END) AS clicked,
        MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsubscribed,
        MAX(CASE WHEN disposition_cd = 5 THEN 1 ELSE 0 END) AS hardbounce,
        MAX(CASE WHEN disposition_cd = 6 THEN 1 ELSE 0 END) AS complaint,
        MIN(CASE WHEN disposition_cd = 1 THEN disposition_dt_tm END) AS sent_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1, 2
) j
WHERE sent_tm IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;


-- ---------------------------------------------------------------------------
-- P3: ordered journey path — step sequence within one send
-- ---------------------------------------------------------------------------
-- For path analysis / sequence mining; not for counting.
-- ---------------------------------------------------------------------------

SELECT
    consumer_id_hashed,
    treatment_id,
    disposition_cd,
    disposition_dt_tm,
    ROW_NUMBER() OVER (PARTITION BY consumer_id_hashed, treatment_id
                       ORDER BY disposition_dt_tm, disposition_cd) AS step_no
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '2024-01-01'
  AND treatment_id = '<PICK_ONE_TREATMENT_ID>'   -- always scope P3; unscoped = full-table sort
ORDER BY consumer_id_hashed, step_no;


-- ---------------------------------------------------------------------------
-- V1: sequentiality validation — journeys that violate the assumed order
-- ---------------------------------------------------------------------------
-- Proves whether the funnel assumption holds. Expect all three violation
-- counts near zero. Non-zero readings and their real-world causes:
--   unsub_without_sent : preference-center opt-out not tied to a send
--   click_without_open : image-blocking suppresses the open pixel (common)
--   open_before_sent   : timestamp/timezone ordering issues
-- ---------------------------------------------------------------------------

SELECT
    CAST(COUNT(*) AS BIGINT)                                        AS journeys_total,
    SUM(CASE WHEN unsubscribed = 1 AND sent = 0 THEN 1 ELSE 0 END)  AS unsub_without_sent,
    SUM(CASE WHEN clicked = 1 AND opened = 0 THEN 1 ELSE 0 END)     AS click_without_open,
    SUM(CASE WHEN first_open_tm < sent_tm THEN 1 ELSE 0 END)        AS open_before_sent
FROM (
    SELECT
        consumer_id_hashed,
        treatment_id,
        MAX(CASE WHEN disposition_cd = 1 THEN 1 ELSE 0 END) AS sent,
        MAX(CASE WHEN disposition_cd = 2 THEN 1 ELSE 0 END) AS opened,
        MAX(CASE WHEN disposition_cd = 3 THEN 1 ELSE 0 END) AS clicked,
        MAX(CASE WHEN disposition_cd = 4 THEN 1 ELSE 0 END) AS unsubscribed,
        MIN(CASE WHEN disposition_cd = 1 THEN disposition_dt_tm END) AS sent_tm,
        MIN(CASE WHEN disposition_cd = 2 THEN disposition_dt_tm END) AS first_open_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1, 2
) j;
