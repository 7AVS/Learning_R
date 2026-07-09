-- crv_vintage_gap_diagnostic.sql
-- Purpose: decompose the gap between crv_cohort_summary_v1_datalab.sql's responder
-- count and crv_vintage_v1_datalab.sql's terminal cum_responders. Account grain,
-- no acct->clnt bridge, same population filter as both files. Diagnostic only --
-- does not change vintage or summary logic.
--
-- What each column proves:
--   summary_responders    = COUNT(DISTINCT acct_no) WHERE responder=1. This is the
--                           TARGET -- exactly crv_cohort_summary_v1_datalab.sql's
--                           "responders" for this (cohort_month, arm).
--   resp_within_window    = COUNT(DISTINCT acct_no) WHERE responder=1 AND
--                           first_response_days BETWEEN 0 AND that account's OWN
--                           (offer_end_date - offer_start_date). This is what
--                           crv_vintage_v1_datalab.sql's terminal cum_responders
--                           actually counts: its day-axis spine runs 0..cohort_max_day
--                           and the dense grid is capped at each cell's own
--                           cohort_max_day, so only responses landing inside
--                           [0, offer_window] have a vintage_day to sum into.
--   resp_null_day         = responder=1 accounts with first_response_days IS NULL.
--                           No vintage_day to land on at all -- silently absent from
--                           the vintage curve's daily_counts CTE.
--   resp_after_window     = responder=1 accounts whose first_response_days falls
--                           AFTER their own offer window ends. These rows exist in
--                           the vintage file's daily_counts CTE (which is not capped)
--                           but never join to dense_grid (which IS capped at
--                           cohort_max_day) -- they drop out silently, not explicitly.
--   resp_negative_day     = responder=1 accounts whose first_response_days is
--                           negative (response dated before offer_start_date). The
--                           vintage day-spine starts at vintage_day=0, so these also
--                           have no matching grid point and drop out.
--   max_first_response_days = MAX(first_response_days) among responder=1 rows in the
--                           cell -- how far past day 0 responses are actually observed.
--   max_offer_window_days / min_offer_window_days = MAX/MIN across all accounts in
--                           the cell of (offer_end_date - offer_start_date) -- the
--                           longest/shortest offer window in that cohort_month x arm.
--                           Compare max_first_response_days to max_offer_window_days:
--                           if the former is much larger, responses are being recorded
--                           well after the offer window closes, i.e. the true
--                           response-measurement window is longer than the offer
--                           window itself.
--
-- resp_null_day + resp_after_window + resp_negative_day = summary_responders -
-- resp_within_window. The four responder=1 buckets (null / within-window /
-- after-window / negative-day) are mutually exclusive and collectively exhaustive,
-- since first_response_days is a single scalar value per row and the account's own
-- offer window (offer_end_date - offer_start_date) is computed per row, not
-- cohort-level, for the BETWEEN/comparison conditions.
--
-- Grain: account (acct_no), no bridge -- identical to both source files.
-- Population filter: offer_start_date >= DATE '2026-01-01' AND
--                     TRIM(action_control) IN ('Action', 'Control')  (same as
--                     crv_cohort_summary_v1_datalab.sql / crv_vintage_v1_datalab.sql)
-- cohort_month = first-of-month(offer_start_date); arm = TRIM(action_control)
-- Engine: Teradata-direct. Counts only -- no rates, no formatting.

WITH
acct_base AS (
    SELECT
        acct_no,
        (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
        CAST(TRIM(action_control) AS VARCHAR(10))                     AS arm,
        responder,
        first_response_days,
        (offer_end_date - offer_start_date)                           AS acct_offer_window_days
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
)
SELECT
    cohort_month,
    arm,
    COUNT(DISTINCT CASE WHEN responder = 1
                         THEN acct_no END)                                    AS summary_responders,
    COUNT(DISTINCT CASE WHEN responder = 1
                          AND first_response_days IS NULL
                         THEN acct_no END)                                    AS resp_null_day,
    COUNT(DISTINCT CASE WHEN responder = 1
                          AND first_response_days BETWEEN 0 AND acct_offer_window_days
                         THEN acct_no END)                                    AS resp_within_window,
    COUNT(DISTINCT CASE WHEN responder = 1
                          AND first_response_days > acct_offer_window_days
                         THEN acct_no END)                                    AS resp_after_window,
    COUNT(DISTINCT CASE WHEN responder = 1
                          AND first_response_days < 0
                         THEN acct_no END)                                    AS resp_negative_day,
    MAX(CASE WHEN responder = 1 THEN first_response_days END)                 AS max_first_response_days,
    MAX(acct_offer_window_days)                                               AS max_offer_window_days,
    MIN(acct_offer_window_days)                                               AS min_offer_window_days
FROM acct_base
GROUP BY cohort_month, arm
ORDER BY cohort_month, arm;
