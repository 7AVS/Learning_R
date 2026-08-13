/* =====================================================================
   32 — CPC 1012 flips -> last email decision before the flip
   ---------------------------------------------------------------------
   Question (skeptics' own framing, inverted): take every client whose
   most recent Banking E-Mail consent (PREF_ID 1012) changed to "No"
   in the past 18 months. For each, find the LAST email decision in the
   tactic history BEFORE that change. How close are they?

   Read rule for the output: emails are frequent, so a "nearby" email is
   expected by chance. If email clicks drive 1012 flips, gap days 0-1
   tower over everything. A flat spread across 0-30+ days = cadence
   coincidence, not attribution.

   Engine: Teradata-direct (QUALIFY, DATE literals, ADD_MONTHS).
   ===================================================================== */

/* ---------- OUTPUT A: flip_month x gap_bucket (save: 32_cpc_1012_gap_by_month.csv) ---------- */
WITH flips AS (
    /* most recent 1012 change-to-No per client, past 18 months */
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002                       /* 5002 = No */
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
),
last_email AS (
    /* last email decision on or before the flip date */
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DTZV01.TACTIC_EVNT_IP_AR_H60M t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-01-01'        /* data floor */
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )  /* EM = email, per channel_codes */
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
),
gapped AS (
    /* one row per flipping client; gap NULL = no email decision found since floor */
    SELECT f.CLNT_NO,
           TRIM(EXTRACT(YEAR FROM f.flip_dt)) || '-' ||
             TRIM(CASE WHEN EXTRACT(MONTH FROM f.flip_dt) < 10 THEN '0' ELSE '' END) ||
             TRIM(EXTRACT(MONTH FROM f.flip_dt))                       AS flip_month,
           f.flip_dt - le.email_dt                                     AS gap_days
    FROM flips f
    LEFT JOIN last_email le ON le.CLNT_NO = f.CLNT_NO
)
SELECT flip_month,
       CASE WHEN gap_days IS NULL   THEN '6_no_email_found'
            WHEN gap_days <= 1      THEN '1_same_or_next_day'
            WHEN gap_days <= 7      THEN '2_within_week'
            WHEN gap_days <= 30     THEN '3_within_month'
            WHEN gap_days <= 90     THEN '4_within_quarter'
            ELSE                         '5_over_90_days' END          AS gap_bucket,
       COUNT(*)                                                        AS n_clients
FROM gapped
GROUP BY 1, 2
ORDER BY 1, 2;


/* ---------- OUTPUT B: day-level gaps 0-90 for the histogram (save: 32_cpc_1012_gap_days.csv) ----------
   Same population. One row per gap day. This is the plot that decides it:
   an attribution story needs a tower at day 0-1.                        */
WITH flips AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS flip_dt
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= ADD_MONTHS(CURRENT_DATE, -18)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
),
last_email AS (
    SELECT f.CLNT_NO, f.flip_dt, t.TREATMT_STRT_DT AS email_dt
    FROM flips f
    JOIN DTZV01.TACTIC_EVNT_IP_AR_H60M t
      ON  t.CLNT_NO = f.CLNT_NO
      AND t.TREATMT_STRT_DT <= f.flip_dt
      AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND ( SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
            OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO ORDER BY t.TREATMT_STRT_DT DESC) = 1
)
SELECT f.flip_dt - le.email_dt                                         AS gap_days,
       COUNT(*)                                                        AS n_clients
FROM flips f
JOIN last_email le ON le.CLNT_NO = f.CLNT_NO
WHERE f.flip_dt - le.email_dt <= 90
GROUP BY 1
ORDER BY 1;
