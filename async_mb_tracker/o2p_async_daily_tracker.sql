-- =============================================================================
-- O2P Async Banner — Daily Tracker
-- =============================================================================
-- Jira: NBA-12268 | Live: April 13, 2026
-- Join: t.clnt_no = b.up_srf_id2_value
-- GA4 filter: lower(it_item_id) = 'i_298045'
-- Tactic: TACTIC_ID = '202609902P', mobile = TRIM(TACTIC_CELL_CD) LIKE '%IM%'
-- Run in: Starburst
-- =============================================================================


-- OUTPUT 1: Daily tracker
SELECT
    b.event_date,
    'O2P'                                                                                          AS campaign,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '202609902P')                                                               AS total_population,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '202609902P'
       AND TRIM(TACTIC_CELL_CD) LIKE '%IM%')                                                       AS mobile_population,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'view_promotion' THEN b.up_srf_id2_value END)   AS view_users,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'select_promotion' THEN b.up_srf_id2_value END) AS click_users
FROM
    (SELECT
        event_date,
        event_name,
        up_srf_id2_value
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE event_date >= date '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion', 'select_promotion')
    ) AS b
INNER JOIN (
    SELECT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '202609902P'
      AND TRIM(TACTIC_CELL_CD) LIKE '%IM%'
    ) AS t
ON t.clnt_no = b.up_srf_id2_value
GROUP BY b.event_date
ORDER BY b.event_date;


-- OUTPUT 2: YTD cumulative summary
SELECT
    'O2P'                                                                                          AS campaign,
    MIN(b.event_date)                                                                              AS date_from_inception,
    MAX(b.event_date)                                                                              AS date_last_event,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '202609902P')                                                               AS total_population,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '202609902P'
       AND TRIM(TACTIC_CELL_CD) LIKE '%IM%')                                                       AS mobile_population,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'view_promotion'
                        THEN b.up_srf_id2_value END)                                               AS unique_view_users,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'select_promotion'
                        THEN b.up_srf_id2_value END)                                               AS unique_click_users
FROM
    (SELECT
        event_date,
        event_name,
        up_srf_id2_value
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE event_date >= date '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion', 'select_promotion')
    ) AS b
INNER JOIN (
    SELECT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '202609902P'
      AND TRIM(TACTIC_CELL_CD) LIKE '%IM%'
    ) AS t
ON t.clnt_no = b.up_srf_id2_value;
