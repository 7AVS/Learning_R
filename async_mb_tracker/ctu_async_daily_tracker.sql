-- =============================================================================
-- CTU Async Banner — Daily Tracker
-- =============================================================================
-- Jira: NBA-12268 | Live: April 10, 2026
-- Join: t.clnt_no = b.up_srf_id2_value
-- GA4 filter: lower(it_item_id) = 'i_300102'
-- Tactic: TACTIC_ID = '2026098CTU', mobile = SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
-- Run in: Starburst
-- =============================================================================


-- OUTPUT 1: Daily tracker
SELECT
    b.event_date,
    'CTU'                                                                                          AS campaign,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '2026098CTU')                                                               AS total_population,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '2026098CTU'
       AND SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%')                                AS mobile_population,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'view_promotion' THEN b.up_srf_id2_value END)   AS view_users,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'select_promotion' THEN b.up_srf_id2_value END) AS click_users
FROM
    (SELECT
        event_date,
        event_name,
        up_srf_id2_value
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE event_date >= date '2026-04-01'
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion', 'select_promotion')
    ) AS b
INNER JOIN (
    SELECT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026098CTU'
      AND SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
    ) AS t
ON t.clnt_no = b.up_srf_id2_value
GROUP BY b.event_date
ORDER BY b.event_date;


-- OUTPUT 2: YTD cumulative summary
SELECT
    'CTU'                                                                                          AS campaign,
    MIN(b.event_date)                                                                              AS date_from_inception,
    MAX(b.event_date)                                                                              AS date_last_event,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '2026098CTU')                                                               AS total_population,
    (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
     WHERE TACTIC_ID = '2026098CTU'
       AND SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%')                                AS mobile_population,
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
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion', 'select_promotion')
    ) AS b
INNER JOIN (
    SELECT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026098CTU'
      AND SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
    ) AS t
ON t.clnt_no = b.up_srf_id2_value;
