-- =============================================================================
-- Async Banner — Combined Daily Tracker (CTU + O2P)
-- =============================================================================
-- Jira: NBA-12268
-- Run in: Starburst
-- =============================================================================


-- OUTPUT 1: Daily tracker (both campaigns)
SELECT
    b.event_date,
    b.campaign,
    b.total_population,
    b.mobile_population,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'view_promotion' THEN b.up_srf_id2_value END)   AS view_users,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'select_promotion' THEN b.up_srf_id2_value END) AS click_users
FROM
    (
    -- CTU banner events
    SELECT
        event_date, event_name, up_srf_id2_value,
        'CTU' AS campaign,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '2026098CTU')                                                           AS total_population,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '2026098CTU'
           AND SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%')                             AS mobile_population
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON t.CLNT_NO = g.up_srf_id2_value
       AND t.TACTIC_ID = '2026098CTU'
       AND SUBSTRING(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
    WHERE g.event_date >= date '2026-04-01'
      AND lower(g.it_item_id) IN ('i_300102')
      AND lower(g.event_name) IN ('view_promotion', 'select_promotion')

    UNION ALL

    -- O2P banner events
    SELECT
        event_date, event_name, up_srf_id2_value,
        'O2P' AS campaign,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '202609902P')                                                           AS total_population,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '202609902P'
           AND TRIM(TACTIC_CELL_CD) LIKE '%IM%')                                                   AS mobile_population
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON t.CLNT_NO = g.up_srf_id2_value
       AND t.TACTIC_ID = '202609902P'
       AND TRIM(t.TACTIC_CELL_CD) LIKE '%IM%'
    WHERE g.event_date >= date '2026-04-01'
      AND lower(g.it_item_id) IN ('i_298045')
      AND lower(g.event_name) IN ('view_promotion', 'select_promotion')
    ) AS b
GROUP BY b.event_date, b.campaign, b.total_population, b.mobile_population
ORDER BY b.campaign, b.event_date;


-- OUTPUT 2: YTD cumulative summary (both campaigns)
SELECT
    b.campaign,
    MIN(b.event_date)                                                                              AS date_from_inception,
    MAX(b.event_date)                                                                              AS date_last_event,
    b.total_population,
    b.mobile_population,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'view_promotion'
                        THEN b.up_srf_id2_value END)                                               AS unique_view_users,
    COUNT(DISTINCT CASE WHEN lower(b.event_name) = 'select_promotion'
                        THEN b.up_srf_id2_value END)                                               AS unique_click_users
FROM
    (
    -- CTU
    SELECT
        event_date, event_name, up_srf_id2_value,
        'CTU' AS campaign,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '2026098CTU')                                                           AS total_population,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '2026098CTU'
           AND SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%')                             AS mobile_population
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON t.CLNT_NO = g.up_srf_id2_value
       AND t.TACTIC_ID = '2026098CTU'
       AND SUBSTRING(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MB%'
    WHERE g.event_date >= date '2026-04-01'
      AND lower(g.it_item_id) IN ('i_300102')
      AND lower(g.event_name) IN ('view_promotion', 'select_promotion')

    UNION ALL

    -- O2P
    SELECT
        event_date, event_name, up_srf_id2_value,
        'O2P' AS campaign,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '202609902P')                                                           AS total_population,
        (SELECT COUNT(DISTINCT CLNT_NO) FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
         WHERE TACTIC_ID = '202609902P'
           AND TRIM(TACTIC_CELL_CD) LIKE '%IM%')                                                   AS mobile_population
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON t.CLNT_NO = g.up_srf_id2_value
       AND t.TACTIC_ID = '202609902P'
       AND TRIM(t.TACTIC_CELL_CD) LIKE '%IM%'
    WHERE g.event_date >= date '2026-04-01'
      AND lower(g.it_item_id) IN ('i_298045')
      AND lower(g.event_name) IN ('view_promotion', 'select_promotion')
    ) AS b
GROUP BY b.campaign, b.total_population, b.mobile_population
ORDER BY b.campaign;
