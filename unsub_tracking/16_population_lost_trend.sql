-- =============================================================================
-- Population lost trend — THE plain read for Power Pack outcome #2
-- =============================================================================
-- S0 prints what the output means (keep it in the screenshot).
-- S1 = one row per month, one column per tracked MNE. ~31 rows.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- S0: readme — what S1 is and its constraints (screenshot together with S1)
SELECT 1 AS ord, CAST('QUESTION' AS VARCHAR(20)) AS item,
       CAST('How many clients do we lose email access to each month, and which tracked campaign triggered it?' AS VARCHAR(200)) AS detail
UNION ALL SELECT 2, 'POPULATION',  'First-EVER email unsub per client since 2024-01-01 (deduped: 1 row per client, earliest unsub). Vendor feedback disposition_cd=4.'
UNION ALL SELECT 3, 'ATTRIBUTION', 'Campaign = the send the client unsubscribed FROM (recorded on the unsub row, not inferred). MNE = SUBSTR(TREATMENT_ID,8,3).'
UNION ALL SELECT 4, 'TRACKED',     'Cards: PCQ PCL PCD AUH CLI MVP CRV CTU O2P | Payments: VDT VUI VUT VDA VAW VCN | Pers.Loans: RCU RCL. other_mne = all remaining NBA campaigns.'
UNION ALL SELECT 5, 'READ AS',     'clients_lost_all = total RBC email first-unsubs that month. tracked_total = sum of the 17 MNE columns. MNE col = 0 means no data (VAW/VCN presence unconfirmed).'
ORDER BY ord;


-- S1: monthly trend, per-MNE columns
WITH first_unsub AS (
    SELECT
        m.CLNT_NO,
        e.TREATMENT_ID,
        e.disposition_dt_tm,
        ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                           ORDER BY e.disposition_dt_tm ASC) AS rn
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
),
dedup AS (
    SELECT SUBSTR(TREATMENT_ID, 8, 3) AS mne, disposition_dt_tm
    FROM first_unsub
    WHERE rn = 1
)
SELECT
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm)   AS unsub_month_yyyymm,
    CAST(COUNT(*) AS BIGINT)                    AS clients_lost_all,
    CAST(SUM(CASE WHEN mne IN ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P',
                               'VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')
                  THEN 1 ELSE 0 END) AS BIGINT) AS tracked_total,
    CAST(SUM(CASE WHEN mne = 'PCQ' THEN 1 ELSE 0 END) AS BIGINT) AS pcq,
    CAST(SUM(CASE WHEN mne = 'PCL' THEN 1 ELSE 0 END) AS BIGINT) AS pcl,
    CAST(SUM(CASE WHEN mne = 'PCD' THEN 1 ELSE 0 END) AS BIGINT) AS pcd,
    CAST(SUM(CASE WHEN mne = 'AUH' THEN 1 ELSE 0 END) AS BIGINT) AS auh,
    CAST(SUM(CASE WHEN mne = 'CLI' THEN 1 ELSE 0 END) AS BIGINT) AS cli,
    CAST(SUM(CASE WHEN mne = 'MVP' THEN 1 ELSE 0 END) AS BIGINT) AS mvp,
    CAST(SUM(CASE WHEN mne = 'CRV' THEN 1 ELSE 0 END) AS BIGINT) AS crv,
    CAST(SUM(CASE WHEN mne = 'CTU' THEN 1 ELSE 0 END) AS BIGINT) AS ctu,
    CAST(SUM(CASE WHEN mne = 'O2P' THEN 1 ELSE 0 END) AS BIGINT) AS o2p,
    CAST(SUM(CASE WHEN mne = 'VDT' THEN 1 ELSE 0 END) AS BIGINT) AS vdt,
    CAST(SUM(CASE WHEN mne = 'VUI' THEN 1 ELSE 0 END) AS BIGINT) AS vui,
    CAST(SUM(CASE WHEN mne = 'VUT' THEN 1 ELSE 0 END) AS BIGINT) AS vut,
    CAST(SUM(CASE WHEN mne = 'VDA' THEN 1 ELSE 0 END) AS BIGINT) AS vda,
    CAST(SUM(CASE WHEN mne = 'VAW' THEN 1 ELSE 0 END) AS BIGINT) AS vaw,
    CAST(SUM(CASE WHEN mne = 'VCN' THEN 1 ELSE 0 END) AS BIGINT) AS vcn,
    CAST(SUM(CASE WHEN mne = 'RCU' THEN 1 ELSE 0 END) AS BIGINT) AS rcu,
    CAST(SUM(CASE WHEN mne = 'RCL' THEN 1 ELSE 0 END) AS BIGINT) AS rcl,
    CAST(SUM(CASE WHEN mne NOT IN ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P',
                                   'VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')
                  THEN 1 ELSE 0 END) AS BIGINT) AS other_mne
FROM dedup
GROUP BY 1
ORDER BY 1;
