-- ============================================================================
-- ENGINE: Starburst/Trino — Trino syntax.
-- Standalone PCL deployment contact-frequency distribution (send-side, the PCL twin of the CRV deployment frequency).
-- Same touch-count definition as Q24 Statement 1: ROW_NUMBER per acct_no ordered by treatmt_strt_dt,
--   floored at DATE '2024-10-01', channel LIKE '%MB%', rolled to client via MAX(pcl_touch_number).
-- No overlap status, no GA4, no CRV — pure PCL histogram.
-- Period: cumulative from 2024-10-01 (full 20-month history), MB channel.
-- Co-applicant accounts EXCLUDED (Section E2 convention).
-- ============================================================================

WITH coapp_accts AS (
    SELECT acct_no
    FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
    WHERE CLNT_NO_A IS NOT NULL
      AND CLNT_NO_A <> CLNT_NO
),
pcl_history AS (   -- full history from Oct-2024; each row gets its cumulative touch number
    SELECT p.clnt_no, p.acct_no,
           ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.treatmt_strt_dt) AS pcl_touch_number
    FROM dl_mr_prod.cards_pli_decision_resp p
    LEFT JOIN coapp_accts x
      ON x.acct_no = p.acct_no
    WHERE p.treatmt_strt_dt >= DATE '2024-10-01'
      AND p.channel LIKE '%MB%'
      AND x.acct_no IS NULL
),
client_freq AS (   -- roll to client: MAX touch number = cumulative contacts over the full window
    SELECT clnt_no,
           MAX(pcl_touch_number) AS pcl_deployment_freq
    FROM pcl_history
    GROUP BY clnt_no
)
SELECT
    pcl_deployment_freq,
    COUNT(*) AS n_clients
FROM client_freq
GROUP BY pcl_deployment_freq
ORDER BY pcl_deployment_freq;
