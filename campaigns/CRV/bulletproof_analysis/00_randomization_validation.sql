-- CRV randomization check: sticky (account-level) vs per-wave assignment
-- Source: dl_mr_prod.cards_crv_install_decis_resp, offer_start_date >= 2024-10-01

-- Result A: account bucket summary (action_only / control_only / both)
SELECT
    CASE
        WHEN ever_action = 1 AND ever_control = 0 THEN 'action_only'
        WHEN ever_action = 0 AND ever_control = 1 THEN 'control_only'
        WHEN ever_action = 1 AND ever_control = 1 THEN 'both'
        ELSE 'unassigned'
    END AS assignment_bucket,
    COUNT(*) AS account_count,
    SUM(COUNT(*)) OVER () AS total_accounts,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(6,2)) AS pct_share
FROM (
    SELECT
        acct_no,
        MAX(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END) AS ever_action,
        MAX(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END) AS ever_control
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
    GROUP BY acct_no
) acct_summary
GROUP BY 1
ORDER BY account_count DESC;


-- Result B: sample of up to 100 accounts in 'both' bucket — for visual inspection
SELECT
    acct_no,
    COUNT(DISTINCT offer_start_date) AS wave_count,
    SUM(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END) AS action_wave_count,
    SUM(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END) AS control_wave_count
FROM dl_mr_prod.cards_crv_install_decis_resp
WHERE offer_start_date >= DATE '2024-10-01'
GROUP BY acct_no
HAVING MAX(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END) = 1
   AND MAX(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END) = 1
QUALIFY ROW_NUMBER() OVER (ORDER BY wave_count DESC, acct_no) <= 100;
