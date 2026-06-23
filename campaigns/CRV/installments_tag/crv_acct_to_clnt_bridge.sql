-- CRV account -> client number, via the monthly account-reports bridge.
-- ENGINE: Teradata-direct (both tables are Teradata -- do NOT federate).
-- CRV table is acct-grain with NO clnt_no; CR_CRD_RPTS_ACCT is the acct->clnt lookup.
SELECT
    c.acct_no,
    r.clnt_no
FROM dl_mr_prod.cards_crv_install_decis_resp c
JOIN D3CV12A.CR_CRD_RPTS_ACCT r
    ON r.acct_no = CAST(c.acct_no AS DECIMAL(13,0))
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.acct_no ORDER BY r.ME_DT DESC) = 1
;
