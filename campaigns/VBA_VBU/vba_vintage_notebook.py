import pandas as pd
import time


def edw_query(sql, desc=""):
    t0 = time.time()
    if desc:
        print(f"  [{desc}] executing...", end=" ", flush=True)
    cursor = EDW.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    cursor.close()
    elapsed = time.time() - t0
    print(f"{len(rows):,} rows in {elapsed:.0f}s")
    return pd.DataFrame(rows, columns=cols)


# ------------------------------------------------------------
# Cell 1 — VBA Vintage Summary
#
# Self-contained SQL — aggregates in SQL, returns small result.
# ------------------------------------------------------------

sql_summary = """
WITH vba AS (
    SELECT DISTINCT
        CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
        E.clnt_no,
        E.treatmt_strt_dt                          AS Treat_Start_DT,
        COALESCE(E.treatmt_end_dt, E.treatmt_strt_dt) AS Treat_End_DT,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2025-11-01'
        AND SUBSTR(E.tactic_id, 8, 3) = 'VBA'
        AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
),
casper AS (
    SELECT
        v.tactic_id,
        v.clnt_no,
        CASE WHEN p.Status = 'A' THEN p.acct_no END AS visa_acct_no,
        CASE WHEN p.Status = 'A' THEN 1 ELSE 0 END  AS visa_app_approved,
        CAST(p.app_rcv_dt AS DATE)                   AS visa_response_dt,
        'Casper' AS response_source
    FROM vba v
    JOIN D3CV12A.appl_fact_dly p
        ON v.clnt_no = p.bus_clnt_no
    WHERE p.app_rcv_dt BETWEEN v.Treat_Start_DT AND v.Treat_End_DT
        AND p.Status IN ('A','D','O')
        AND p.PROD_APPRVD IN ('B','E')
        AND (p.Cell_Code IS NULL OR p.Cell_Code NOT IN ('PATACT','GV0320'))
        AND p.CR_LMT_CHG_IND = 'N'
        AND p.visa_prod_cd NOT IN ('CCL','BXX')
),
scot_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        TRY_CAST(
            creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS BIGINT
        )                                                              AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode = 'FULFILLED' THEN 1 ELSE 0
        END)                                                           AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory = 'CREDIT_CARD'
    GROUP BY
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER),
        TRY_CAST(creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS BIGINT)
),
scot AS (
    SELECT
        v.tactic_id,
        v.clnt_no,
        s.visa_acct_no,
        s.visa_app_approved,
        s.visa_response_dt,
        'Scott' AS response_source
    FROM vba v
    JOIN scot_raw s
        ON v.clnt_no = s.clnt_no
    WHERE s.visa_response_dt BETWEEN v.Treat_Start_DT AND v.Treat_End_DT
),
responses AS (
    SELECT tactic_id, clnt_no, visa_acct_no, visa_app_approved,
           CAST(visa_response_dt AS DATE) AS visa_response_dt, response_source
    FROM casper
    UNION ALL
    SELECT tactic_id, clnt_no, visa_acct_no, visa_app_approved,
           CAST(visa_response_dt AS DATE) AS visa_response_dt, response_source
    FROM scot
),
success AS (
    SELECT *
    FROM (
        SELECT
            tactic_id,
            clnt_no,
            visa_acct_no,
            visa_app_approved,
            visa_response_dt,
            response_source,
            ROW_NUMBER() OVER (
                PARTITION BY tactic_id, clnt_no
                ORDER BY visa_response_dt ASC
            ) AS rn
        FROM responses
        WHERE visa_app_approved = 1
    ) t
    WHERE rn = 1
),
aggregated AS (
    SELECT
        v.tactic_id,
        v.tst_grp_cd,
        MIN(v.Treat_Start_DT) AS treat_start_dt,
        COUNT(DISTINCT v.clnt_no) AS leads,
        COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1 THEN s.clnt_no END) AS successes_any,
        COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1 AND s.response_source = 'Casper' THEN s.clnt_no END) AS successes_casper,
        COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1 AND s.response_source = 'Scott' THEN s.clnt_no END) AS successes_scott,
        ROUND(COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1 THEN s.clnt_no END) * 100.0 / COUNT(DISTINCT v.clnt_no), 2) AS rate_any,
        ROUND(COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1 AND s.response_source = 'Casper' THEN s.clnt_no END) * 100.0 / COUNT(DISTINCT v.clnt_no), 2) AS rate_casper,
        ROUND(COUNT(DISTINCT CASE WHEN s.visa_app_approved = 1 AND s.response_source = 'Scott' THEN s.clnt_no END) * 100.0 / COUNT(DISTINCT v.clnt_no), 2) AS rate_scott
    FROM vba v
    LEFT JOIN responses s ON v.tactic_id = s.tactic_id AND v.clnt_no = s.clnt_no
    GROUP BY v.tactic_id, v.tst_grp_cd
)
SELECT *
FROM aggregated
ORDER BY tactic_id, tst_grp_cd
"""

df_summary = edw_query(sql_summary, desc="VBA Summary")
print(df_summary)


# ------------------------------------------------------------
# Cell 2 — VBA Vintage Curves (0-90 days)
#
# Self-contained SQL — aggregates in SQL, returns small result.
# ------------------------------------------------------------

sql_vintage = """
WITH vba_pop AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
        E.treatmt_strt_dt                          AS Treat_Start_DT,
        COALESCE(E.treatmt_end_dt, E.treatmt_strt_dt) AS Treat_End_DT,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2025-11-01'
        AND SUBSTR(E.tactic_id, 8, 3) = 'VBA'
        AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
),
casper_apps AS (
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        vba.Treat_Start_DT,
        vba.Treat_End_DT,
        vba.tst_grp_cd,
        CASE WHEN p3c.Status = 'A' THEN p3c.acct_no END AS visa_acct_no,
        CASE WHEN p3c.Status = 'A' THEN 1 ELSE 0 END    AS visa_app_approved,
        CAST(p3c.app_rcv_dt AS DATE)                     AS visa_response_dt
    FROM vba_pop vba
    INNER JOIN D3CV12A.appl_fact_dly p3c
        ON vba.clnt_no = p3c.bus_clnt_no
    WHERE p3c.app_rcv_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
        AND p3c.Status IN ('A','D','O')
        AND p3c.PROD_APPRVD IN ('B','E')
        AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code NOT IN ('PATACT','GV0320'))
        AND p3c.CR_LMT_CHG_IND = 'N'
        AND p3c.visa_prod_cd NOT IN ('CCL','BXX')
),
scot_apps_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        MAX(
            CASE
                WHEN creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid IS NOT NULL
                THEN TRY_CAST(
                    creditapplication_borrowers_facilities_facilityborroweroptions_products_creditcarddetails_creditcardaccount_cardholders_tsysaccountid AS BIGINT
                )
            END
        )                                                              AS visa_acct_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS visa_response_dt,
        CASE
            WHEN MAX(CASE WHEN creditapplication_creditapplicationstatuscode = 'FULFILLED' THEN 1 ELSE 0 END) = 1
            THEN 1 ELSE 0
        END                                                            AS visa_app_approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory = 'CREDIT_CARD'
    GROUP BY CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER)
),
scot_apps AS (
    SELECT
        vba.clnt_no,
        vba.tactic_id,
        vba.Treat_Start_DT,
        vba.Treat_End_DT,
        vba.tst_grp_cd,
        scot.visa_acct_no,
        scot.visa_app_approved,
        scot.visa_response_dt
    FROM vba_pop vba
    INNER JOIN scot_apps_raw scot
        ON vba.clnt_no = scot.clnt_no
    WHERE scot.visa_response_dt BETWEEN vba.Treat_Start_DT AND vba.Treat_End_DT
),
earliest_primary_by_client AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        tst_grp_cd,
        MIN(visa_response_dt) AS first_response_dt
    FROM casper_apps
    WHERE visa_app_approved = 1
    GROUP BY clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT, tst_grp_cd
),
earliest_secondary_by_client AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        tst_grp_cd,
        MIN(visa_response_dt) AS first_response_dt
    FROM scot_apps
    WHERE visa_app_approved = 1
    GROUP BY clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT, tst_grp_cd
),
vintages_primary AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        tst_grp_cd,
        CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END AS vintage
    FROM earliest_primary_by_client
    WHERE CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END BETWEEN 0 AND 90
),
vintages_secondary AS (
    SELECT
        clnt_no,
        tactic_id,
        Treat_Start_DT,
        Treat_End_DT,
        tst_grp_cd,
        CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END AS vintage
    FROM earliest_secondary_by_client
    WHERE CASE
            WHEN first_response_dt < Treat_Start_DT THEN 0
            ELSE DATE_DIFF('day', Treat_Start_DT, first_response_dt)
        END BETWEEN 0 AND 90
),
cohort AS (
    SELECT
        SUBSTR(b.tactic_id, 8, 3)                 AS mne,
        b.tst_grp_cd,
        b.Treat_Start_DT,
        b.Treat_End_DT,
        COUNT(DISTINCT b.clnt_no)                  AS leads
    FROM vba_pop b
    GROUP BY SUBSTR(b.tactic_id, 8, 3), b.tst_grp_cd, b.Treat_Start_DT, b.Treat_End_DT
),
successes_primary AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.tst_grp_cd,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS success_daily_primary
    FROM vintages_primary v
    GROUP BY SUBSTR(v.tactic_id, 8, 3), v.tst_grp_cd, v.Treat_Start_DT, v.Treat_End_DT, v.vintage
),
successes_secondary AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.tst_grp_cd,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS success_daily_secondary
    FROM vintages_secondary v
    GROUP BY SUBSTR(v.tactic_id, 8, 3), v.tst_grp_cd, v.Treat_Start_DT, v.Treat_End_DT, v.vintage
),
scaffold AS (
    SELECT
        c.mne,
        c.tst_grp_cd,
        c.Treat_Start_DT,
        c.Treat_End_DT,
        c.leads,
        t.vintage
    FROM cohort c
    CROSS JOIN UNNEST(SEQUENCE(0, 90)) AS t(vintage)
)
SELECT
    s.mne,
    s.tst_grp_cd,
    s.Treat_Start_DT,
    s.Treat_End_DT,
    s.vintage,
    s.leads,
    COALESCE(p.success_daily_primary, 0)           AS success_daily_primary,
    SUM(COALESCE(p.success_daily_primary, 0)) OVER (
        PARTITION BY s.mne, s.tst_grp_cd, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS success_cum_primary,
    COALESCE(sc.success_daily_secondary, 0)        AS success_daily_secondary,
    SUM(COALESCE(sc.success_daily_secondary, 0)) OVER (
        PARTITION BY s.mne, s.tst_grp_cd, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS success_cum_secondary
FROM scaffold s
LEFT JOIN successes_primary p
    ON p.mne = s.mne
    AND p.tst_grp_cd = s.tst_grp_cd
    AND p.Treat_Start_DT = s.Treat_Start_DT
    AND p.Treat_End_DT = s.Treat_End_DT
    AND p.vintage = s.vintage
LEFT JOIN successes_secondary sc
    ON sc.mne = s.mne
    AND sc.tst_grp_cd = s.tst_grp_cd
    AND sc.Treat_Start_DT = s.Treat_Start_DT
    AND sc.Treat_End_DT = s.Treat_End_DT
    AND sc.vintage = s.vintage
ORDER BY s.mne, s.tst_grp_cd, s.Treat_Start_DT, s.Treat_End_DT, s.vintage
"""

df_vintage = edw_query(sql_vintage, desc="VBA Vintage Curves")
print(df_vintage)


# ------------------------------------------------------------
# Cell 3 — Export to CSV
# ------------------------------------------------------------

df_summary.to_csv('vba_vintage_summary.csv', index=False)
print(f"Summary exported: vba_vintage_summary.csv ({len(df_summary)} rows)")

df_vintage.to_csv('vba_vintage_curves.csv', index=False)
print(f"Vintage curves exported: vba_vintage_curves.csv ({len(df_vintage)} rows)")
