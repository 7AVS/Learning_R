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
# Connector Validation
#
# Testing whether the EDW.cursor() connection can reach both
# Teradata (EDW) tables and EDL (Starburst/Trino) tables
# through the same cursor. If both pass, the VBA vintage CTE
# chain can be written as a single unified query. If EDL fails,
# we split into two queries and merge in pandas.
# ------------------------------------------------------------

# Test 1 — Teradata (EDW): tactic history table, VBA rows
sql_edw = """
SELECT *
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'VBA'
LIMIT 5
"""

try:
    df_edw = edw_query(sql_edw, desc="EDW / Teradata")
    print("PASS — EDW cursor reached Teradata")
    print(df_edw)
except Exception as e:
    print(f"FAIL — EDW cursor could not reach Teradata: {e}")


# Test 2 — EDL: SCOT credit application snapshot (via EDW cursor)
# Fields from original SAS transcription (vba_success_original_sas.sql)
sql_edl = """
SELECT
    creditapplication_borrowers_borrowersrfnumber,
    creditapplication_creditapplicationstatuscode,
    creditapplication_createddatetime,
    creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory IN ('CREDIT_CARD')
LIMIT 5
"""

try:
    df_edl = edw_query(sql_edl, desc="EDL / Starburst")
    print("PASS — EDW cursor reached EDL")
    print(df_edl)
except Exception as e:
    print(f"FAIL — EDW cursor could not reach EDL: {e}")


# ------------------------------------------------------------
# Result
#
# - Both queries returned rows: unified CTE approach works.
#   The VBA vintage chain can be written as a single query
#   joining Teradata and EDL tables through one cursor.
# - EDL query failed: need separate queries — pull Teradata
#   (Casper primary) and EDL (SCOT secondary) independently,
#   then merge on clnt_no in pandas.
# ------------------------------------------------------------
