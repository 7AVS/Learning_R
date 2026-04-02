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


# ------------------------------------------------------------
# Cell 2 — Run VBA Vintage Queries
#
# Reads vba_vintage_curves_trino.sql and runs both queries:
#   Query 1 = Summary (leads, responders, rates by fiscal qtr)
#   Query 2 = Vintage curves (0-90 day daily + cumulative)
# ------------------------------------------------------------

import os

sql_path = os.path.join(os.path.dirname(__file__), 'vba_vintage_curves_trino.sql')
with open(sql_path) as f:
    raw_sql = f.read()

# Split on marker between Query 1 and Query 2
parts = raw_sql.split('-- @@SPLIT@@')
# Strip comments-only blocks and find actual queries (contain WITH or SELECT)
queries = []
for part in parts:
    stripped = '\n'.join(line for line in part.strip().split('\n')
                        if not line.strip().startswith('--') and line.strip())
    if stripped.strip().rstrip(';'):
        queries.append(part.strip().rstrip(';'))

print(f"Found {len(queries)} queries in SQL file")

# Query 1 — Summary
try:
    df_summary = edw_query(queries[0], desc="VBA Summary")
    print(df_summary)
except Exception as e:
    print(f"Query 1 (Summary) failed: {e}")

# Query 2 — Vintage Curves
try:
    df_vintage = edw_query(queries[1], desc="VBA Vintage Curves")
    print(df_vintage)
except Exception as e:
    print(f"Query 2 (Vintage Curves) failed: {e}")


# ------------------------------------------------------------
# Cell 3 — Export to CSV
# ------------------------------------------------------------

try:
    df_summary.to_csv(os.path.join(os.path.dirname(__file__), 'vba_vintage_summary.csv'), index=False)
    print(f"Summary exported: vba_vintage_summary.csv ({len(df_summary)} rows)")
except Exception as e:
    print(f"Summary CSV export failed: {e}")

try:
    df_vintage.to_csv(os.path.join(os.path.dirname(__file__), 'vba_vintage_curves.csv'), index=False)
    print(f"Vintage curves exported: vba_vintage_curves.csv ({len(df_vintage)} rows)")
except Exception as e:
    print(f"Vintage curves CSV export failed: {e}")
