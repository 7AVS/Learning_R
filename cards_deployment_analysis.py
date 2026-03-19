# ===================================================================
# Cards Campaign Deployment Analysis
# ===================================================================
# Understanding: how many campaigns, how frequent, channels, size,
# overlap, timing, treatment windows, and targeting patterns.

from pyspark.sql import SparkSession
import pandas as pd
import os
from datetime import datetime

spark = SparkSession.builder \
    .appName("Cards Deployment Analysis") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

DB = "dl_mr_prod"
PCD = f"{DB}.cards_pcd_ongoing_decis_resp"
PLI = f"{DB}.cards_pli_decision_resp"
TPA = f"{DB}.cards_tpa_pcq_decision_resp"

eda_results = {}
HDFS_OUT = "/user/427966379/eda_output"
try:
    os.system(f"hdfs dfs -mkdir -p {HDFS_OUT}")
except:
    pass

print("=== Cards Campaign Deployment Analysis ===\n")


# ===================================================================
# Section 1: Campaign/Tactic Inventory
# ===================================================================
# How many distinct campaigns exist in each table?

# PCD: distinct tactic_id_parent
cursor = EDW.cursor()
cursor.execute(f"SELECT COUNT(DISTINCT tactic_id_parent) FROM {PCD}")
pcd_tactics = cursor.fetchall()[0][0]
cursor.close()
print(f"PCD distinct tactic_id_parent: {pcd_tactics}")

# PCD: top tactics by size
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent, COUNT(*) AS acct_count,
           MIN(response_start) AS earliest_start,
           MAX(response_end) AS latest_end
    FROM {PCD}
    GROUP BY tactic_id_parent
    ORDER BY acct_count DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\nPCD: Tactics by size")
print(df.to_string(index=False))
eda_results["S01_PCD_tactics_by_size"] = df

# PLI: distinct parent_tactic_id
cursor = EDW.cursor()
cursor.execute(f"SELECT COUNT(DISTINCT parent_tactic_id) FROM {PLI}")
pli_tactics = cursor.fetchall()[0][0]
cursor.close()
print(f"\nPLI distinct parent_tactic_id: {pli_tactics}")

# PLI: top tactics by size
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, COUNT(*) AS acct_count,
           MIN(actual_strt_dt) AS earliest_start,
           MAX(treatmt_end_dt) AS latest_end
    FROM {PLI}
    GROUP BY parent_tactic_id
    ORDER BY acct_count DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\nPLI: Tactics by size")
print(df.to_string(index=False))
eda_results["S01_PLI_tactics_by_size"] = df

# TPA: distinct tactic_id
cursor = EDW.cursor()
cursor.execute(f"SELECT COUNT(DISTINCT tactic_id) FROM {TPA}")
tpa_tactics = cursor.fetchall()[0][0]
cursor.close()
print(f"\nTPA distinct tactic_id: {tpa_tactics}")

# TPA: top tactics by size
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id, COUNT(*) AS acct_count,
           MIN(treatmt_start_dt) AS earliest_start,
           MAX(treatmt_end_dt) AS latest_end
    FROM {TPA}
    GROUP BY tactic_id
    ORDER BY acct_count DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\nTPA: Tactics by size")
print(df.to_string(index=False))
eda_results["S01_TPA_tactics_by_size"] = df

# Tactic inventory summary
tactic_summary = pd.DataFrame([
    {"table": "PCD", "distinct_tactics": pcd_tactics},
    {"table": "PLI", "distinct_tactics": pli_tactics},
    {"table": "TPA", "distinct_tactics": tpa_tactics}
])
print("\nTactic Inventory Summary:")
print(tactic_summary.to_string(index=False))
eda_results["S01_tactic_inventory_summary"] = tactic_summary


# ===================================================================
# Section 2: Deployment Frequency — by month
# ===================================================================
# How often are new campaigns launched?

# PCD: accounts per month (by response_start)
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT CAST(YEAR(response_start) AS VARCHAR) || '-' || LPAD(CAST(MONTH(response_start) AS VARCHAR), 2, '0') AS yr_month,
           COUNT(*) AS acct_count,
           COUNT(DISTINCT tactic_id_parent) AS tactics
    FROM {PCD}
    GROUP BY CAST(YEAR(response_start) AS VARCHAR) || '-' || LPAD(CAST(MONTH(response_start) AS VARCHAR), 2, '0')
    ORDER BY yr_month
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Monthly deployment volume ---")
print(df.to_string(index=False))
eda_results["S02_PCD_monthly_volume"] = df

# PLI: accounts per month (by actual_strt_dt)
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT CAST(YEAR(actual_strt_dt) AS VARCHAR) || '-' || LPAD(CAST(MONTH(actual_strt_dt) AS VARCHAR), 2, '0') AS yr_month,
           COUNT(*) AS acct_count,
           COUNT(DISTINCT parent_tactic_id) AS tactics
    FROM {PLI}
    WHERE actual_strt_dt IS NOT NULL
    GROUP BY CAST(YEAR(actual_strt_dt) AS VARCHAR) || '-' || LPAD(CAST(MONTH(actual_strt_dt) AS VARCHAR), 2, '0')
    ORDER BY yr_month
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Monthly deployment volume ---")
print(df.to_string(index=False))
eda_results["S02_PLI_monthly_volume"] = df

# TPA: accounts per month (by treatmt_start_dt)
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT CAST(YEAR(treatmt_start_dt) AS VARCHAR) || '-' || LPAD(CAST(MONTH(treatmt_start_dt) AS VARCHAR), 2, '0') AS yr_month,
           COUNT(*) AS acct_count,
           COUNT(DISTINCT tactic_id) AS tactics
    FROM {TPA}
    WHERE treatmt_start_dt IS NOT NULL
    GROUP BY CAST(YEAR(treatmt_start_dt) AS VARCHAR) || '-' || LPAD(CAST(MONTH(treatmt_start_dt) AS VARCHAR), 2, '0')
    ORDER BY yr_month
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Monthly deployment volume ---")
print(df.to_string(index=False))
eda_results["S02_TPA_monthly_volume"] = df


# ===================================================================
# Section 3: Deployment Size Distribution
# ===================================================================
# How big are individual campaigns?

# PCD: distribution of tactic sizes
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CASE
            WHEN acct_count < 10000 THEN '< 10K'
            WHEN acct_count < 50000 THEN '10K-50K'
            WHEN acct_count < 100000 THEN '50K-100K'
            WHEN acct_count < 500000 THEN '100K-500K'
            WHEN acct_count < 1000000 THEN '500K-1M'
            ELSE '1M+'
        END AS size_bucket,
        COUNT(*) AS num_tactics,
        SUM(acct_count) AS total_accts
    FROM (
        SELECT tactic_id_parent, COUNT(*) AS acct_count
        FROM {PCD}
        GROUP BY tactic_id_parent
    ) t
    GROUP BY CASE
            WHEN acct_count < 10000 THEN '< 10K'
            WHEN acct_count < 50000 THEN '10K-50K'
            WHEN acct_count < 100000 THEN '50K-100K'
            WHEN acct_count < 500000 THEN '100K-500K'
            WHEN acct_count < 1000000 THEN '500K-1M'
            ELSE '1M+'
        END
    ORDER BY MIN(acct_count)
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Tactic size distribution ---")
print(df.to_string(index=False))
eda_results["S03_PCD_tactic_size_dist"] = df

# PLI: distribution of tactic sizes
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CASE
            WHEN acct_count < 10000 THEN '< 10K'
            WHEN acct_count < 50000 THEN '10K-50K'
            WHEN acct_count < 100000 THEN '50K-100K'
            WHEN acct_count < 500000 THEN '100K-500K'
            WHEN acct_count < 1000000 THEN '500K-1M'
            ELSE '1M+'
        END AS size_bucket,
        COUNT(*) AS num_tactics,
        SUM(acct_count) AS total_accts
    FROM (
        SELECT parent_tactic_id, COUNT(*) AS acct_count
        FROM {PLI}
        GROUP BY parent_tactic_id
    ) t
    GROUP BY CASE
            WHEN acct_count < 10000 THEN '< 10K'
            WHEN acct_count < 50000 THEN '10K-50K'
            WHEN acct_count < 100000 THEN '50K-100K'
            WHEN acct_count < 500000 THEN '100K-500K'
            WHEN acct_count < 1000000 THEN '500K-1M'
            ELSE '1M+'
        END
    ORDER BY MIN(acct_count)
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Tactic size distribution ---")
print(df.to_string(index=False))
eda_results["S03_PLI_tactic_size_dist"] = df

# TPA: distribution of tactic sizes
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        CASE
            WHEN acct_count < 10000 THEN '< 10K'
            WHEN acct_count < 50000 THEN '10K-50K'
            WHEN acct_count < 100000 THEN '50K-100K'
            WHEN acct_count < 500000 THEN '100K-500K'
            WHEN acct_count < 1000000 THEN '500K-1M'
            ELSE '1M+'
        END AS size_bucket,
        COUNT(*) AS num_tactics,
        SUM(acct_count) AS total_accts
    FROM (
        SELECT tactic_id, COUNT(*) AS acct_count
        FROM {TPA}
        GROUP BY tactic_id
    ) t
    GROUP BY CASE
            WHEN acct_count < 10000 THEN '< 10K'
            WHEN acct_count < 50000 THEN '10K-50K'
            WHEN acct_count < 100000 THEN '50K-100K'
            WHEN acct_count < 500000 THEN '100K-500K'
            WHEN acct_count < 1000000 THEN '500K-1M'
            ELSE '1M+'
        END
    ORDER BY MIN(acct_count)
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Tactic size distribution ---")
print(df.to_string(index=False))
eda_results["S03_TPA_tactic_size_dist"] = df


# ===================================================================
# Section 4: Channel Mix per Campaign
# ===================================================================
# What channel combinations are deployed?

# PCD: channel combination patterns
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT channels, COUNT(*) AS acct_count,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {PCD}
    GROUP BY channels
    ORDER BY acct_count DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Channel combinations ---")
print(df.head(25).to_string(index=False))
eda_results["S04_PCD_channel_combos"] = df.head(25)

# PCD: channel flag rates per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent,
           COUNT(*) AS size,
           CAST(100.0 * SUM(CASE WHEN channel_deploy_cc = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS cc_pct,
           CAST(100.0 * SUM(CASE WHEN channel_deploy_dm = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS dm_pct,
           CAST(100.0 * SUM(CASE WHEN channel_deploy_do = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS do_pct,
           CAST(100.0 * SUM(CASE WHEN channel_deploy_em = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS em_pct,
           CAST(100.0 * SUM(CASE WHEN channel_deploy_im = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS im_pct,
           CAST(100.0 * SUM(CASE WHEN channel_deploy_iv = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS iv_pct,
           CAST(100.0 * SUM(CASE WHEN channel_deploy_rd = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS rd_pct,
           CAST(100.0 * SUM(CASE WHEN channel_em_reminder = 'Y' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS em_remind_pct
    FROM {PCD}
    GROUP BY tactic_id_parent
    ORDER BY size DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Channel rates per tactic ---")
print(df.to_string(index=False))
eda_results["S04_PCD_channel_per_tactic"] = df

# PLI: channel flag rates per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id,
           COUNT(*) AS size,
           CAST(100.0 * SUM(CAST(channel_cc AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS cc_pct,
           CAST(100.0 * SUM(CAST(channel_dm AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS dm_pct,
           CAST(100.0 * SUM(CAST(channel_do AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS do_pct,
           CAST(100.0 * SUM(CAST(channel_ec AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS ec_pct,
           CAST(100.0 * SUM(CAST(channel_em AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS em_pct,
           CAST(100.0 * SUM(CAST(channel_im AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS im_pct,
           CAST(100.0 * SUM(CAST(channel_in AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS in_pct,
           CAST(100.0 * SUM(CAST(channel_iu AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS iu_pct,
           CAST(100.0 * SUM(CAST(channel_iv AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS iv_pct,
           CAST(100.0 * SUM(CAST(channel_mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS mb_pct,
           CAST(100.0 * SUM(CAST(channel_rd AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS rd_pct
    FROM {PLI}
    GROUP BY parent_tactic_id
    ORDER BY size DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Channel rates per tactic ---")
print(df.to_string(index=False))
eda_results["S04_PLI_channel_per_tactic"] = df

# PLI: channel text field distribution
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT channel, COUNT(*) AS acct_count,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {PLI}
    GROUP BY channel
    ORDER BY acct_count DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Channel text combinations ---")
print(df.head(25).to_string(index=False))
eda_results["S04_PLI_channel_combos"] = df.head(25)

# TPA: channel flag rates per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id,
           COUNT(*) AS size,
           CAST(100.0 * SUM(CAST(chnl_dm AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS dm_pct,
           CAST(100.0 * SUM(CAST(chnl_do AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS do_pct,
           CAST(100.0 * SUM(CAST(chnl_em AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS em_pct,
           CAST(100.0 * SUM(CAST(chnl_im AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS im_pct,
           CAST(100.0 * SUM(CAST(chnl_in AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS in_pct,
           CAST(100.0 * SUM(CAST(chnl_iu AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS iu_pct,
           CAST(100.0 * SUM(CAST(chnl_iv AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS iv_pct,
           CAST(100.0 * SUM(CAST(chnl_mb AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS mb_pct,
           CAST(100.0 * SUM(CAST(chnl_md AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS md_pct,
           CAST(100.0 * SUM(CAST(chnl_rd AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS rd_pct,
           CAST(100.0 * SUM(CAST(chnl_em_reminder AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS em_remind_pct
    FROM {TPA}
    GROUP BY tactic_id
    ORDER BY size DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Channel rates per tactic ---")
print(df.to_string(index=False))
eda_results["S04_TPA_channel_per_tactic"] = df

# TPA: channel text field distribution
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT channel, COUNT(*) AS acct_count,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {TPA}
    GROUP BY channel
    ORDER BY acct_count DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Channel text combinations ---")
print(df.head(25).to_string(index=False))
eda_results["S04_TPA_channel_combos"] = df.head(25)


# ===================================================================
# Section 5: Treatment Windows
# ===================================================================
# How long are treatment periods?

# PCD: treatment window duration
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        MIN(DATE_DIFF('day', response_start, response_end)) AS min_days,
        MAX(DATE_DIFF('day', response_start, response_end)) AS max_days,
        CAST(AVG(DATE_DIFF('day', response_start, response_end)) AS DECIMAL(8,1)) AS avg_days
    FROM {PCD}
    WHERE response_start IS NOT NULL AND response_end IS NOT NULL
""")
r = cursor.fetchall()[0]
cursor.close()
print(f"\n--- PCD: Treatment window duration ---")
print(f"  Min: {r[0]} days, Max: {r[1]} days, Avg: {r[2]} days")
eda_results["S05_PCD_treatment_window"] = pd.DataFrame([{"min_days": r[0], "max_days": r[1], "avg_days": r[2]}])

# PLI: treatment window duration
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        MIN(DATE_DIFF('day', treatmt_strt_dt, treatmt_end_dt)) AS min_days,
        MAX(DATE_DIFF('day', treatmt_strt_dt, treatmt_end_dt)) AS max_days,
        CAST(AVG(DATE_DIFF('day', treatmt_strt_dt, treatmt_end_dt)) AS DECIMAL(8,1)) AS avg_days
    FROM {PLI}
    WHERE treatmt_strt_dt IS NOT NULL AND treatmt_end_dt IS NOT NULL
""")
r = cursor.fetchall()[0]
cursor.close()
print(f"\n--- PLI: Treatment window duration ---")
print(f"  Min: {r[0]} days, Max: {r[1]} days, Avg: {r[2]} days")
eda_results["S05_PLI_treatment_window"] = pd.DataFrame([{"min_days": r[0], "max_days": r[1], "avg_days": r[2]}])

# TPA: treatment window duration
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        MIN(DATE_DIFF('day', treatmt_start_dt, treatmt_end_dt)) AS min_days,
        MAX(DATE_DIFF('day', treatmt_start_dt, treatmt_end_dt)) AS max_days,
        CAST(AVG(DATE_DIFF('day', treatmt_start_dt, treatmt_end_dt)) AS DECIMAL(8,1)) AS avg_days
    FROM {TPA}
    WHERE treatmt_start_dt IS NOT NULL AND treatmt_end_dt IS NOT NULL
""")
r = cursor.fetchall()[0]
cursor.close()
print(f"\n--- TPA: Treatment window duration ---")
print(f"  Min: {r[0]} days, Max: {r[1]} days, Avg: {r[2]} days")
eda_results["S05_TPA_treatment_window"] = pd.DataFrame([{"min_days": r[0], "max_days": r[1], "avg_days": r[2]}])


# ===================================================================
# Section 6: Test vs Control Split per Campaign
# ===================================================================

# PCD: test/control by tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent, act_ctl_seg, COUNT(*) AS cnt
    FROM {PCD}
    GROUP BY tactic_id_parent, act_ctl_seg
    ORDER BY tactic_id_parent, act_ctl_seg
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Test/Control split per tactic ---")
print(df.to_string(index=False))
eda_results["S06_PCD_test_control_per_tactic"] = df

# PLI: test group by tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, tst_grp_cd, COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id, tst_grp_cd
    ORDER BY parent_tactic_id, tst_grp_cd
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Test group per tactic ---")
print(df.to_string(index=False))
eda_results["S06_PLI_test_group_per_tactic"] = df

# TPA: test/control by tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id, act_ctl_seg, COUNT(*) AS cnt
    FROM {TPA}
    GROUP BY tactic_id, act_ctl_seg
    ORDER BY tactic_id, act_ctl_seg
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Test/Control split per tactic ---")
print(df.to_string(index=False))
eda_results["S06_TPA_test_control_per_tactic"] = df


# ===================================================================
# Section 7: Customer Targeting Overlap
# ===================================================================
# Are the same customers being targeted across multiple campaigns?

# PCD: how many tactics per customer?
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactics_per_customer, COUNT(*) AS num_customers
    FROM (
        SELECT acct_no, clnt_no, COUNT(DISTINCT tactic_id_parent) AS tactics_per_customer
        FROM {PCD}
        GROUP BY acct_no, clnt_no
    ) t
    GROUP BY tactics_per_customer
    ORDER BY tactics_per_customer
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Tactics per customer ---")
print(df.to_string(index=False))
eda_results["S07_PCD_tactics_per_customer"] = df

# PLI: how many tactics per customer?
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactics_per_customer, COUNT(*) AS num_customers
    FROM (
        SELECT acct_no, clnt_no, COUNT(DISTINCT parent_tactic_id) AS tactics_per_customer
        FROM {PLI}
        GROUP BY acct_no, clnt_no
    ) t
    GROUP BY tactics_per_customer
    ORDER BY tactics_per_customer
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Tactics per customer ---")
print(df.to_string(index=False))
eda_results["S07_PLI_tactics_per_customer"] = df

# TPA: times_targeted distribution (built-in field)
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT times_targeted, COUNT(*) AS num_rows
    FROM {TPA}
    WHERE times_targeted IS NOT NULL
    GROUP BY times_targeted
    ORDER BY times_targeted
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: times_targeted distribution ---")
print(df.to_string(index=False))
eda_results["S07_TPA_times_targeted_dist"] = df

# TPA: distinct tactics per customer
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactics_per_customer, COUNT(*) AS num_customers
    FROM (
        SELECT clnt_no, COUNT(DISTINCT tactic_id) AS tactics_per_customer
        FROM {TPA}
        GROUP BY clnt_no
    ) t
    GROUP BY tactics_per_customer
    ORDER BY tactics_per_customer
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Distinct tactics per customer ---")
print(df.to_string(index=False))
eda_results["S07_TPA_tactics_per_customer"] = df


# ===================================================================
# Section 8: Cross-Table Customer Overlap
# ===================================================================
# Are the same customers in multiple tables?

cursor = EDW.cursor()
cursor.execute(f"""
    SELECT
        COUNT(DISTINCT CASE WHEN pcd = 1 AND pli = 1 AND tpa = 1 THEN clnt_no END) AS in_all_3,
        COUNT(DISTINCT CASE WHEN pcd = 1 AND pli = 1 AND tpa = 0 THEN clnt_no END) AS pcd_pli_only,
        COUNT(DISTINCT CASE WHEN pcd = 1 AND pli = 0 AND tpa = 1 THEN clnt_no END) AS pcd_tpa_only,
        COUNT(DISTINCT CASE WHEN pcd = 0 AND pli = 1 AND tpa = 1 THEN clnt_no END) AS pli_tpa_only,
        COUNT(DISTINCT CASE WHEN pcd = 1 AND pli = 0 AND tpa = 0 THEN clnt_no END) AS pcd_only,
        COUNT(DISTINCT CASE WHEN pcd = 0 AND pli = 1 AND tpa = 0 THEN clnt_no END) AS pli_only,
        COUNT(DISTINCT CASE WHEN pcd = 0 AND pli = 0 AND tpa = 1 THEN clnt_no END) AS tpa_only,
        COUNT(DISTINCT clnt_no) AS total_unique
    FROM (
        SELECT clnt_no,
               MAX(CASE WHEN src = 'PCD' THEN 1 ELSE 0 END) AS pcd,
               MAX(CASE WHEN src = 'PLI' THEN 1 ELSE 0 END) AS pli,
               MAX(CASE WHEN src = 'TPA' THEN 1 ELSE 0 END) AS tpa
        FROM (
            SELECT DISTINCT clnt_no, 'PCD' AS src FROM {PCD}
            UNION ALL
            SELECT DISTINCT clnt_no, 'PLI' AS src FROM {PLI}
            UNION ALL
            SELECT DISTINCT clnt_no, 'TPA' AS src FROM {TPA}
        ) all_clients
        GROUP BY clnt_no
    ) overlap
""")
r = cursor.fetchall()[0]
cursor.close()
overlap_df = pd.DataFrame([{
    "in_all_3": r[0], "pcd_pli_only": r[1], "pcd_tpa_only": r[2],
    "pli_tpa_only": r[3], "pcd_only": r[4], "pli_only": r[5],
    "tpa_only": r[6], "total_unique": r[7]
}])
print("\n--- Cross-table customer overlap ---")
print(overlap_df.to_string(index=False))
eda_results["S08_cross_table_overlap"] = overlap_df


# ===================================================================
# Section 9: Response Rates by Campaign
# ===================================================================
# Which campaigns convert best?

# PCD: response rate per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent,
           COUNT(*) AS size,
           CAST(100.0 * SUM(CAST(responder_anyproduct AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS resp_any_pct,
           CAST(100.0 * SUM(CAST(responder_targetproduct AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS resp_target_pct,
           CAST(100.0 * SUM(CAST(responder_upgrade_path AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS resp_upgrade_pct
    FROM {PCD}
    GROUP BY tactic_id_parent
    ORDER BY size DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Response rate per tactic ---")
print(df.to_string(index=False))
eda_results["S09_PCD_response_per_tactic"] = df

# PLI: response rate per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id,
           COUNT(*) AS size,
           CAST(100.0 * SUM(CAST(responder_cli AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS resp_cli_pct
    FROM {PLI}
    GROUP BY parent_tactic_id
    ORDER BY size DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Response rate per tactic ---")
print(df.to_string(index=False))
eda_results["S09_PLI_response_per_tactic"] = df

# TPA: application/approval rate per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id,
           COUNT(*) AS size,
           CAST(100.0 * SUM(CAST(app_completed AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS app_completed_pct,
           CAST(100.0 * SUM(CAST(app_approved AS INTEGER)) / COUNT(*) AS DECIMAL(5,2)) AS app_approved_pct
    FROM {TPA}
    GROUP BY tactic_id
    ORDER BY size DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Application/approval rate per tactic ---")
print(df.to_string(index=False))
eda_results["S09_TPA_response_per_tactic"] = df


# ===================================================================
# Section 10: Product Mix per Campaign
# ===================================================================

# PCD: product distribution per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent, product_at_decision, COUNT(*) AS cnt
    FROM {PCD}
    GROUP BY tactic_id_parent, product_at_decision
    ORDER BY tactic_id_parent, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Product mix per tactic ---")
print(df.to_string(index=False))
eda_results["S10_PCD_product_per_tactic"] = df

# PLI: product distribution per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, product_current, COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id, product_current
    ORDER BY parent_tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Product mix per tactic ---")
print(df.to_string(index=False))
eda_results["S10_PLI_product_per_tactic"] = df

# TPA: target product per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id, offer_prod_latest, COUNT(*) AS cnt
    FROM {TPA}
    GROUP BY tactic_id, offer_prod_latest
    ORDER BY tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Target product per tactic ---")
print(df.to_string(index=False))
eda_results["S10_TPA_product_per_tactic"] = df


# ===================================================================
# Section 11: Offer Details
# ===================================================================

# PCD: offer descriptions
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT offer_description, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {PCD}
    GROUP BY offer_description
    ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Offer descriptions ---")
print(df.head(30).to_string(index=False))
eda_results["S11_PCD_offer_descriptions"] = df.head(30)

# PLI: offer descriptions
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT offer_description, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {PLI}
    GROUP BY offer_description
    ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Offer descriptions ---")
print(df.head(30).to_string(index=False))
eda_results["S11_PLI_offer_descriptions"] = df.head(30)

# TPA: offer descriptions
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT offer_description_latest, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {TPA}
    GROUP BY offer_description_latest
    ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Offer descriptions ---")
print(df.head(30).to_string(index=False))
eda_results["S11_TPA_offer_descriptions"] = df.head(30)

# PLI: CLI offer amount by tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id,
           MIN(cli_offer) AS min_offer,
           MAX(cli_offer) AS max_offer,
           CAST(AVG(cli_offer) AS DECIMAL(10,2)) AS avg_offer,
           MIN(limit_increase_amt) AS min_increase,
           MAX(limit_increase_amt) AS max_increase,
           CAST(AVG(limit_increase_amt) AS DECIMAL(10,2)) AS avg_increase
    FROM {PLI}
    WHERE cli_offer IS NOT NULL
    GROUP BY parent_tactic_id
    ORDER BY COUNT(*) DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: CLI offer amounts per tactic ---")
print(df.to_string(index=False))
eda_results["S11_PLI_cli_offer_per_tactic"] = df

# TPA: offer rate and bonus per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id,
           MIN(offer_rate_latest) AS min_rate,
           MAX(offer_rate_latest) AS max_rate,
           CAST(AVG(offer_rate_latest) AS DECIMAL(6,2)) AS avg_rate,
           MIN(offer_bonus_points_latest) AS min_bonus,
           MAX(offer_bonus_points_latest) AS max_bonus,
           CAST(AVG(offer_bonus_points_latest) AS DECIMAL(10,0)) AS avg_bonus
    FROM {TPA}
    WHERE offer_rate_latest IS NOT NULL
    GROUP BY tactic_id
    ORDER BY COUNT(*) DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Offer rate & bonus per tactic ---")
print(df.to_string(index=False))
eda_results["S11_TPA_offer_details_per_tactic"] = df


# ===================================================================
# Section 12: Wave Analysis
# ===================================================================

# PCD: wave distribution
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT wave, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {PCD}
    WHERE wave IS NOT NULL
    GROUP BY wave
    ORDER BY wave
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Wave distribution ---")
print(df.to_string(index=False))
eda_results["S12_PCD_waves"] = df

# PLI: wave distribution
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT wave, COUNT(*) AS cnt,
           CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
    FROM {PLI}
    WHERE wave IS NOT NULL
    GROUP BY wave
    ORDER BY wave
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Wave distribution ---")
print(df.to_string(index=False))
eda_results["S12_PLI_waves"] = df


# ===================================================================
# Section 13: Offer Configuration Details
# ===================================================================

# PCD: distinct offer descriptions with bonus cash/points
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT offer_description,
           MIN(offer_bonus_cash) AS min_cash, MAX(offer_bonus_cash) AS max_cash,
           MIN(offer_bonus_points) AS min_pts, MAX(offer_bonus_points) AS max_pts,
           COUNT(*) AS acct_count
    FROM {PCD}
    WHERE offer_description IS NOT NULL
    GROUP BY offer_description
    ORDER BY acct_count DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Offer configs ---")
print(df.to_string(index=False))
eda_results["S13_PCD_offer_configs"] = df

# PCD: upgrade path combinations (product_at_decision → target_product)
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT product_at_decision, target_product, target_product_name,
           invitation_to_upgrade, COUNT(*) AS cnt
    FROM {PCD}
    GROUP BY product_at_decision, target_product, target_product_name, invitation_to_upgrade
    ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Upgrade path combinations ---")
print(df.to_string(index=False))
eda_results["S13_PCD_upgrade_paths"] = df

# PLI: CLI offer configurations per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, increase_decrease,
           MIN(cli_offer) AS min_offer, MAX(cli_offer) AS max_offer,
           CAST(AVG(cli_offer) AS DECIMAL(10,2)) AS avg_offer,
           COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id, increase_decrease
    ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: CLI offer configs per tactic ---")
print(df.to_string(index=False))
eda_results["S13_PLI_cli_configs"] = df

# TPA: offer rate + bonus + credit limit + fee waiver configs
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT offer_prod_latest, offer_prod_latest_name,
           MIN(offer_rate_latest) AS min_rate, MAX(offer_rate_latest) AS max_rate,
           MIN(offer_bonus_points_latest) AS min_bonus, MAX(offer_bonus_points_latest) AS max_bonus,
           MIN(offer_cr_lmt_latest) AS min_cr_lmt, MAX(offer_cr_lmt_latest) AS max_cr_lmt,
           offer_fee_waiver_latest,
           COUNT(*) AS cnt
    FROM {TPA}
    GROUP BY offer_prod_latest, offer_prod_latest_name, offer_fee_waiver_latest
    ORDER BY cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Offer configs by product ---")
print(df.to_string(index=False))
eda_results["S13_TPA_offer_configs"] = df


# ===================================================================
# Section 14: Strategy & Segmentation Settings
# ===================================================================

# PCD: strategy_seg_cd x act_ctl_seg combinations
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent, strategy_seg_cd, act_ctl_seg, cmpgn_seg,
           COUNT(*) AS cnt
    FROM {PCD}
    GROUP BY tactic_id_parent, strategy_seg_cd, act_ctl_seg, cmpgn_seg
    ORDER BY tactic_id_parent, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Strategy x Test/Control x Campaign Segment ---")
print(df.to_string(index=False))
eda_results["S14_PCD_strategy_settings"] = df

# PLI: tst_grp_cd x strategy_id x action_code combinations per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, tst_grp_cd, strategy_id, action_code,
           COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id, tst_grp_cd, strategy_id, action_code
    ORDER BY parent_tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Test Group x Strategy x Action Code ---")
print(df.to_string(index=False))
eda_results["S14_PLI_strategy_settings"] = df

# PLI: parent_test_group x test_groups_period x dm/em_redeploy
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, parent_test_group, test_groups_period,
           dm_redeploy_test_grp, em_redeploy_test_grp,
           COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id, parent_test_group, test_groups_period,
             dm_redeploy_test_grp, em_redeploy_test_grp
    ORDER BY parent_tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Test hierarchy per tactic ---")
print(df.to_string(index=False))
eda_results["S14_PLI_test_hierarchy"] = df

# TPA: target_seg x strtgy_seg_typ x strtgy_seg_cd x act_ctl_seg
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id, target_seg, strtgy_seg_typ, strtgy_seg_cd,
           act_ctl_seg, cmpgn_seg, tpa_ita,
           COUNT(*) AS cnt
    FROM {TPA}
    GROUP BY tactic_id, target_seg, strtgy_seg_typ, strtgy_seg_cd,
             act_ctl_seg, cmpgn_seg, tpa_ita
    ORDER BY tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Full strategy configuration ---")
print(df.to_string(index=False))
eda_results["S14_TPA_strategy_settings"] = df


# ===================================================================
# Section 15: Channel Configuration per Tactic (detailed)
# ===================================================================

# PCD: full channel config per tactic (which channels turned on/off)
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent, channels,
           channel_deploy_cc, channel_deploy_dm, channel_deploy_do,
           channel_deploy_em, channel_deploy_im, channel_deploy_iv,
           channel_deploy_rd, channel_em_reminder,
           COUNT(*) AS cnt
    FROM {PCD}
    GROUP BY tactic_id_parent, channels,
             channel_deploy_cc, channel_deploy_dm, channel_deploy_do,
             channel_deploy_em, channel_deploy_im, channel_deploy_iv,
             channel_deploy_rd, channel_em_reminder
    ORDER BY tactic_id_parent, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Channel config per tactic ---")
print(df.to_string(index=False))
eda_results["S15_PCD_channel_config"] = df

# PLI: creative IDs per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, dm_creative_id, em_creative_id,
           COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id, dm_creative_id, em_creative_id
    ORDER BY parent_tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Creative IDs per tactic ---")
print(df.to_string(index=False))
eda_results["S15_PLI_creative_ids"] = df

# TPA: full channel config per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id, channel,
           CAST(SUM(CAST(chnl_dm AS INTEGER)) AS INTEGER) AS dm,
           CAST(SUM(CAST(chnl_do AS INTEGER)) AS INTEGER) AS do_ch,
           CAST(SUM(CAST(chnl_em AS INTEGER)) AS INTEGER) AS em,
           CAST(SUM(CAST(chnl_im AS INTEGER)) AS INTEGER) AS im,
           CAST(SUM(CAST(chnl_in AS INTEGER)) AS INTEGER) AS in_ch,
           CAST(SUM(CAST(chnl_iu AS INTEGER)) AS INTEGER) AS iu,
           CAST(SUM(CAST(chnl_iv AS INTEGER)) AS INTEGER) AS iv,
           CAST(SUM(CAST(chnl_mb AS INTEGER)) AS INTEGER) AS mb,
           CAST(SUM(CAST(chnl_md AS INTEGER)) AS INTEGER) AS md,
           CAST(SUM(CAST(chnl_rd AS INTEGER)) AS INTEGER) AS rd,
           CAST(SUM(CAST(chnl_em_reminder AS INTEGER)) AS INTEGER) AS em_rem,
           COUNT(*) AS cnt
    FROM {TPA}
    GROUP BY tactic_id, channel
    ORDER BY tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Channel config per tactic ---")
print(df.to_string(index=False))
eda_results["S15_TPA_channel_config"] = df


# ===================================================================
# Section 16: Treatment Window Configuration
# ===================================================================

# PCD: response windows per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent,
           MIN(response_start) AS start_min, MAX(response_start) AS start_max,
           MIN(response_end) AS end_min, MAX(response_end) AS end_max,
           COUNT(*) AS cnt
    FROM {PCD}
    GROUP BY tactic_id_parent
    ORDER BY start_min
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Treatment windows per tactic ---")
print(df.to_string(index=False))
eda_results["S16_PCD_treatment_windows"] = df

# PLI: treatment windows per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id,
           MIN(actual_strt_dt) AS start_min, MAX(actual_strt_dt) AS start_max,
           MIN(treatmt_strt_dt) AS treatmt_start_min, MAX(treatmt_strt_dt) AS treatmt_start_max,
           MIN(treatmt_end_dt) AS treatmt_end_min, MAX(treatmt_end_dt) AS treatmt_end_max,
           COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id
    ORDER BY start_min
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Treatment windows per tactic ---")
print(df.to_string(index=False))
eda_results["S16_PLI_treatment_windows"] = df

# TPA: treatment windows per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id,
           MIN(treatmt_start_dt) AS start_min, MAX(treatmt_start_dt) AS start_max,
           MIN(treatmt_end_dt) AS end_min, MAX(treatmt_end_dt) AS end_max,
           MIN(report_dt) AS report_min, MAX(report_dt) AS report_max,
           COUNT(*) AS cnt
    FROM {TPA}
    GROUP BY tactic_id
    ORDER BY start_min
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Treatment windows per tactic ---")
print(df.to_string(index=False))
eda_results["S16_TPA_treatment_windows"] = df


# ===================================================================
# Section 17: Customer Targeting Profile at Decision
# ===================================================================
# What customer attributes are used in targeting decisions?

# PCD: product x relationship_mgmt x credit_phase per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id_parent, product_at_decision, product_grouping_at_decision,
           relationship_mgmt, COUNT(*) AS cnt
    FROM {PCD}
    GROUP BY tactic_id_parent, product_at_decision, product_grouping_at_decision, relationship_mgmt
    ORDER BY tactic_id_parent, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PCD: Customer profile per tactic ---")
print(df.to_string(index=False))
eda_results["S17_PCD_customer_profile"] = df

# PLI: product x usage_behaviour x spid_label per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT parent_tactic_id, product_current, product_grouping_current,
           usage_behaviour, spid_label, COUNT(*) AS cnt
    FROM {PLI}
    GROUP BY parent_tactic_id, product_current, product_grouping_current,
             usage_behaviour, spid_label
    ORDER BY parent_tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- PLI: Customer profile per tactic ---")
print(df.to_string(index=False))
eda_results["S17_PLI_customer_profile"] = df

# TPA: target_seg x offer_prod_latest x cmpgn_seg per tactic
cursor = EDW.cursor()
cursor.execute(f"""
    SELECT tactic_id, target_seg, offer_prod_latest, offer_prod_latest_group,
           cmpgn_seg, COUNT(*) AS cnt
    FROM {TPA}
    GROUP BY tactic_id, target_seg, offer_prod_latest, offer_prod_latest_group, cmpgn_seg
    ORDER BY tactic_id, cnt DESC
""")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
cursor.close()
df = pd.DataFrame(rows, columns=cols)
print("\n--- TPA: Customer profile per tactic ---")
print(df.to_string(index=False))
eda_results["S17_TPA_customer_profile"] = df


# ===================================================================
# Section 18: Distinct Configuration Summary
# ===================================================================
# How many unique settings exist for each config dimension?

config_counts = []

for label, tbl, configs in [
    ("PCD", PCD, [
        ("tactic_id_parent", "tactic_id_parent"),
        ("strategy_seg_cd", "strategy_seg_cd"),
        ("act_ctl_seg", "act_ctl_seg"),
        ("cmpgn_seg", "cmpgn_seg"),
        ("channels", "channels"),
        ("product_at_decision", "product_at_decision"),
        ("target_product", "target_product"),
        ("offer_description", "offer_description"),
        ("report_groups_period", "report_groups_period"),
        ("test_groups_period", "test_groups_period"),
        ("wave", "wave"),
    ]),
    ("PLI", PLI, [
        ("parent_tactic_id", "parent_tactic_id"),
        ("tst_grp_cd", "tst_grp_cd"),
        ("strategy_id", "strategy_id"),
        ("action_code", "action_code"),
        ("rpt_grp_cd", "rpt_grp_cd"),
        ("channel", "channel"),
        ("product_current", "product_current"),
        ("offer_description", "offer_description"),
        ("wave", "wave"),
        ("wave2", "wave2"),
        ("parent_test_group", "parent_test_group"),
        ("test_groups_period", "test_groups_period"),
        ("dm_creative_id", "dm_creative_id"),
        ("em_creative_id", "em_creative_id"),
    ]),
    ("TPA", TPA, [
        ("tactic_id", "tactic_id"),
        ("target_seg", "target_seg"),
        ("strtgy_seg_typ", "strtgy_seg_typ"),
        ("strtgy_seg_cd", "strtgy_seg_cd"),
        ("act_ctl_seg", "act_ctl_seg"),
        ("cmpgn_seg", "cmpgn_seg"),
        ("tpa_ita", "tpa_ita"),
        ("channel", "channel"),
        ("offer_prod_latest", "offer_prod_latest"),
        ("offer_description_latest", "offer_description_latest"),
        ("test_group_latest", "test_group_latest"),
    ]),
]:
    for config_name, col in configs:
        cursor = EDW.cursor()
        cursor.execute(f"SELECT COUNT(DISTINCT {col}) FROM {tbl}")
        distinct_count = cursor.fetchall()[0][0]
        cursor.close()
        config_counts.append({"table": label, "config_field": config_name, "distinct_values": distinct_count})

config_df = pd.DataFrame(config_counts)
print("\n--- Configuration Dimensions: Distinct Value Counts ---")
print(config_df.to_string(index=False))
eda_results["S18_config_distinct_counts"] = config_df


print("\n=== Deployment Analysis Complete ===")


# ===================================================================
# Save Results to HDFS (HTML)
# ===================================================================

print("\n=== Saving Deployment Analysis Results ===")

# Section labels for grouping
SECTION_MAP = {
    "S01": "Section 1: Campaign/Tactic Inventory",
    "S02": "Section 2: Deployment Frequency (Monthly)",
    "S03": "Section 3: Deployment Size Distribution",
    "S04": "Section 4: Channel Mix per Campaign",
    "S05": "Section 5: Treatment Windows",
    "S06": "Section 6: Test vs Control Split",
    "S07": "Section 7: Customer Targeting Overlap",
    "S08": "Section 8: Cross-Table Customer Overlap",
    "S09": "Section 9: Response Rates by Campaign",
    "S10": "Section 10: Product Mix per Campaign",
    "S11": "Section 11: Offer Details",
    "S12": "Section 12: Wave Analysis",
    "S13": "Section 13: Offer Configuration Details",
    "S14": "Section 14: Strategy & Segmentation Settings",
    "S15": "Section 15: Channel Configuration per Tactic (detailed)",
    "S16": "Section 16: Treatment Window Configuration",
    "S17": "Section 17: Customer Targeting Profile at Decision",
    "S18": "Section 18: Distinct Configuration Summary",
}

html_parts = []
html_parts.append("""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Cards Deployment Analysis</title>
<style>
body { font-family: Consolas, monospace; font-size: 12px; margin: 20px; background: #1e1e1e; color: #d4d4d4; }
h1 { color: #569cd6; border-bottom: 2px solid #569cd6; padding-bottom: 8px; }
h2 { color: #4ec9b0; margin-top: 30px; border-bottom: 1px solid #4ec9b0; padding-bottom: 4px; }
h3 { color: #ce9178; margin-top: 15px; }
table { border-collapse: collapse; margin: 10px 0 20px 0; width: auto; }
th { background: #264f78; color: #d4d4d4; padding: 4px 12px; text-align: left; border: 1px solid #3c3c3c; font-size: 11px; }
td { padding: 3px 12px; border: 1px solid #3c3c3c; font-size: 11px; white-space: nowrap; }
tr:nth-child(even) { background: #2d2d2d; }
tr:nth-child(odd) { background: #1e1e1e; }
tr:hover { background: #264f78; }
.section { margin-bottom: 40px; }
.timestamp { color: #808080; font-size: 10px; }
</style></head><body>
""")

html_parts.append(f"<h1>Cards Campaign Deployment Analysis</h1>")
html_parts.append(f"<p class='timestamp'>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
html_parts.append(f"<p>Tables: PCD = {PCD} | PLI = {PLI} | TPA = {TPA}</p>")

# Group eda_results by section prefix
for section_key in sorted(SECTION_MAP.keys()):
    section_label = SECTION_MAP[section_key]
    matching = {k: v for k, v in sorted(eda_results.items()) if k.startswith(section_key)}
    if matching:
        html_parts.append(f"<div class='section'><h2>{section_label}</h2>")
        for name, df in matching.items():
            # Clean name: strip section prefix and underscores
            clean_name = name[4:].replace("_", " ")
            html_parts.append(f"<h3>{clean_name}</h3>")
            html_parts.append(df.to_html(index=False, border=0, classes="eda-table"))
        html_parts.append("</div>")

html_parts.append("</body></html>")
html_report = "\n".join(html_parts)

# Save locally then push to HDFS
local_html = "/tmp/cards_deployment_analysis.html"
html_hdfs_path = f"{HDFS_OUT}/cards_deployment_analysis.html"
try:
    with open(local_html, "w") as f:
        f.write(html_report)
    os.system(f"hdfs dfs -put -f {local_html} {html_hdfs_path}")
    print(f"\n  HTML Report saved: {html_hdfs_path}")
    print(f"  Open in HUE File Browser to view")
except Exception as e:
    print(f"  FAILED to save HTML: {e}")

try:
    print(f"  Local copy: {local_html}")
except:
    pass

print(f"\n=== Deployment Analysis Complete -- {len(eda_results)} tables saved ===")
