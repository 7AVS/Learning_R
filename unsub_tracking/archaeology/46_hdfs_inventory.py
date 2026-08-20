# %% [markdown]
# # 46 — HDFS inventory: what we own, what it is, what can go
#
# Walks /user/427966379/ and tabulates every dataset (size, files, last modified),
# then overlays what we KNOW each one is (source pack, reusable-for, delete-safety).
# Run [1], eyeball, then we mark keep/delete together - nothing here deletes anything.

# %% [0] spark + fs
from pyspark.sql import SparkSession
import pandas as pd
spark = SparkSession.builder.getOrCreate()
jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
ROOT = "/user/427966379/"
print("fs ready, root =", ROOT)

# %% [1] walk two levels deep - one row per dataset dir, sized via ContentSummary
rows = []
def _scan(path, depth):
    for st in fs.listStatus(jvm.org.apache.hadoop.fs.Path(path)):
        p = st.getPath().toString().split("/user/427966379/", 1)[-1]
        if st.isDirectory():
            cs = fs.getContentSummary(st.getPath())
            rows.append({
                "path": p,
                "size_gb": round(cs.getLength() / 1e9, 3),
                "n_files": cs.getFileCount(),
                "modified": pd.Timestamp(st.getModificationTime(), unit="ms").strftime("%Y-%m-%d"),
            })
            if depth < 2:
                _scan(st.getPath().toString(), depth + 1)
_scan(ROOT, 1)
inv = pd.DataFrame(rows).sort_values("size_gb", ascending=False).reset_index(drop=True)
print(f"{len(inv)} directories | total {inv.loc[~inv.path.str.contains('/'), 'size_gb'].sum():.1f} GB at top level:")
display(inv)

# %% [2] overlay what we KNOW (from the packs that landed each dataset) - unknowns stay
# 'REVIEW'. Verdicts: KEEP-LIVE (feeds current deck work), KEEP-RESERVOIR (validated
# extracts, expensive to re-pull), ARCHIVE-CANDIDATE (evidence for shipped analyses),
# REVIEW (unidentified - do not delete blind).
KNOWN = {
    "unsub_cpc/unsub_base":        ("client unsub events Jul25-Jun26 (CLNT_NO, unsub_tm, TREATMENT_ID) - reservoir extract [3]-[6]", "KEEP-RESERVOIR: pack 45 unsub side derivable from this"),
    "unsub_cpc/unsub_topup":       ("same schema, complement months from 2024-11 - reservoir [24]-[26]", "KEEP-RESERVOIR: extends unsub coverage back to 2024-11"),
    "unsub_cpc/cpc_pref":          ("CPC 4-switch full history slice (1002/1012/1014/1006) - reservoir [7]", "KEEP-RESERVOIR"),
    "unsub_cpc/q2_recipients":     ("Apr-Jun26 email recipients - reservoir", "ARCHIVE-CANDIDATE: evidence pack shipped"),
    "unsub_cpc/postunsub_sends":   ("sends after unsub, evidence pack", "ARCHIVE-CANDIDATE"),
    "unsub_cpc/gate_mne_agg":      ("gate x mne aggregates, evidence pack", "ARCHIVE-CANDIDATE"),
    "unsub_cpc/blank_mne_sample":  ("DEFAULT-stream sample - identified 2026-07-25", "ARCHIVE-CANDIDATE"),
    "unsub_cpc/cpc_landing_allsw": ("all-switch post-unsub CPC landing - archaeology T1", "ARCHIVE-CANDIDATE"),
    "unsub_cpc/no1002_email_card": ("1002=No email-multiplicity check (T4)", "ARCHIVE-CANDIDATE"),
    "unsub_cpc/unsub_match_diag":  ("unsub match diagnostics (RT3 audit)", "ARCHIVE-CANDIDATE"),
    "unsub_cpc/em_dtl_snapshots":  ("CIDM EM_DTL daily-snapshot archive (44b [13]) - table has NO history upstream", "KEEP-LIVE: irreplaceable once landed"),
    "unsub_cpc/cpc_mthly_1012":    ("CPC_RB_PREF_MTHLY 1012 month slices (44b/45)", "KEEP-LIVE: waterfall anchors"),
    "unsub_cpc/vendor_monthly_mne":("pack 45 earlier-draft monthly bites (superseded by SQL-first rewrite)", "REVIEW: delete if [1]-era partial lands exist"),
    "unsub_cpc/ucp_monthly_flows": ("UCP monthly flag flows (45 [4] earlier draft)", "KEEP-LIVE: expensive Spark loop, reusable"),
    "unsub_cpc/sf_unsub_clients_2024_2026": ("sf_unsubscribe client list (superseded - vendor set now EVENT+MASTER)", "REVIEW: delete-candidate"),
    "unsub_cpc/_meta":             ("landing manifests (sql md5, timestamps) for reservoir datasets", "KEEP-RESERVOIR: provenance"),
}
inv["what_it_is"] = inv["path"].map(lambda p: next((v[0] for k, v in KNOWN.items() if p == k or p.startswith(k + "/")), "unknown"))
inv["verdict"]    = inv["path"].map(lambda p: next((v[1] for k, v in KNOWN.items() if p == k or p.startswith(k + "/")), "REVIEW"))
top = inv[~inv.path.str.contains("/")].copy()
print("Top-level verdict table (drill into any REVIEW row before touching it):")
display(inv.sort_values(["verdict", "size_gb"], ascending=[True, False]))
