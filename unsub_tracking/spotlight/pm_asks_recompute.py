# pm_asks_recompute.py — PM asks #2 (attrition) and #3 (population-fixed
# profit), notebook cells. Paste into the unsub_unified notebook AFTER the
# pipeline frames exist. Assumes two Spark frames already loaded:
#   b_ucp   = the b_ucp_v3 output (clnt_no, prof_then, prof_now,
#             held_t/i/b/c_then, held_t/i/b/c_now, prod_cnt_then/now;
#             held_* = -1 means no UCP match at that anchor,
#             prof NULL when unmatched)
#   b_coh   = the Piece B cohort with the leaver flag
#             (clnt_no, cards_unsub_by_anchor 0/1)
# One assertion per cell. Nothing here re-pulls from Teradata.

from pyspark.sql import functions as F

# ---------------------------------------------------------------- Cell 1
# Join + population accounting. THEN population is the fixed base.
j = b_coh.join(b_ucp, "clnt_no", "left")
n_coh = b_coh.count()
n_j = j.count()
print(f"cohort {n_coh:,} -> joined {n_j:,} (must match; LEFT join keeps base fixed)")
assert n_j == n_coh, "join changed the population - STOP"

grp = F.when(F.col("cards_unsub_by_anchor") == 1, "leaver").otherwise("stayer")
j = j.withColumn("grp", grp)

# ---------------------------------------------------------------- Cell 2
# Population ledger per group: matched then / matched now / vanished-now.
# 'vanished_now' (matched then, missing now) is the survivorship leak the
# PM flagged. Show it before any profit number.
ledger = (j.groupBy("grp").agg(
    F.count("*").alias("clients_then_pop"),
    F.sum(F.when(F.col("prof_then").isNotNull(), 1).otherwise(0)).alias("matched_then"),
    F.sum(F.when(F.col("prof_now").isNotNull(), 1).otherwise(0)).alias("matched_now"),
    F.sum(F.when(F.col("prof_then").isNotNull() & F.col("prof_now").isNull(), 1)
           .otherwise(0)).alias("vanished_now"),
))
ledger.show()

# ---------------------------------------------------------------- Cell 3
# PROFIT three ways, per group. (a) survivors-only = the published number
# (replication check). (b) population-fixed, vanished treated as $0 now.
# (c) population-fixed, then-profit of the vanished shown separately so
# the reader sees what left. (b) is the PM's requested basis.
prof = (j.filter(F.col("prof_then").isNotNull()).groupBy("grp").agg(
    F.count("*").alias("n_then_matched"),
    F.avg("prof_then").alias("avg_then_all"),
    F.avg(F.when(F.col("prof_now").isNotNull(), F.col("prof_then"))).alias("avg_then_survivors"),
    F.avg("prof_now").alias("avg_now_survivors"),          # (a) published basis
    F.avg(F.coalesce(F.col("prof_now"), F.lit(0.0))).alias("avg_now_zerofill"),  # (b)
    F.avg(F.when(F.col("prof_now").isNull(), F.col("prof_then"))).alias("avg_then_of_vanished"),  # (c)
))
prof.show()
print("READ: (a) avg_now_survivors vs avg_then_survivors = old claim;"
      " (b) avg_now_zerofill vs avg_then_all = population-fixed delta;"
      " if (b) flips the story, (b) is the number we report.")

# ---------------------------------------------------------------- Cell 4
# ATTRITION (ask #2). Base: held the card category at THEN (held_c_then=1).
# lost_cards  = held_c_now = 0  (still visible in UCP, no card category)
# vanished    = held_c_now = -1 (no UCP match at now - left-bank PROXY,
#               labeled as proxy, not proven bank exit)
att = (j.filter(F.col("held_c_then") == 1).groupBy("grp").agg(
    F.count("*").alias("held_cards_then"),
    F.sum(F.when(F.col("held_c_now") == 0, 1).otherwise(0)).alias("lost_cards_now"),
    F.sum(F.when(F.col("held_c_now") == -1, 1).otherwise(0)).alias("vanished_from_ucp_now"),
))
att.show()
print("READ: rates = lost/held and vanished/held per group; leavers vs"
      " stayers. Selection caveat applies (unsubbers skew younger/4-7yr"
      " tenure) - this is descriptive, not causal.")
