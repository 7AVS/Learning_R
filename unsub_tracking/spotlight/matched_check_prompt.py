# matched_check_prompt.py - the mean-reversion check ordered by the marketing-director red team
# (2026-08-03). Run AFTER the delta exhibits. print(PROMPT) to read.

PROMPT = """
# MATCHED-BAND CHECK - can the "growth stops" claim survive mean reversion?

Context: the red team's strongest objection to the delta story: "you selected
a group BECAUSE it spends more, then watched it drift down - that's mean
reversion, not an unsub effect." This check answers it. It needs NO new
pull - the client-grain panel is landed on HDFS (b_panel: one row per cohort
client with spend_3mo_then, spend_3mo_now, the leaver flag, cards_unsub_mne).

## The check

1. Read the landed parquet - EXACT paths and schemas (verified from the
   pipeline code, build 03e; still run printSchema() first and STOP if it
   disagrees):
   - hdfs:///user/427966379/unsub_unified/b_cohort_v3/bite_?  (the bite_?
     glob, NEVER bite_* - sidecar marker files pollute wider globs)
     columns: clnt_no (long), any_unsub_by_anchor (int),
     cards_unsub_by_anchor (int), cards_ex_fwc_unsub_by_anchor (int),
     cards_unsub_mne (string, NULL for stayers)
   - hdfs:///user/427966379/unsub_unified/b_dfp_v3/bite_?
     columns: clnt_no (long), n_accts_total (long),
     spend_3mo_then (double, NULLable), spend_3mo_now (double, NULLable)
   Join on clnt_no (left join cohort->dfp). Leaver = cards_unsub_by_anchor
   = 1; stayer = 0. Avg monthly spend = spend_3mo_then/3 and _now/3.
   Sanity before anything: cohort rows must be 4,783,193 (today's build) -
   any other number means wrong/stale data, STOP.
2. Restrict to clients with spend_3mo_then/3 (avg monthly spend THEN) in a
   band, and within the band compare CARDS UNSUBSCRIBERS vs STAYERS:
   - avg monthly spend then (should be ~equal by construction - print it)
   - avg monthly spend now
   - delta and delta%
   - n for each group
3. Bands: $1,000-2,000 and $2,000-3,000 per month (add $500-1,000 and
   $3,000+ if ns allow; skip any cell under 300 clients and say so).
4. Output ONE table: band x group x {n, then, now, delta, delta_pct}.
5. Also print the same for a MEDIAN-based version of the deltas.

## How to read it (print this interpretation logic with the result)
- If stayers in the SAME band grew while matched unsubscribers stalled or
  fell -> the growth gap is NOT mean reversion; the unsub-marks-stalling
  story stands, now matched on starting spend.
- If matched stayers also stalled/fell -> the original gap was mean
  reversion + composition; the deck keeps the director's honest version
  (first-contact discipline, suppression fix, blast guardrail) and drops
  any spend-stall claim.

## Rules
- Descriptive language only; no causal wording either way.
- Print ns everywhere; NULL spend rows excluded and counted.
- Do not touch any other exhibit. No HTML.

## Optional second cut (only if the first is done and clean)
Same matched comparison restricted to PCL+PCD unsubscribers vs matched
stayers - the whale story specifically.
"""

if __name__ == "__main__":
    print(PROMPT)
