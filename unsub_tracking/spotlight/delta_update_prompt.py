# delta_update_prompt.py - the ONLY prompt for the post-03e notebook update.
# Supersedes q345_focus_prompt for the before/after section. print(PROMPT) to read.

PROMPT = """
# UPDATE PASS - new Piece B data (then->now->delta). Narrow scope.

The pipeline was rebuilt. Two files changed, one is new. Everything else is
byte-compatible - DO NOT touch Q1/Q2/Q3/Q4/Q6 or any settled query. No HTML
anywhere - notebook plots and tables are the deliverable.

## What changed in the data

- Anchors moved: cohort = Cards-marketing-mailed clients on/before
  2025-06-30 (n = 4,783,193); the SAME clients re-measured at 2026-06-30.
  Both dates are closed - the delta is real, no waiting.
- Groups: STAYERS (did not unsub from a cards campaign by the anchor) vs
  cards-campaign unsubscribers; leavers carry the campaign of their FIRST
  cards unsub (per-campaign groups with >=500 leavers; smaller pooled as
  LEAVERS_OTHER; LEAVERS_UNMAPPED is a real visible group - a mapping gap,
  keep it shown).
- NEW FILE b_delta_summary.csv (258 rows): long format, group x metric x
  {then/now/delta}. Metrics include n_clients, spend_monthly_avg/median
  (3-month window / 3 - "avg monthly card spend"), prof (UCP annual
  profitability ESTIMATE - label it so, it is not validated LTV),
  prod_cnt, pct_held_t/i/b/c, seg mix pcts, pct_no_ucp_match,
  pct_no_dfp_match.
- RESHAPED b_before_after_cube.csv (218 rows): tier(then) x seg(then) x
  groups counts, PLUS tier_now and seg_now dims - migration is pivotable.

FIRST ACTION before any query: DESCRIBE both files (duckdb) and read the
actual column names. Never guess a column.

## Build the new before/after section (replaces the old Q5 and its park note)

B1. HEADLINE - one table + one chart: STAYERS vs LEAVERS_ALL, metrics
    spend_monthly_avg, prof, prod_cnt - columns then / now / delta (and
    delta as % of then). Plain title stating the question: "Clients who
    unsubscribed from Cards campaigns - what were they worth then, what
    are they worth now, vs clients who stayed?" Every number with its n;
    show pct_no_dfp_match / pct_no_ucp_match beside spend / prof numbers.
B2. PER CAMPAIGN - same metrics for each campaign group, sorted by spend
    delta. Which campaigns' unsubscribers were worth most and fell
    hardest? LEAVERS_UNMAPPED shown, labeled as a mapping gap.
B3. PRODUCTS - pct_held_t/i/b/c then vs now per group: did leavers thin
    their RBC relationship while stayers held? (T/I/B/C separate, never
    collapsed.)
B4. BEHAVIOR MIGRATION - from b_before_after_cube: seg_then x seg_now
    shares for leavers vs stayers (Revolver->Transactor->Dormant flows).
    One matrix per group, or one diverging comparison - pick the clearer.
B5. THE MAYA ARTIFACT - one heatmap, tier(then) x seg(then), leaver
    counts + rate, produced and labeled "requested template"; not the
    analysis centerpiece. Done once.

## Rules that stand (do not re-derive, do not expand)
- Plain words; % rates with n; NULL buckets ('untiered', no-match) visible,
  never folded in; no causal language - "value that walked away",
  composition only; delta sign convention stated on every table (now minus
  then; negative = decline).
- Sanity anchors: cohort 4,783,193; if a group's ns do not sum across
  campaign groups + LEAVERS_OTHER + LEAVERS_UNMAPPED to LEAVERS_ALL,
  stop and report the gap instead of shipping.
- Fix ONLY cells that break because of the b-cube schema change; touch
  nothing else. No HTML.
"""

if __name__ == "__main__":
    print(PROMPT)
