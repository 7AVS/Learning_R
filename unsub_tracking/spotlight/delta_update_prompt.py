# delta_update_prompt.py - v2 (2026-08-03 pm). The ONLY prompt for the delta section rebuild.
# Supersedes v1 and q345_focus_prompt. print(PROMPT) to read.

PROMPT = """
# DELTA SECTION - REBUILD v2. Narrow scope. No HTML anywhere.

Files: b_delta_summary.csv (long: group x metric x then/now/delta) and
b_before_after_cube.csv (tier_then x seg_then x groups + tier_now/seg_now).
FIRST ACTION: DESCRIBE both files and print the headers. Never guess a column.
Do not touch Q1/Q2/Q3/Q6 or any settled cell. Fix Q4's TIBC panel (orders at
the end). Everything else stays.

## DEFINITIONS BOX - print this ONCE as a markdown cell, verbatim, before
## any delta exhibit. Every exhibit must be readable against it.
- Cohort: 4,783,193 clients mailed by a Cards marketing campaign on/before
  Jun 30 2025. Same clients measured at Jun 30 2025 ("then") and
  Jun 30 2026 ("now"). Delta = now minus then; negative = decline.
- STAYERS: cohort clients who did NOT unsubscribe from a Cards campaign by
  the anchor. Campaign rows (PCQ, PCL, ...) = clients whose FIRST Cards
  unsubscribe was from that campaign. LEAVERS_ALL = all of them together.
- Spend = average monthly CARD spend (3-month window Apr-Jun, summed, / 3).
  Cards products only.
- Profitability = UCP annual profitability ESTIMATE (all RBC products, not
  just cards; not a validated lifetime value).
- Product count = how many of the four product CATEGORIES the client holds
  (Transaction / Investment / Borrowing / Credit) - 0 to 4. A change means
  gaining or losing a CATEGORY, not buying one more product inside it.

## DISPLAY EXCLUSIONS - absolute, no exceptions
- no_ucp_match: NEVER a bar/row/cell in any chart. One footnote per
  exhibit: "excludes N clients with no UCP match (X%)".
- LEAVERS_UNMAPPED and LEAVERS_OTHER: OUT of all displays. One footnote
  with their ns. Displays show: STAYERS, LEAVERS_ALL, and named campaigns.
- 'untiered', 'no_data', 'other_or_none': OUT of matrices/heatmaps; ns in
  a footnote.

## STRUCTURE - two blocks by data availability, each block titled with it
BLOCK 1 - "Measured for ALL cohort clients (UCP)": profitability + product
  count (+ the four category held-%s).
BLOCK 2 - "Cards-only measures": card spend + revolver/transactor.
Groups in BOTH blocks: STAYERS vs LEAVERS_ALL, then PCQ / PCL / PCD (and
FWC if >=500 leavers) as the drill-down rows.

## EXHIBITS
D1 (Block 2) HEADLINE: table + one chart. STAYERS vs LEAVERS_ALL: spend
   then / now / delta / delta% . Units on every number ($). n per group +
   pct_no_dfp_match beside spend.
D2 (Block 1) Same shape for profitability and product count.
   PROFITABILITY CHECK FIRST (required, before charting): a +322%-style
   delta% appeared in v1 - verify against the MEDIAN. Report avg AND
   median; if they diverge badly or 'then' values near zero distort the %,
   LEAD with median and absolute delta, and say why in one line.
D3 PER CAMPAIGN: one table, campaign rows vs the pinned STAYERS baseline
   row - spend delta (Block 2) and prof/product delta (Block 1) side by
   side. Sorted by spend delta. Label plainly: "each row = that campaign's
   unsubscribers".
D4 PRODUCTS DETAIL: held-% of each category (T/I/B/C separate) then vs
   now, STAYERS vs LEAVERS_ALL - one grouped chart, legend mandatory.
D5 BEHAVIOR MIGRATION (Block 2): a clean 3x3 matrix per group (STAYERS,
   LEAVERS_ALL): rows seg_then (Revolver/Transactor/Dormant), cols seg_now,
   cell = % of the group's members who were in that then-segment (rows sum
   to 100%). Excluded categories per the exclusions rule. If a cell rests
   on <500 clients, mute it.
D6 MAYA TEMPLATE (once): heatmap tier_then (High/Mid/Low ONLY) x seg_then
   (R/T/D ONLY): leaver count + leaver rate per cell. Footnote the
   untiered/no_data ns. Label "requested template".

## Q4 TIBC FIX (the one non-delta item)
The cards series EXISTS in a4_profile_cube (leavers_cards_unsub per
combination row). Rebuild the TIBC panel: same design as age/tenure panels
(two series - cards unsubscribers vs stayers - representation ratio),
legend MANDATORY, no_ucp_match excluded per the exclusions rule, combos
written in words. If you believe a needed column is missing: PRINT the
a4 header and STOP with one line saying exactly what is missing - never
ship a legend-less or single-series panel again.

## SANITY GATES (run before showing anything)
- Campaign groups + LEAVERS_OTHER + LEAVERS_UNMAPPED must sum to
  LEAVERS_ALL's n; print the check.
- STAYERS n + LEAVERS_ALL n = 4,783,193; print the check.
- Delta sign spot-check: pick one campaign, verify delta = now - then from
  the raw rows; print it.
Plain words everywhere; % with n; no causal language ("value that walked
away", composition only). No HTML.

## FINAL POLISH (2026-08-03 evening - after full review of the v2 exhibits)
The build is approved in structure. Ten last items, then done:
F1. PRINT the three sanity gates' outputs visibly in the notebook (group
    sums to LEAVERS_ALL; STAYERS+LEAVERS_ALL=4,783,193; one delta
    spot-check). They were ordered; rendered charts are not proof.
F2. D2a claims "avg and median consistent" - SHOW the median row in the
    table. A claim without the number does not ship.
F3. Add the diff-of-diffs line under D2a/D2b: "stayers grew $213 vs
    leavers $179 - gap $34" (and the product-count equivalent). Don't make
    the reader subtract.
F4. Noise labels: PCL's spend delta (-$0.88 on n=3,087) reads "~flat
    (within noise)"; any per-campaign or rate comparison without a CI gets
    a one-line uncertainty note.
F5. D5: print the exclusion SHARES per group next to the matrices ("no
    then-segment: 53% of leavers vs 29% of stayers - concentrated in
    newly-acquired clients"), and make the leaver Dormant->Dormant cell
    readable.
F6. D6: mute cells under 500 clients (n=3 and n=8 are showing full-weight
    colors); note the color scale spans only 0.11-0.25pp (or anchor at 0);
    add one line defining the rate's denominator.
F7. Q6: put the numeric label ON the actual peak point; if not already
    shown, print the peak recomputed excluding the immature months.
F8. Deep-dive section: fix the header (the trend chart is 12-month, the
    volume/rate/table are Jan-Apr - say both); move the legend box off the
    PCQ bar; add a bubble-size key; small unlabeled dots get labels or an
    explicit "unlabeled = remaining small MNEs, see table".
F9. One markdown line atop the delta section: "Observational before/after.
    The stayers baseline controls for market trend, not for who chooses to
    unsubscribe - descriptive, not causal."
F10. Reconcile the RECON print's 10,439,799 clients vs the pipeline's
    10,439,796 (3 rows) - one duckdb check, print the answer.
"""

if __name__ == "__main__":
    print(PROMPT)
