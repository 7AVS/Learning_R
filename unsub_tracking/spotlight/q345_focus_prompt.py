# q345_focus_prompt.py - FOCUSED revision: Q3/Q4/Q5 only. Supersedes prior revision prompts
# for this pass. Paste PROMPT to the notebook LLM. print(PROMPT) to read.

PROMPT = """
# FOCUS PASS - Q3, Q4, Q5 ONLY. Nothing else.

Do not rebuild or touch any other section. Do NOT build an HTML page - that
request is withdrawn; the notebook plots are the deliverable.

## Q3 - Contact Frequency (fix clarity, keep structure)

1. Define the groups ON the chart, first line: "Stayers = clients who did
   not unsubscribe from a Cards campaign (Jan-Apr). Cards unsubs = clients
   who did." Same two groups in ALL four panels.
2. Add one explainer line above the top row: "Each color sums to 100%:
   bars show how each group's members are distributed across email-volume
   buckets. Left panel counts ALL RBC emails they received; right panel
   counts only Cards emails."
3. Print raw counts (group members in bucket / group total) beside each
   percentage.
4. Bottom rate panels: matched y-axis across the pair; recolor rate bars
   neutral grey (red currently means 'unsubs group' above and 'rate'
   below - one color, two meanings).

## Q4 - Who Unsubscribes vs Baseline (rebuild details)

1. REMOVE no_ucp_match from the bars entirely. One footnote line instead:
   "9.2% of clients (958.6K) have no UCP match and are excluded from
   these panels."
2. Primary series = Cards unsubscribers vs stayers (the ratio). The
   any-RBC series is secondary: keep it only if it changes a conclusion;
   otherwise drop it and say so. Every panel self-describing: legend on
   EVERY panel, same x-range (0.6-1.4) on every panel, fixed axis floor.
3. TIBC panel rebuilt to the SAME design as age/tenure (same series, same
   legend, same scale). Do not switch to single-series direction-coloring
   - that is what made bars look 'missing'.
4. Dig for the gold: after the three ratio panels, add ONE deeper cut -
   within Cards, the representation ratio by MNE for the top 5 Cards
   campaigns (which campaigns' unsubscribers skew youngest / newest /
   thinnest product mix?). Pick the single most story-worthy dimension,
   not all three.

## Q5 - PARK IT

Replace all Q5 charts with one markdown cell: "Q5 - Value migration
(anchor Aug 2025 -> Aug 2026): endpoint data available after 2026-08-31.
The cohort, flags and starting positions are landed; the migration view
(tier/segment deltas, value that walked away) is produced by the September
re-run." Nothing else. Do not show anchor-position heatmaps as findings.

## Rules that stand for these three sections
Plain words; every rate carries its raw counts; windows stated in titles;
thin cells muted with rates at 2 decimals max; no causal wording.
"""

if __name__ == "__main__":
    print(PROMPT)
