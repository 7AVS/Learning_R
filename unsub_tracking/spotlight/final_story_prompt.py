# final_story_prompt.py - THE final delta-section rebuild. REPLACES all prior delta
# exhibits (D1-D6 + matched-check cells) in one pass. Supersedes delta_update_prompt
# and matched_check_prompt entirely. No pipeline changes, no new data - CSVs only.

PROMPT = """
# FINAL DELTA SECTION - one clean replacement. No HTML. CSVs only.

DELETE the existing delta exhibits (D1-D6 and the matched-check cells) and
build this final set in their place. Do not append another version - this
REPLACES. All numbers from b_delta_summary.csv and b_before_after_cube.csv
(DESCRIBE first; never guess a column). Keep the definitions box (update its
group lines if needed). Numbers must reconcile with the earlier Q1-Q6
sections (same files, same ns) - if any number disagrees with a prior
exhibit, STOP and report it instead of shipping.

THE STORY THESE EXHIBITS TELL (print as the section's opening markdown):
"Clients who unsubscribe from Cards emails are disproportionately tenured,
high-spend card users. After unsubscribing, their spend, product holdings
and profitability HOLD - matched on starting spend tier, they decline no
faster than stayers. What the bank loses is the CHANNEL: email access to
some of its best card spenders - mostly lost at the first or second email.
Descriptive, not causal."

## E1 - WHO THEY ARE (composition, then-state)
From the cube: tier_then distribution of cards unsubscribers vs stayers
(% of each group in High/Mid/Low; untiered footnoted). One paired-bar
chart + ns. Title: "Unsubscribers skew toward the high-spend tier".

## E2 - WHAT HOLDS (the honest before/after)
One compact table, STAYERS vs LEAVERS_ALL: spend_monthly_avg, prof,
prod_cnt - then / now / delta. NO growth-gap framing. Beside it, the
MATCHED-TIER table (tier_then x group x % stayed in tier, from the cube:
High 77.6% leavers vs 76.1% stayers; Mid 54.9% vs 54.5% - recompute, do
not hardcode) with the one-line conclusion: "Matched on starting tier,
leavers decline no faster than stayers - the aggregate gap was
composition. No post-unsub deterioration is detectable." This kill-test
exhibit STAYS IN - it is the credibility anchor of the section.

## E3 - WHAT IS LOST (the channel)
One exhibit quantifying lost access: number of cards unsubscribers by
tier_then (counts - e.g. how many High-tier clients went email-dark),
plus the two cross-references as printed lines with their numbers:
"48% of cards unsubscribers had received only 1-2 cards emails (first
contact)" and the enterprise repeat-unsub figure ("~37K clients had to
unsubscribe more than once - suppression gap", labeled enterprise-wide).
Title: "What we lose is the ability to talk to them".

## E4 - BEHAVIOR MIGRATION, MATCHED (settles the de-revolve claim)
The cube has tier_then x seg_then x seg_now: recompute revolver->
transactor migration WITHIN the same tier_then, leavers vs stayers.
- If leaver-revolvers still de-revolve more within tier: keep the finding
  with the matched numbers.
- If the difference disappears: replace with one line "revolver migration
  matches stayers once tier-matched - no differential" and drop the chart.
Print whichever conclusion the numbers give.

## E5 - MAYA TEMPLATE (unchanged)
Keep the existing D6 heatmap exactly as approved (H/M/L x R/T/D, muted
thin cells). Do not rebuild.

## RULES
Plain words; % with n everywhere; exclusions as footnotes (no_ucp_match,
untiered, OTHER/UNMAPPED never plotted); no causal language; delta = now
minus then stated once; every exhibit self-describing (legend + units).
Sanity prints before exhibits: STAYERS + LEAVERS_ALL = 4,783,193; campaign
groups sum to LEAVERS_ALL. No HTML.
"""

if __name__ == "__main__":
    print(PROMPT)
