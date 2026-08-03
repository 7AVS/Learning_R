# plot_revision_prompt.py - paste this PROMPT to the work-env LLM AFTER the analysis notebook
# exists. It is the consolidated red-team review of every plot + the exact fixes, from the
# repo-side review of 2026-08-03. Same .py-wrapped-markdown pattern as the other prompts.

PROMPT = """
# REVISION ORDERS - every plot reviewed, fix list per chart

Your analysis was reviewed chart by chart against the pipeline's actual
definitions. The computations were correct; the fixes below are semantics,
labeling and chart construction. Apply ALL of them, then rebuild the plots
BEFORE any HTML page is written. Nothing here requires re-running the
pipeline - the cubes are correct as landed.

## GLOBAL CONVENTIONS (apply to every chart, no exceptions)

G1. **Rate format: percent, not per-1,000.** unsub rate % = unsubs /
    senders * 100, shown with 2 decimals ("0.59%"). Kill every "per 1,000"
    axis and label. Denominator rule: 'senders' (unique clients mailed)
    exists ONLY in a2. The monthly curve's 'sends' are delivered EVENTS -
    a monthly ratio must be labeled "per delivered email", or keep the
    curve as volumes. NEVER write unsubs/(unsubs+senders) - senders
    already includes the unsubscribers; the formula is unsubs/senders.
G2. **Every rate shows its n** - the denominator count, on or beside the
    bar/cell/point. A rate with no n does not ship.
G3. **Small-base guard:** any rate whose denominator < 10,000 gets a
    "small base" badge or is dropped from rankings (POT, CEC, WNH, MET,
    the Retain group). A tiny audience makes a loud fake rate.
G4. **Two Cards definitions exist - never mix them in one number:**
    - "Cards pod (12 MNEs)": PCQ PCL PCD AUH CLI CRV VBA VBU CRO CEC VIF
      MET - matches the pipeline's cards_unsub flag; deduped total 11,252.
    - "Cards LOB (Andre's mapping, incl FWC/FIFA etc.)": per-MNE sums are
      UPPER BOUNDS (multi-list overlap ~34%); no deduped total exists yet.
    Every chart title/legend says which definition it uses.
G5. **'leaver' in the b cube = client who unsubscribed from ANY email list
    on/before 2025-08-31.** NOT business attrition, NOT account closure.
    True attrition only appears at t12 (September) as 'untiered'.
G6. **No causal language.** "Driven by", "lethal", "reaction", "irritates"
    -> replace with "coincides with", "concentrated in", "peaks at".
    Piece B is "value that walked away" - composition, never cost-caused.
G7. **The 9.2% no_ucp_match bucket is shown wherever UCP dims appear** -
    its own bar/row, never folded into 0 or silently dropped.
G8. Provenance columns are ignored in analysis (audit stamps only).

## PER-CHART ORDERS

### Q1 - share by MNE (table + bar)
KEEP: structure, the dashed deduped-reference line, the upper-bound note.
FIX: (a) apply G4 - the LOB tags are Andre's mapping, say so in the
subtitle; (b) the "Cards deduped = 11,252 / 10.3%" callout must appear on
EVERY artifact built from this table, not just the first one; (c) add a
one-line footer: "per-MNE rows sum to 146,706 = 134% of the deduped
109,431 - multi-list overlap is why sums are upper bounds."

### Q2 - volume vs rate panels
KEEP: the two-panel volume/intensity split and the "also top volume" tags.
FIX: (a) G1 percent format; (b) G2 sender counts labeled per bar;
(c) G3 - any <10k-sender MNE out of the rate ranking or badged;
(d) callout: PCQ is the only Cards-pod campaign in BOTH top-10s.

### Q3 - contact frequency (two panels)
KEEP: the finding pair (load gradient + first-touch peak) - it is real.
FIX: (a) DELETE the "x10 for scale" trick - two separate small-multiple
panels, each with its own y-axis; never a scaled series on a shared axis;
(b) retitle without causal words: left "Heavier-mailed clients unsub less
(survivorship: engaged clients accumulate mail)", right "Cards unsubs
concentrate at first contact (1-2 emails)"; the survivorship caveat
applies to BOTH panels - selection, not treatment effect; (c) G2: print
clients_total per bucket under each x label.

### Q4 - profile (age / tenure / TIBC)
KEEP: all three findings (age skew, cards 4-7yr peak, T+C-only top mix).
FIX: (a) **UNSTACK the bars** - cards is a SUBSET of enterprise; stacking
them double-counts cards and inflates every bar. Use grouped bars or
enterprise bars with a cards marker; (b) G7 - add the no_ucp_match bucket
as its own category in each panel; (c) G2 - n per band; (d) TIBC panel:
label combos in words ("Transaction + Credit only") not T=1 I=0 flags,
and add n per combo.

### Q5 - before/after heatmaps
KEEP: the tier x segment cut and the concentration finding.
ORDER (Andre 2026-08-03): LEAD with the cards-unsub prevalence panel
(leavers_cards_unsub_subset / clients_total) - THAT is the Cards profit
story. The any-list prevalence (~4%) becomes one labeled context line
("share of the cohort that disengaged from email overall"), not a
co-equal panel. Keep the ratio panel (cards share of unsubs) - it carries
the high-spend concentration finding.
FIX: (a) G5 relabel everywhere - panel titles become: "Any-list unsub
prevalence by anchor (% of cohort)", "Cards-list unsub prevalence", and
"Cards share of unsubs (%)"; the third panel's finding restated as:
"Cards-list unsubs skew toward High-spend clients (10.3% of
High-Transactor unsubbers vs 4.7% of Low-Revolver)"; (b) G2 - n printed
in every heatmap cell (counts exist in the table); (c) the High x Dormant
cell (86 leavers, 8 cards) gets a small-base mask; (d) the "3x4 template"
table prints in full including clients_total, and verify
stayers + leavers = clients_total per row; (e) t12 stays out entirely
until September - one footnote sentence only.

### Q6 - the 12-month curve
KEEP: the FIFA story - it is the headline of the whole analysis.
FIX: (a) title becomes "Cards unsub share peaked at 3x baseline in
Feb-Apr 2026, coinciding with the FIFA campaign (FWC)" - G6; (b) RUN THE
TIMING CHECK that upgrades "coincides" to something stronger: from the c
cube, plot FWC sends by month vs FWC unsubs_attributed by month on small
multiples - if unsubs track sends with 0-1 month lag, say "unsubs
followed FWC send waves"; include this chart; (c) caption the 17.6%:
"share of monthly unsub EVENTS - not directly comparable to the 10.3%
deduped unique-client share"; (d) grey out or annotate the last 1-2
months (202606-202607): recent events' identity-bridge rows lag loading -
immature months, not a real decline; (e) G4 - the red series is Cards
LOB per Andre's mapping; label it.

### Action-type deep dive (4-panel)
KEEP: the decomposition idea and the monthly trend by type.
FIX: (a) "Pre_Attract" is ONE campaign - label the series/bars "FWC
(FIFA)" wherever the category has n=1 MNE; (b) G1/G2/G3 on the intensity
panel; (c) "irritates" -> neutral wording; (d) Operational showing zero:
verify it is a true zero and state it ("0 unsubs across n sends"), not an
artifact of exclusions.

### MNE landscape (bubble + bar/diamond chart)
KEEP: size-vs-rate landscape concept.
FIX: (a) the axis formula label is WRONG - change to unsubs/senders (G1)
and pin the units (0.59 means 0.59%); (b) G3 small-base floor removes the
misleading top points (POT, CEC); (c) print the full 17-row source table
in the notebook output - 16 of 17 rows were invisible in review; every
pixel-read number must be checkable; (d) G4 label.

## ROUND 2 — ANDRE'S REVIEW (2026-08-03). Overrides anything conflicting above.
Apply on the NEW 03d CSVs (7 files incl a1_lob_dedup.csv). Items tagged (A) are
Andre's direct orders; (R) are technical-review items that still stand.

### VOICE — applies to every title, axis, caption (A)
V1. Plain, precise, few words. Never a complicated word for a simple concept.
    BANNED: "attributed" (say "unsubs" / "unsubscriptions"), "any-list" (say
    "unsubscribed from any RBC email" vs "from a Cards campaign"), "upper
    bounds", "multi-list overlap", "heavier-mailed clients" (say "clients who
    received more emails"), "leaver" without definition, "prevalence" where
    "unsub rate" works.
    The overlap caveat - REFRAMED AS A FINDING (Andre 2026-08-03): by system
    design, one unsubscribe should log to CPC and suppress ALL future
    marketing mail, so a second unsubscribe should be impossible outside the
    short propagation window between log and next decisioning. Observed:
    campaign counts add to 146,706 vs 109,431 unique people - ~37K clients
    (1 in 4 unsubscribers) had to unsubscribe MORE THAN ONCE. State it that
    way: "a measure of the suppression gap", an exception/defect signal (ties
    to the CPC leak workstream), never "normal multi-list behavior".
    NEW REQUIRED CHART: for repeat unsubscribers, histogram the time between
    first and second unsub (days). Short gaps = propagation lag; long gaps =
    the suppression flag not consulted. One query on the event table.
V2. EVERY section states its measurement window in the title or first line -
    and ANNOUNCES when the window changes from the previous section (monthly
    Aug-Jun -> recent Jan-Apr -> anchor Aug-2025+12m). These transitions will
    be walked through with peers; they must be self-evident.
V3. Number formatting everywhere: thousands commas; abbreviate 1.2M / 450K on
    axes and labels. Annotations must never overlap other text (Q1 currently
    does). Axis titles and units consistent across ALL charts.

### STRUCTURE — the narrative arc stays as built (A)
Panoramic (enterprise by LOB) -> zoom to Cards monthly -> enterprise by MNE
(top offenders) -> volume-vs-rate context -> frequency -> profile -> Piece B
-> curve. Keep it. UNKNOWN LOB rows stay visible (mapping gap is known;
Andre resolves the mapping separately - do not silently drop them).

### PER-SECTION ORDERS
S1 (Cards monthly zoom) (A): the line time series does not work for reading
   individual campaigns through the year. Replace with a HEATMAP: rows = Cards
   MNEs, cols = months, cell = unsubs (or rate %) - one glance shows who
   spiked when. Optionally small-multiples for the top 5 MNEs. Propose both,
   pick what reads best.
S2 (Q1 top offenders) (A): fine as a bar chart; fix voice (V1), state the
   window (V2), fix the overlapping annotation, plain-words overlap caveat.
S3 (Q3 frequency) (A - REDESIGN): current panels unclear. Must state: emails
   counted Jan-Apr only (same window as the unsubs); say explicitly which
   emails the x-axis counts (all RBC vs Cards emails) on each panel. NEW
   primary view: DISTRIBUTION COMPARISON - for clients who unsubscribed from
   a Cards campaign vs clients who stayed, what share of each group received
   1-2 / 3-5 / 6-10 / 11+ emails (side-by-side bars or the representation
   ratio). Answers Andre's question directly: "do unsubscribers get more mail
   than stayers?" Rate-by-bucket becomes the secondary panel. Data exists in
   a3 (stayers + leavers per bucket).
S4 (Q4 profile) (A - REDESIGN): the shared-axis grouped bars fail (Cards
   crushed) and the story is incomplete without the baseline. New pattern per
   dimension (age, tenure, TIBC): the REPRESENTATION RATIO view - share of
   unsubscribers in a band DIVIDED BY share of stayers in that band, diverging
   bars around 1.0 (the pattern from unsub_value_exploration.html Andre
   endorsed). This answers "is young unsub just because we mail young people"
   by construction. Build it at two zoom levels: (a) Cards vs enterprise,
   (b) WITHIN Cards by MNE for the top Cards campaigns. Keep stayers as the
   baseline everywhere. TIBC panel rebuilt this way too (current one is
   wrong: enterprise-only, unsorted, overclaimed title). Be selective - the
   best 3-4 charts, not every possible cut.
S5 (Q5 / Piece B) (A - SIMPLIFY): drop the any-RBC-email lens from the
   DISPLAY (keep in data). Follow ONLY: Cards-mailed cohort, split into
   CARDS-CAMPAIGN UNSUBSCRIBERS vs stayers. State on-chart: tier (High/Mid/
   Low spend) and segment (Revolver/Transactor/Dormant) are measured AT THE
   ANCHOR (Aug 2025); the same clients get re-measured Aug 2026 (available
   after 2026-08-31 - one clean sentence, not a warning wall). TWO heatmaps
   max: (1) Cards unsub rate % per tier x segment cell, n printed in-cell,
   thin cells muted; (2) where the unsubscribers SIT - each cell's share of
   all cards unsubscribers (the "how big is each box" view). Kill the third.
S6 (Q6 curve) (A): keep - it works. Make axis labels/scales consistent
   across the Q6 panels. (R) still required: recompute the share peak
   excluding immature months 202606-202607 and state on-chart that the peak
   survives (or doesn't).
S7 (action-type deep dive): keep, per-send rates (R2-9 logic), plain words.

### TECHNICAL ITEMS THAT STAND (R)
T1. Reliability: mute/hatch thin heatmap cells (whole Dormant column);
    Wilson CIs or a separate small-base table where denominators differ
    wildly (12K vs 4.2M).
T2. One rate format everywhere INCLUDING tables: percent with % sign, 2
    decimals. No bare decimals (0.188 reads as 18.8%).
T3. Window-mismatch rule: no rate where sends and unsubs come from different
    windows (the FNI case: 0 in-window sends, 2,281 unsubs from earlier
    sends - show volume + "unsubs from earlier sends", rate = n/a).
T4. Investigate the PBA send spikes (202603: 7.9M, 202606: 8.4M vs ~1M
    normal) before charting monthly trends; report the cause.
T5. Explicit zeros ("0 unsubs, n=540"), no blank bars. Check Attract's rate
    for short-window inflation vs first-send dates.
T6. Kill the dual-axis combo chart (senders bars + rate diamonds); use the
    two-panel pattern. Label log axes "log scale". Fix truncated labels.
T7. Export final charts as PNGs from the notebook; full source tables
    printed under each chart; re-render the FWC timing-check panel in full.

## ROUND 3 - POLISH (2026-08-03, after review of the rebuilt set)
The redesigns landed and the arithmetic audits clean. Final touches only:
P1. Q3: print raw counts (unsubs/denominator) beside every rate; give the two
    bottom rate panels a MATCHED y-axis; recolor the rate bars neutral (grey)
    - red currently means "unsubs group" in the top row and "rate" below,
    same color two meanings.
P2. Q4: restore the legend on the Product Mix panel; one shared x-range for
    all three panels; fixed axis floor so no bar extends past the tick range.
P3. Q5: add a light caution on Mid/Dormant (57 events) - one thin-flag tier
    is not enough; cap thin-cell rates at 2 decimals (0.315% on n=9 is false
    precision - print "~0.3%").
P4. Verify the subplots(4,1) code vs the rendered 2x2 layout - stale render
    or intentional reshape; make code match output.
P5. Deliver the three ordered exhibits still unseen: (a) the FWC timing panel
    fully rendered; (b) the LOB dedup five totals printed LARGE as the
    denominator card (enterprise / all-cards / marketing / ex-FIFA /
    non-marketing unique clients); (c) the suppression-gap histogram (days
    between a repeat unsubscriber's first and second unsub).

## AFTER APPLYING: rebuild all plots, re-print the full source tables
under each, then STOP and show the revised set for review BEFORE building
the HTML page. The HTML build then follows the exploration-page prompt
already provided, using ONLY revised charts and their recorded numbers.
"""

if __name__ == "__main__":
    print(PROMPT)
