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

## ROUND 2 ORDERS (2026-08-03, review of the revised full set)

Round-1 orders were verified applied - good. These are the remaining fixes,
found by a second full review. Apply ON THE NEW 03d CSVs (7 files incl
a1_lob_dedup.csv) once the full run lands - not on the old data.

R2-1. **Reliability encoding.** No CIs exist anywhere. Minimum: (a) heatmap
  cells with thin n get muted/hatched color regardless of rate - color must
  not scream "severe" on n=1.9K as loudly as on n=620K; flag the whole
  Dormant COLUMN as thin, not just one cell; (b) on any rate ranking that
  spans wildly different denominators (Q2 rate panel: 12K vs 4.2M), add
  Wilson 95% CI whiskers or drop sub-50k rows to a separate "small base"
  table.
R2-2. **One rate format everywhere, including TABLES.** The M0/M-cards
  tables print bare decimals (0.188) beside %-formatted tables (0.11%) -
  a reader will read 0.188 as 18.8%. Every rate cell and axis carries %.
R2-3. **Window-mismatch rule (the FNI case).** A campaign can have
  in-window unsubs with zero in-window sends (unsubs from PRIOR sends -
  real, not a bug). Rule: a rate is computed ONLY when sends and unsubs
  share the window; otherwise show volume + tag "unsubs from pre-window
  sends", rate = n/a. Apply globally; FNI is the known instance.
R2-4. **PBA send spikes 202603 + 202606 (7.9M/8.4M vs ~1M other months):**
  investigate BEFORE charting any monthly trend - which PBA MNEs, real
  burst or double-count? Report the answer.
R2-5. **Q6 robustness check (required):** recompute the Cards share-of-
  events peak EXCLUDING the immature months (202606-202607) and confirm
  the ~17.6% Apr peak stands independent of the shrinking denominator.
  State the result on-chart.
R2-6. **Q6 chart 1:** stacked-subset bar again - replace with two clean
  series (enterprise volume line + cards volume line) or grouped bars;
  make the immature-month marking a hard visual break (hatch + axis note),
  not opacity alone.
R2-7. **Q4 age/tenure:** cards bars are crushed on the shared axis - the
  "4-7yr peak" is invisible. Give cards its own panel or print value
  labels on every cards bar.
R2-8. **Q4 TIBC:** add the cards-list series (the panel is enterprise-only
  - the cards mandate is missing); sort bars by value; key the colors; and
  drop the word "only" from the title - two other combos sit in the same
  band.
R2-9. **Action-type monthly trend:** raw counts invite the volume-artifact
  read. Add the monthly SENDS for FWC (or plot monthly unsub rate %) so
  the Feb-Apr spike is shown per-send, not just per-calendar; small
  series (Onboard/Attract/Deepen) get their own scale or panel.
R2-10. **KILL the dual-axis combo chart (senders bars + rate diamonds on
  two x-axes).** Highest misread risk in the set - PCQ's rate diamond
  visually lands at FWC's volume. Use the existing two-panel pattern
  (volume panel + rate panel). Fix truncated MNE labels; label any log
  axis "log scale".
R2-11. Explicit zeros: Operational prints "0 unsubs (n=540)" - blank bar
  reads as missing data. Also check Attract's 0.58%: if its campaigns
  started mid-window, the shorter exposure inflates the rate - verify
  against first-send dates and caption if so.
R2-12. **Deliverable hygiene:** export every final chart as PNG from the
  notebook (no phone photos - a screen-overlay icon currently blocks data
  points in three captures); print every source table in full under its
  chart; re-render the FWC timing-check panel fully visible (it was cut
  off - and it is now a headline exhibit: "unsubs track send waves with
  0-1 month lag").

## AFTER APPLYING: rebuild all plots, re-print the full source tables
under each, then STOP and show the revised set for review BEFORE building
the HTML page. The HTML build then follows the exploration-page prompt
already provided, using ONLY revised charts and their recorded numbers.
"""

if __name__ == "__main__":
    print(PROMPT)
