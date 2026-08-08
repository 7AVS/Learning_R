# 2-Slide Story — "What measuring unsubs taught us" (narrative for red team, 2026-08-07)

Deliverable: Power Pack Q3 spotlight, 2 slides. Audience: pod + stakeholders (CARDS focus,
bank-wide = comparator only). Every number below traces to
`results_2026-08-07_full_notebook_run.md` (notebook run of commit 6d6a2a2).
Charts tell the story; text is a nudge. No hedges in slide copy; caveats printed on exhibits.

---

## THE STORY IN ONE BREATH

Cards barely bleeds email consent — half the enterprise average, and the one big spike was an
event, not the program. Where we DO bleed, it is not over-contacting: it is shallow
relationships — acquisition audiences, the youngest and oldest clients, people re-contacted
after a gap. And an unsub is not a lost client — it is a lost channel to a client whose
spend is already flattening. That makes unsub rate a cheap, early relationship-depth
signal we can act on with targeting and cadence tests.

---

## SLIDE 1 — WHERE Cards loses the channel (landscape + spikes)

**Headline:** "Cards unsubs are low and event-driven — 0.18% per send vs 0.37% Loyalty;
the 202604 spike to 17.6% of enterprise unsubs was FIFA, and it receded."

**Exhibit A (PURPOSE-BUILT — one visual unit, not the notebook charts):** "The landscape."
Timeline of Cards' share of enterprise unsub events (4.8 → 17.6% peak 202604 → 8.4/7.9
receding; mature avg 9.9% dashed) with the FIFA wave annotated as an EVENT — FWC send bars
ghosted behind the line, 0-1 month lag arrow, window shaded; 202606-07 marked immature.
Hanging off its right: a compact rate ladder of Cards campaigns (0.16-0.58%), PCQ highlighted
as the one structural outlier (only Cards campaign in both enterprise top-10s). One reading
path: low and stable → one event spike → one structural outlier.

**Comparator strip (small):** LOB avg unsub per send: Cards 0.18% · PSI 0.20% · Commercial 0.21% ·
Loyalty 0.37% · (PBA 0.12%). Bank-wide is context, not the story.

**Slide-1 learnings (nudge text):**
1. The program is not burning the channel — events and acquisition audiences are.
2. PCQ is the campaign to watch (0.58%, 771K mailed, acquisition audience).

---

## SLIDE 2 — WHO unsubs and WHAT it costs (the levers)

**Headline:** "Unsub risk is a relationship-depth signal — and the cost is the channel,
not the client."

**Exhibit C (PURPOSE-BUILT — does not exist yet): the RISK MULTIPLES LADDER.**
One horizontal chart; every place risk concentrates, expressed as a multiple of its safe
counterpart (each bar is a ratio within its own basis; basis printed under each bar):
- 3-5 emails vs 6-10 (7-month honest bands) — x4.2 (1.00% vs 0.24%)
- re-contacted after a gap vs true first-contact — x3.8 (1.27% vs 0.33%)
- Cards-only exposure vs all-three-programs — x3.1 (0.55% vs 0.18%)
- acquisition vs deepen audiences — x2.0 (0.56% vs 0.28%)
- <25 vs mid-age, per email delivered — x1.8 (1.37 vs 0.76 per 1k; 65+ elevated too: 1.11)
One picture carries the thesis: shallow relationship = high multiple; depth = protection.
Side annotations: "saturation is dead — heaviest-contact bands have the LOWEST rates" and
"intensity is flat by age (4.4-4.7 emails/client) — propensity, not over-contacting."
[Printed caveats: unsub stops the email count (leavers tilt low-band partly by construction);
exposure groups mutually exclusive.]

**Exhibit D — value strip** (three small numbers, one row):
- Depth protects: clients on all three programs unsub from Cards lists at 0.18% vs
  Cards-only 0.55% (mutually exclusive exposure groups). Leavers hold 1.99 product
  categories vs stayers 2.44.
- Spend diverges: leavers' card spend -1.1% YoY vs stayers +2.3% (DFP-matched).
- Unsub ≠ attrition: leavers' profit still grows (+25.1% vs +23.6%, everyone-anchored basis);
  exits are modestly elevated (presence exits 5.9% vs 2.6%; card loss 1.91% vs 1.63%) —
  descriptive, groups not matched.

**Slide-2 CALL (the one marketing line, business-case position):**
"Unsub rate is an early relationship-thinning signal. Protect the channel where the
relationship is shallow: cadence/suppression tests on (a) PCQ acquisition audiences and
(b) re-contact-after-gap cohorts — both are measurable with existing randomization."

---

## WHAT GETS CUT (catalogued, not shown)
Q0 6-panel grid · Q1 enterprise tables (kept as one comparator strip) · Q2a stacked bars ·
Q3a/b/e detail · Q4 v1 + Q4-LB full table (one number survives) · Q5 v1/v2 (superseded by v3 —
v2's 65+ bar is a denominator artifact) · ATTRITION chart 2 · D2b chart (one number survives) ·
D5/D6 heatmaps · PROFIT CHECK panel (a).

## DEFINITIONS THAT MUST BE ON-SLIDE (red-team pre-empt)
- "Unsub" = completed per-list opt-out (disposition 4, verified 2026-08-05), ATTRIBUTED to the
  list unsubbed. Attribution ≠ exposure (they differ up to 4.6x per campaign) — label every
  exhibit with which one it uses (all exhibits here are attribution-based).
- Frequency bands = delivered Cards emails Oct 2025-Apr 2026; unsub window Jan-Apr 2026.
- Q5a v3 universe = Cards-mailed clients only; per-email = unsubs / emails delivered.
- Attrition/profit/spend panels are DESCRIPTIVE (groups not matched; leavers skew younger,
  4-7yr tenure).

## KNOWN GAPS / OPEN ANGLES (offer to red team, not on slides)
1. Repeat unsubs: distribution pending `evidence_repeat_unsub.sql` (Andre to run).
2. Engagement class ("losing the listeners" — unsubbers are engaged clients): museum-era
   finding, not rebuilt on the unified pipeline. Candidate third angle if red team asks who
   the unsubbers were behaviorally.
3. Per-email propensity by TENURE (only age built); tenure is the other edge dimension.
4. 202606-07 immature — t12 rerun after 2026-09-01.
5. TIBC Spotlight-2 ask is served by the depth strip (Cards-specific composition framing per
   brief); full Q5c TIBC chart available if stakeholders want the breakdown.

## TENSIONS WITH PRIOR LOCKED DESIGN (Andre decision)
- This supersedes the 2026-07-22 "Anatomy of an Unsub" 3-number single-slide design (built on
  museum-era data). Kept from it: channel-loss-not-client-loss framing; CPC cold-open stays CUT.
- Old verdict "frequency headline DEAD" referred to saturation (more mail → more unsubs) —
  still dead. The NEW frequency finding is the opposite shape (risk at LOW-but-repeated
  contact) and comes from the honest-bands rebuild; not a contradiction, but red team should
  see both statements.
