# 2-Slide Story — "What measuring unsubs taught us" (narrative for red team, 2026-08-07)

Deliverable: Power Pack Q3 spotlight, 2 slides. Audience: pod + stakeholders (CARDS focus,
bank-wide = comparator only). Every number below traces to
`results_2026-08-07_full_notebook_run.md` (notebook run of commit 6d6a2a2).
Charts tell the story; text is a nudge. No hedges in slide copy; caveats printed on exhibits.

---

## THE STORY IN ONE BREATH

Cards is at par with the rest of the enterprise on email consent loss, and the one big
spike was an event, not the program. Where we DO bleed, it is not over-contacting: it is
shallow relationships — acquisition audiences, and people re-contacted after a gap. And
an unsub is not a lost client — leavers' profit still grows even as their card spend turns
negative. That makes unsub rate a cheap, early relationship-depth signal we can act on
with targeting and cadence tests.

---

## SLIDE 1 — Cards is at par with the enterprise on consent loss

**Headline:** "Cards unsub rate is ~0.18% per send, the same as the rest of the enterprise
excluding Loyalty (~0.18%); the all-LOB average (~0.27%) sits higher only because Loyalty
is ~48% of all sends at 0.37%. The 202604 spike was FIFA, an event, and it receded."
[Enterprise figures pending first run of the rewritten Exhibit 1.]

**Exhibit 1 (PURPOSE-BUILT — single panel):** Cards unsub per delivered email, by month,
mature window vs immature trailing months (bridge lag) shaded. FIFA wave (Apr 2026)
annotated as a fact, not a causal claim. Comparator (CARDS vs ENTERPRISE all-LOB vs
ENTERPRISE ex-LOYALTY, plus LOYALTY's rate and its share of sends) computed over the SAME
mature window as the chart — one window on the slide, not two.

**Slide-1 learnings (nudge text):**
1. Cards is not an outlier on consent loss once Loyalty's higher rate and larger send
   volume are separated out.
2. The 202604 spike was FIFA, an event, and it receded — not a program-level trend.

---

## SLIDE 2 — WHO unsubs and WHAT it costs (the levers)

**Headline:** "Unsub risk is a relationship-depth signal — re-contact after a gap and
acquisition-type audiences carry the highest rates."

**Exhibit 2 (PURPOSE-BUILT — two panels, paired bars):** Each panel is two actual rates on
its own y-axis with its own basis line — no ratio axis, no shared scale. The multiple is a
small in-panel annotation only.
- Panel A — re-contacted after a gap vs true first-contact — 1.27% vs 0.33% (x3.8)
- Panel B — acquisition-type (Attract) vs deepen-type (Deepen) actions — 0.56% vs 0.28%
  (x2.0); PCQ marked as a reference line at 0.58% — the extreme of the acquisition pattern,
  not a separate category.

**Value (text only, no chart):**
- Leavers' average annual profit still grows: +25.1% (everyone-anchored basis, Jun 2025 →
  Jun 2026, no-longer-present clients counted at $0).
- Leavers' card spend turns negative: -1.1%, while stayers' card spend is positive: +2.3%
  (DFP-matched). Descriptive — groups not matched; leavers skew younger, 4-7yr tenure.

**Slide-2 CALL (the one marketing line, business-case position):**
"Unsub rate is an early relationship-thinning signal. Protect the channel where the
relationship is shallow: cadence/suppression tests on (a) PCQ acquisition audiences and
(b) re-contact-after-gap cohorts — both are measurable with existing randomization."

---

## CUT 2026-08-07 (approved)

Cut from 3 exhibits / 5 findings to 2 exhibits / 3 findings. Dropped findings, one line each:
- **Frequency: 3-5 emails vs 6-10 (x4.2)** — unsub stops the email count, so leavers are
  mechanically pushed into low bands. The comparison partly measures its own construction.
- **Cards-only vs all-three-programs (x3.1)** — pure selection: being on three programs is
  depth by definition, not evidence depth causes lower risk.
- **Age <25 vs mid-age (x1.8)** — clean read, but no action attached. Q&A reserve, not a
  slide finding.

The age U-shape and the 3-5 email risk peak remain documented in
`results_2026-08-07_full_notebook_run.md` for Q&A — not on the slides.

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
- Value numbers (profit, spend) are DESCRIPTIVE (groups not matched; leavers skew younger,
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
