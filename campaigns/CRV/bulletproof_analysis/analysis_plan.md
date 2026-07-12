CRV-PCL BULLETPROOF ANALYSIS — PLAN & RATIONALE
==================================================
Captured: 2026-05-26
Purpose: preserve the WHY behind the 12 queries. The queries themselves
are self-explanatory; this file is the reasoning + context + locked
methodology + open items, so future sessions don't lose the thread.


----------------------------------------------------------------
1. WHY WE REOPENED CRV
----------------------------------------------------------------

The original CRV-vs-PCL cannibalization deck was delivered May 2025
(closed 2026-05-15). Recommendation: discontinue CRV on the mobile
banner. Headline: ~42K PCL conversions recoverable, translated to
~$30M IBT. That $30M figure made executives' eyes pop — and now we
have eyes on us. The recommendation is a transfer of dollars from one
line of business (CRV) to another (PCL), held by different executives,
who will scrutinize hard. The case needs to be bulletproof.

Bulletproof means: methodology is airtight, the causal claim is
defensible, the $$ figure is reproducible and net (not just gross),
the alternative options have been considered.


----------------------------------------------------------------
2. CROSS-LOB STAKES
----------------------------------------------------------------

- PCL exec WILL accept the recovery story (gets the dollars)
- CRV exec WILL push back (loses the dollars and the campaign)
- Both will scrutinize the math, the audience, and the methodology
- Neutral framing matters: "yield exhausted on one side + recovery on
  the other" is more defensible than "kill CRV"
- Throttling (not killing) is the obvious soft-landing exec ask — we
  must be ready with numbers for that scenario


----------------------------------------------------------------
3. WHAT THE DECK NEEDS TO DEFEND
----------------------------------------------------------------

a) The 0.97pp Action-vs-Control PCL response gap is the CAUSAL piece.
   The 3.6pp no-overlap-vs-Action gap is selection bias and should
   NOT be the headline. The original deck conflated them; the
   bulletproof version separates them explicitly.

b) The ~$30M is GROSS PCL recovery, not net. Net = PCL gained MINUS
   CRV conversions forgone. We need both counts to compute the net
   number (Excel-side, post-SQL).

c) The 4.4M overlap cohort needs to be characterized — what kind of
   clients are they? Are they the PCL ICP? Is the audience comparable
   across arms?

d) Robustness: the gap should hold across months, across audience
   profiles, across alternative throttling policies.

e) Statistical significance: the gap is real, not noise.


----------------------------------------------------------------
4. THE KEY DISTINCTION: CAUSAL VS SELECTION
----------------------------------------------------------------

Action vs Control comparison = CAUSAL (randomization survives in the
overlap subset, validated by Query 01 balance test)

Action vs no-overlap = NOT CAUSAL (different audience, different
selection process). The no-overlap clients were never deployed CRV
in the window — they're a structurally different group.

The slide should center the 0.97pp causal piece. The no-overlap line
either gets dropped or gets an explicit "different audience" caveat.


----------------------------------------------------------------
5. GROSS VS NET ECONOMICS
----------------------------------------------------------------

Gross PCL recovery = 42K conversions × $6.95 PCL NIBT/IBT = ~$30M
                    (PCL $-per-conversion confirmed by Andre)

Net = Gross PCL gained MINUS gross CRV forgone
CRV forgone = (CRV-Action conversion rate on overlap cohort) ×
              (4.4M overlap leads) × (CRV $-per-conversion, TBD)

Substitution effect (Query 07) further nuances this — if CRV and PCL
are SUBSTITUTES (clients pick one or the other for similar needs),
stopping CRV captures the same demand cleanly via PCL. If COMPLEMENTS
(different needs), we lose distinct customer value. Co-conversion
rate in the overlap cohort tells us which.

Andre does all $-math in Excel. SQL outputs counts only.


----------------------------------------------------------------
6. WHAT EACH QUERY BULLETPROOFS
----------------------------------------------------------------

00 — RANDOMIZATION VALIDATION (sticky vs per-wave)
     Validates whether lead-grain methodology is overkill or
     necessary. RESULT (2026-05-26): 0.20% crossover, effectively
     sticky. Lead grain still correct but practical impact minimal.

01 — BALANCE TEST
     Does the original CRV randomization survive when we slice to the
     PCL-mobile overlap subset? If yes, the 0.97pp gap is causal on
     the subset. If no, the headline number needs caveating.

02 — OVERLAP DAYS DISTRIBUTION
     When CRV and PCL deploy to the same client in overlapping
     windows, how many days do they actually compete? Mean, median,
     p10–p90, min, max — gives the slide a concrete "they compete
     for X days on average" number.

03 — BIDIRECTIONAL OVERLAP SHARE
     What % of PCL overlaps CRV (the ~60% number on the deck) AND
     what % of CRV overlaps PCL (missing from original deck). Both
     directions matter for the exec audience.

04 — CI ON THE 0.97PP GAP
     95% confidence interval and z-stat. Kills "could just be noise"
     pushback before it starts. With n=4.4M the CI is tight.

05 — CRV ECONOMICS ON OVERLAP COHORT
     APR, term, fee, principal distributions. Tells us what CRV is
     actually delivering to the cannibalized clients — proves the
     CRV-side $$ that's being given up if we discontinue.

06 — NET ECONOMICS (COUNTS ONLY)
     Action and Control responder counts on overlap, for both CRV
     and PCL. Andre multiplies by $ in Excel for the net figure.

07 — SUBSTITUTION TEST
     For overlap leads: % CRV-only converters, % PCL-only converters,
     % both, % neither. Substitution vs complementarity question.
     Critical for the CRV-exec rebuttal.

08 — DECILE DISTRIBUTION
     PCL decile composition across (Action / Control / no-overlap)
     cohorts. Characterizes audience. Explains the high no-overlap
     PCL response rate (likely structural audience difference).
     CRV decile NOT available on curated table.

09 — TIMING WITHIN OVERLAP
     When both deploy to same client, who arrives first? crv_first
     vs pcl_first vs same_day. Sequence affects interpretation
     (intrusion vs fatigue).

10 — RETIRED (see RESULTS_CATALOG). Delay objection answered by Q04's persistent steady-state gap; treadmill cadence makes time-to-PCL an artifact.

11 — THROTTLING SCENARIOS
     For caps N in {2,3,4,5}: contacts removed, CRV conversions
     forgone, PCL overlap exposures affected. Soft-landing
     alternative to full kill. Exec WILL ask this — we have the
     answer ready.


----------------------------------------------------------------
7. METHODOLOGY LOCKS
----------------------------------------------------------------

These were debated and decided in session. Don't re-litigate.

- Date window: offer_start_date / treatmt_strt_dt >= '2024-10-01'.
  NO upper bound in SQL. Andre excludes partial May 2026 manually
  in Excel reporting.

- Channel filter (different per campaign — CRITICAL):
  * CRV side: channels_deployed LIKE '%IM%'
    (CRV mobile-banner deployment stamps IM in data — backend
    labeling quirk, CRV-specific)
  * CRV-Control rows: NO channel filter (Control not deployed)
  * PCL side: channel LIKE '%MB%'
    (PCL labels mobile correctly as MB — no quirk)
  * Both target the mobile banner surface
  * NEVER filter PCL with %IM% — that picks up online banner,
    different surface, wrong audience

- Lead grain throughout: each (acct_no × CRV wave × arm) is its
  own observation. NO collapsing to unique-account grain. Same
  client appearing in multiple waves contributes multiple rows.
  Section E's original Action-priority dedup was discarded.

- PCL has NO Action/Control split — confirmed 2026-05-26. Only
  filter PCL by channel and date, never by arm.

- CRV-Control filter on curated: action_control = 'Control'
- CRV-Control filter on tactic event: tst_grp_cd = 'TG8'

- Counts only in SQL. All $-math, rates, ratios computed in Excel.

- Output: one row per slice (month, arm, decile, etc.). Side-by-
  side column layout where direct comparison helps (Query 01
  restructured this way; 07 / 09 may follow if needed).

- PCL $-per-conversion: $6.95 NIBT/IBT (confirmed)
- CRV $-per-conversion: PENDING from Andre


----------------------------------------------------------------
8. METHODOLOGY CORRECTIONS DURING THIS SESSION
----------------------------------------------------------------

- Initially queries used dedup logic inherited from Section E.
  Dropped after Andre confirmed lead grain is correct.

- Initially PCL channel filter was %IM% (matched the original
  deck). Corrected to %MB% mid-session. Original deck's numbers
  may be affected — if exec asks for exact reproduction of the
  $30M figure, we cite the original methodology; for any NEW
  analysis the corrected filter applies.

- CHAR truncation bug discovered after Andre's first run of
  Query 01 showed control_count = 0. Cause: Teradata UNION ALL
  may force first SELECT's column length, truncating 'Control'
  (CHAR 7) to 'Contro' (CHAR 6). Fix: CAST all string label
  literals to VARCHAR(N) before UNION ALL.

- ORDER BY with column names after UNION ALL fails in Teradata.
  All ORDER BY converted to positional integers.


----------------------------------------------------------------
9. BLIND SPOTS IDENTIFIED (CONSIDERED, MOST DEPRIORITIZED)
----------------------------------------------------------------

- Throttle vs kill — modeled in Query 11. ADDRESSED.

- "Are overlap clients the PCL ICP?" — moot. By definition both
  campaigns deployed to them. Dropped.

- "Does CRV exposure change PCL eligibility?" — moot. Section E
  randomization handles it. Dropped.

- "Why is no-overlap PCL response so much higher?" — partial
  answer via Query 08 decile distribution. Andre deprioritized
  deep audience decomposition (scope creep).

- "Did CRV merely delay PCL?" — addressed by Q04's persistent ~1pp gap across 17/20 months in steady state; pure delay would wash out. Q10 retired (treadmill cadence makes time-to-PCL uninterpretable).

- "EM_IM_DO bundle" — ~5-10% of CRV deployments. Bulletproof
  analysis focused on IM-only (95% of CRV). Bundle is a follow-up
  if asked.

- Result B of Query 00 (sample of 100 "both" accounts) — NOT
  REVIEWED. Low priority, 0.20% crossover is in the noise.


----------------------------------------------------------------
10. OPEN ITEMS PENDING
----------------------------------------------------------------

- CRV $-per-conversion from Andre → feeds Query 06 net economics
  multiplication in Excel.

- Run remaining queries (02-11) in Teradata Studio Express. As
  results come in, append to findings.txt.

- Side-by-side restructure for Queries 07 and 09 if their default
  row-per-slice layout proves hard to read.

- Synthesis of all findings into a slide-ready narrative — once
  Queries 03, 04, 06, 07, 08, 10, 11 have results.


----------------------------------------------------------------
11. FOR FUTURE SESSIONS
----------------------------------------------------------------

If you (Claude) are reading this in a future session:
- Memory: project_crv_cannibalization_status.md has the locked
  methodology. Read it first.
- This file: explains WHY each query exists. Read if you're being
  asked to extend / modify the analysis.
- findings.txt: rolling results log. Append new results here.
- The 12 query files: the analysis itself. Don't modify methodology
  without re-reading this plan first.


----------------------------------------------------------------
12. SESSION 2 UPDATES (2026-05-26 late) — METHODOLOGY CORRECTION
----------------------------------------------------------------

The big shift this session: discovered CRV-centric framing was wrong
for cannibalization measurement. Refactored to PCL-LEAD centric.

WHY THE PREVIOUS FRAMING WAS WRONG:
- Old Q04 counted unique CRV-Action waves, each row a separate
  observation. A client with 5 CRV-Action waves overlapping 1 PCL
  deployment contributed 5 rows. Their PCL outcome got counted 5×.
- Produced 1.5pp gap — inflated by multi-wave client weighting.

CORRECT FRAMING:
- PCL-LEAD CENTRIC. Each (acct × PCL deployment) is ONE observation.
- EXISTS semi-join checks "any overlapping CRV-Action wave?" → binary
  flag. No fan-out. Multi-CRV-wave is invisible in the count.
- Produces 1.08pp gap (the headline figure for the bulletproof).
- Matches original Section E methodology exactly.
- Audit agent verified line-by-line. Verdict: CORRECT.

QUERIES REFACTORED:
- Q02 (overlap days) — PCL-centric, EXISTS / LEFT JOIN where needed
- Q04 (CI on gap) — PCL-centric, FLOAT throughout, per-month rows
- Q06 (net counts) — PCL-centric, EXISTS flags for both overlap + CRV-conversion
- Q07 (substitution) — PCL-centric, MIN(crv_first_response_date) per PCL lead
- Q08 (decile distribution) — PCL-centric, EXISTS semi-joins
- Q09 (timing within overlap) — PCL-centric, MIN(crv_strt_dt) per PCL lead

QUERIES KEPT CRV-CENTRIC (intentional):
- Q00 (randomization check — CRV-side question)
- Q01 (balance test — checks CRV-side randomization on subset)
- Q03 (bidirectional — both sides explicitly compared)
- Q05 (installment economics — CRV-side product behavior)
- Q10 (time from CRV start)
- Q11 (throttling — acts on CRV contacts)

Q05 EXPANSION:
- v1 single SELECT installment economics — added cohort split (v2: 2 cohorts,
  v3: 4 cohorts including CRV-Control)
- Fee columns dropped (always empty in data)
- Per-account metrics added: n_waves, n_accounts, n_transactions,
  txns_per_acct, mean_principal_per_acct
- Per-transaction distributions: APR, term, principal (mean/p50/p90)
- Output: 4 cohorts x (overall + ~20 months) = ~84 rows

TERADATA SYNTAX GOTCHAS CAPTURED:
- See memory file [[feedback-teradata-syntax-gotchas]]
- Locked rules: no PERCENTILE in CTEs feeding another query;
  no EXISTS in CASE WHEN; no (FORMAT) on arithmetic in CAST;
  FLOAT on every operand in stats math; CAST string literals
  before UNION ALL; ORDER BY positional; avoid `cohort` as alias.


----------------------------------------------------------------
13. STATE AT END OF SESSION 2 (2026-05-26 ~22:30)
----------------------------------------------------------------

VALIDATED + INTERPRETED (numbers in findings.md):
✓ Q00 — randomization sticky (0.20% crossover)
✓ Q00 Result C — wave geometry: ~21 CRV waves/month × ~92 days;
                  ~4.4 PCL waves/month × ~69 days
✓ Q01 — control_count bug fixed; volumes match original Section E
✓ Q02 — PCL responders ~7 day shorter overlap than all_leads
✓ Q03 — 28% / 27% randomization confidence; 67% PCL footprint
✓ Q04 — 1.08pp PCL-centric gap, z=16.23, all 20 months consistent
                  AUDITED CORRECT (independent agent)
✓ Q05 — 4-cohort installment economics. ~11 plans/converter avg

NOT YET RUN:
- Q06 (net economics counts)
- Q07 (substitution + conversion order)
- Q08 (decile distribution)
- Q09 (timing within overlap)
- Q10 (time-to-PCL)
- Q11 (throttling scenarios)

PROPOSED FOR NEXT SESSION:
- Add "recurring clients" block to Q05:
  per cohort, n_recurring (≥2 plans), pct_recurring,
  p50/p90/max plans per account, mean_months_active
  ~8 cols × 4 rows (overall only, no monthly).
  Andre approved direction; deferred to next session.

OPEN INPUTS:
- CRV $-per-conversion from Andre (for net economics Excel math)
