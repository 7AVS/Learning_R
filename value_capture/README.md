# Value Capture — Q2 Entity-Reporting Pipeline

Produces rows for a partner team's Q2 entity-reporting Excel table. Two campaign blocks exist now
(PCL sales modal, PCQ Modal Sales); two more will arrive later from teammates with different internal
logic. The design centers on a fixed **interchange contract** so any block — regardless of engine,
population definition, or arm logic — can target it. Everything downstream (pooling, stratified lift,
significance) is computed in **SQL** — there is no workbook and no Python layer.

## Engine

Everything under `value_capture/` runs **Teradata-direct** (bare table names, no catalog prefix — do
NOT run through Starburst federation). This is a change from the pipeline's first version: PCL's block
originally ran Trino/Starburst; it was converted to Teradata-direct so the whole folder runs in one
engine, matching PCQ's block (which was already Teradata-direct because of the tactic-event scan its
per-cohort file uses elsewhere).

## Directory layout

```
value_capture/
  README.md                      this file
  blocks/
    pcl_sales_modal_block.sql    Teradata-direct — re-grain of p9_vcl_full_measurement.sql, cohort_month grain
    pcq_ms_block.sql             Teradata-direct — re-aggregation of pcq_ms_summary.sql QUERY 2, cohort_month grain
  value_capture_report.sql       Teradata-direct — ONE query, final report rows (pooled + stratified stats)
```

## Workflow

1. A campaign block (existing or a teammate's new one) emits rows in the **interchange contract**
   shape, one CTE per campaign.
2. Each block's CTE gets UNION'd into `all_rows` inside `value_capture_report.sql` at the
   **TEAMMATE HOOK-UP POINT** marked in that file.
3. Run `value_capture_report.sql` Teradata-direct — it pools cohort_months and strata, computes
   stratified lift and a two-proportion significance test, and returns one row per test contrast
   (decision-sized, ~3-4 rows currently: PCL, PCQ-gated, PCQ-ungated, plus a validation row only if
   unmapped codes appear).
4. `DESC`, `Type`, `Reference Document`, and `Notes` are typed in by hand on the partner sheet — they're
   business framing, not derivable from the SQL.

For **per-cohort** presentation (cohort_month grain, not pooled), query `blocks/pcl_sales_modal_block.sql`
or `blocks/pcq_ms_block.sql` directly — they are unchanged and stay at that finer grain.
`value_capture_report.sql` re-derives their same population/arm/success logic (ported inline, not by
literally including the files — Teradata has no cross-file include) and pools it to the final numbers.

## The interchange contract

One row per **(test contrast x stratum x cohort_month)** out of a campaign block CTE. Columns, exact
names and order:

| Column | Meaning |
|---|---|
| `mne` | campaign mnemonic (e.g. `PCL`, `PCQ`) |
| `test_desc` | description of the test contrast (must be IDENTICAL text across every row of that test — it's the pooling key in `value_capture_report.sql`) |
| `trt_start_dt` | treatment window start actually observed in data |
| `trt_end_dt` | treatment window end actually observed in data |
| `success_name` | human label for the success event |
| `stratum` | `'overall'` for unstratified tests; a decile label (`'D1'`..`'D10'`) for stratified ones |
| `cohort_month` | `YYYY-MM` of `treatmt_strt_dt`/`treatmt_start_dt` — ALWAYS present, even though pooling across months happens downstream (repo hard rule) |
| `test_clients` | distinct clients, TREATED arm only |
| `test_successes` | distinct clients, treated arm, success = 1 |
| `control_clients` | distinct clients, control arm only |
| `control_successes` | distinct clients, control arm, success = 1 |

**Rules for any block (existing or a teammate's new one):**
- Counts only — unique clients, no rates, no divisions, no lift computed in the block itself.
- `test_clients`/`test_successes` = the TREATED arm only. Never invert this.
- Internal logic (population, engine, arm derivation, success definition) is entirely the block
  owner's call — the contract only fixes what comes OUT.
- If a block can't cleanly split test vs. control per row at build time (arm codes not known/stable),
  map codes to `test`/`control` in SQL via a small `arm_map` CTE (see PCQ's pattern in
  `value_capture_report.sql`) — and surface any code that fails the map as its own output row
  (`test_clients`/etc = NULL), never a silent drop.

### Why PCL and PCQ look different internally

- **PCL** already knows which arm is treated (challenger/WMS) vs. control (champion/NMS) at SQL-build
  time (`report_groups_period LIKE` pattern), so its CTE emits ONE contract row per `cohort_month`
  with both `test_*` and `control_*` columns filled directly (arm pivoted with `CASE`/`COUNT(DISTINCT
  CASE WHEN ...)` inside the query).
- **PCQ** deliberately does NOT hardcode which `test_group_latest` code is challenger vs. champion in
  the base extraction (codes drift across deployments/sources — see `campaigns/sales_modal/README.md`
  Open Decision #2). `value_capture_report.sql` maps codes via an explicit `arm_map` CTE — literal
  `SELECT ... UNION ALL SELECT ...` rows (`'NG3_CHMP'→'control'`, `'NG3_CHLN'`/`'NG3_CHLG'→'test'`,
  clearly marked EDIT POINT/VERIFY; not a `VALUES` row-constructor, which Teradata doesn't reliably
  support as a CTE body) and joins it in — any `test_group_latest` value not in that list produces a
  distinct `pcq_unmapped_row` in the final output (test_desc = a dynamic
  `'UNMAPPED test_group codes: N code(s), e.g. X .. Y -- fix arm_map and rerun'` message built from
  `COUNT`/`MIN`/`MAX`, since Teradata has no `array_agg`/`array_join` to list every code; counts NULL)
  instead of silently dropping those clients.

## Stats — implemented as SQL (in `value_capture_report.sql`)

All arithmetic runs on **post-aggregation** counts (`SUM()` results), cast to `FLOAT`, guarded with
`NULLIF`/`CASE` against division by zero — never on raw source columns, so it stays 9881-safe.

**`paired`** — pool `cohort_month` per `(mne, test_desc, stratum)`:
`n1=SUM(test_clients)`, `x1=SUM(test_successes)`, `n0=SUM(control_clients)`, `x0=SUM(control_successes)`.

**`strata_stats`** — per stratum:
`d = x1/n1 - x0/n0`, `w = n1*n0/(n1+n0)`, `pbar = (x1+x0)/(n1+n0)`, `v = pbar*(1-pbar)*(1/n1+1/n0)`.

**`test_stats`** — pool strata per `(mne, test_desc)` (this is where PCQ's 10 deciles collapse back
into one number per test):
`leads = SUM(n1)`; `lift = SUM(w*d)/SUM(w)`; `se = SQRT(SUM(w*w*v))/SUM(w)`; `z = lift/se`;
`p_value = 2*(1-Φ(ABS(z)))`; `significance = IF p_value<0.05 THEN 'Y' ELSE 'N'`.

Single-stratum tests (PCL: `stratum='overall'` only) collapse this to the plain two-proportion
z-test automatically — the single stratum's `w` cancels out of `SUM(w*d)/SUM(w)` leaving `d`, and
`SQRT(SUM(w*w*v))/SUM(w)` leaves `SQRT(v)`, the standard SE. No special-casing needed.

**No `normal_cdf` in Teradata** — Φ(z) (the standard-normal CDF) is approximated via the Zelen &
Severo / Abramowitz & Stegun 26.2.17 rational approximation (`zs_base`/`zs_t`/`zs_phi`/`zs_cdf` CTEs;
`t` computed once and carried through, powers done as repeated multiplication, not `**`). Max abs
error on the CDF is < 7.5e-8; since `p_value` doubles that, the observed error on `p_value` runs up to
~1.5e-7 — verified numerically against `scipy.stats.norm` at z=2.0520/1.96/0.5/3.29, all deltas
< 1.4e-7, nowhere near enough to flip a significance call at p<0.05.

## Final report columns

`value_capture_report.sql`'s final `SELECT`, in partner-template left-to-right order (NULL placeholder
columns sit in their correct template position so the layout lines up when pasted):

| Output column | Partner sheet column | Source |
|---|---|---|
| `mne` | MNE | computed |
| `desc_manual` | DESC | NULL placeholder — fill by hand |
| `type_manual` | Type | NULL placeholder — fill by hand |
| `test_desc` | Test Desc | computed |
| `trt_start_dt` | Treatment Start Date | `MIN` over the test |
| `trt_end_dt` | Treatment End Date | `MAX` over the test |
| `success_name` | Success | computed |
| `leads_unique_clients` | Leads/Unique Clients | `SUM(n1)` across strata/cohorts |
| `lift_pp` | Lift | `lift * 100`, rounded to 2 decimals |
| `z` | *(supporting/audit)* | not on the partner sheet, kept for QA |
| `p_value` | *(supporting/audit)* | not on the partner sheet, kept for QA |
| `significance` | P-value/Significance | `Y`/`N` at p < 0.05 |
| `reference_document` | Reference Document | NULL placeholder — fill by hand |
| `notes` | Notes | NULL placeholder — fill by hand |

## Open decisions inherited from `campaigns/sales_modal/`

Both still apply and are NOT resolved by this pipeline — see `campaigns/sales_modal/README.md`:
1. **Period-ASC gating** (PCQ only) — general canon gates success numerators to
   `TRIM(asc_on_app_source)='Period-ASC'`; Andre's 2026-06 instruction for the MS descriptive read was
   to leave it ungated. `value_capture_report.sql` ships BOTH as separate test contrasts
   (`... approved (Period-ASC gated)'` / `'... approved (ungated)'`) — Andre picks which row goes to
   the partner sheet. `completed`-metric variants stay available at cohort/decile grain in
   `blocks/pcq_ms_block.sql` but are not carried into this rollup (approved was picked as the
   reported success metric).
2. **Population split** (PCQ only) — `test_group_latest` (assignment/ITT) vs. `ms_targeted` (delivery,
   post-assignment, self-selected). This pipeline uses assignment (`test_group_latest`) because only
   ITT is valid for a lift-flavored read — fixed for this pipeline, not left open.

## For teammates adding a new block

1. Write your block's SQL in whatever engine/logic fits your campaign. Output exactly the 11 contract
   columns (or map raw arm codes to `test`/`control` in SQL, PCQ-style, with an unmapped-code guard row).
2. Counts only. Unique clients. `test_clients`/`test_successes` = treated arm, never inverted.
3. Keep `cohort_month` even if you plan to pool immediately — it's a repo-wide hard rule.
4. Give your `test_desc` string ONE fixed, exact value across every row of your test — it's the
   pooling key.
5. Add your CTE to the `all_rows` UNION ALL at the TEAMMATE HOOK-UP POINT in
   `value_capture_report.sql`. No other changes needed — `paired`/`strata_stats`/`test_stats` pick up
   any `(mne, test_desc, stratum)` combination automatically.
