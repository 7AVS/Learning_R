# Value Capture — Q2 Entity-Reporting Pipeline

Produces rows for a partner team's Q2 entity-reporting Excel table. Two campaign blocks exist now
(PCL sales modal, PCQ Modal Sales); two more will arrive later from teammates with different internal
logic. The design centers on a fixed **interchange contract** so any block — regardless of engine,
population definition, or arm logic — can target it. The workbook (not the SQL) does all statistics.

## Directory layout

```
value_capture/
  README.md                          this file
  blocks/
    pcl_sales_modal_block.sql        Trino/Starburst — re-grain of p9_vcl_full_measurement.sql
    pcq_ms_block.sql                 Teradata-direct — re-aggregation of pcq_ms_summary.sql QUERY 2
  build_value_capture_workbook.py    openpyxl script — run it to (re)generate the workbook
  value_capture_builder.xlsx         generated output (untracked — regenerate, don't hand-edit the formulas)
```

## The interchange contract

One output row per **(test contrast x stratum x cohort_month)**. Columns, exact names and order:

| Column | Meaning |
|---|---|
| `mne` | campaign mnemonic (e.g. `PCL`, `PCQ`) |
| `test_desc` | description of the test contrast (must be IDENTICAL text across every row of that test — it's the join key the workbook pools on) |
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
- Counts only — unique clients, no rates, no divisions, no lift computed in SQL.
- `test_clients`/`test_successes` = the TREATED arm only. Never invert this.
- Internal logic (population, engine, arm derivation, success definition) is entirely the block
  owner's call — the contract only fixes what comes OUT.
- If a block can't cleanly split test vs. control per row (e.g. arm codes aren't known/stable at
  SQL-build time), it may emit one row per raw arm code instead and rely on the workbook's manual
  `arm_role` mapping column on the INPUT sheet — see "PCQ's raw-code rows" below.

### Why the two current blocks look different on the wire

- **PCL** (`blocks/pcl_sales_modal_block.sql`) already knows which arm is treated (challenger/WMS) vs.
  control (champion/NMS) at SQL-build time, so it emits ONE contract row per `cohort_month` with both
  `test_*` and `control_*` columns filled from the same row (arm pivoted inside the query).
- **PCQ** (`blocks/pcq_ms_block.sql`) deliberately does NOT hardcode which `test_group_latest` code is
  challenger vs. champion (codes drift across deployments/sources — see
  `campaigns/sales_modal/README.md` Open Decision #2). It emits one row per
  `test_group_latest x decile x cohort_month`, i.e. one row per ARM CODE, not a pre-paired test/control
  row. Andre (or whoever pastes) reads the raw code off that row and transcribes it into the INPUT
  sheet's `test_clients`/`control_clients` side per the `arm_role` tag — see next section.

## The INPUT sheet and `arm_role`

`INPUT` holds the contract's 11 columns plus two manual columns:
- **`arm_role`** — `test`, `control`, or `both`. `both` is for rows like PCL's, which already carry a
  paired test+control read on one line. `test`/`control` is for rows like PCQ's raw-code output, where
  a single line is ONE arm and the paste-in step decides which side of the ledger it belongs on (and
  therefore which pair of contract columns — `test_*` or `control_*` — actually gets a nonzero value on
  that row; the other pair is left 0/blank).
- **`success_pick`** — free-text note, e.g. `responder_cli`, `approved_asc`, `completed_raw`. Documents
  which of a block's success variants was transcribed into `test_successes`/`control_successes` for
  that row. PCQ ships FOUR success variants (`approved_asc`, `completed_asc`, `approved_raw`,
  `completed_raw`) — Andre picks one pair per test contrast; this column records which.

All downstream formulas (`PAIRED`, `TESTS`) pool `test_role`+`both` rows into the treated-arm totals and
`control_role`+`both` rows into the control-arm totals, so PCL's pre-paired rows and PCQ's split rows
both feed the same math without special-casing.

## PAIRED sheet — pooling cohort_months, pairing arms

One row per `(mne, test_desc, stratum)`, pooling every `cohort_month` for that combination:
`n1`/`x1` (treated clients/successes), `n0`/`x0` (control clients/successes), then the per-stratum
building blocks for a stratified (Cochran-Mantel-Haenszel-style weighted) two-proportion test:
`p1=x1/n1`, `p0=x0/n0`, `d=p1-p0`, `w=n1*n0/(n1+n0)`, `pbar=(x1+x0)/(n1+n0)`,
`var=pbar*(1-pbar)*(1/n1+1/n0)`, plus helper columns `wd=w*d` and `w2var=w^2*var` that `TESTS` sums
across strata. All ratios are `IF`-guarded against division by zero.

## TESTS sheet — one row per test contrast

Manual descriptive fields (`mne`, `DESC`, `Type`, `test_desc`, `success_name`, `Reference Document`,
`Notes`) plus computed fields via `SUMIFS`/formulas over `INPUT`/`PAIRED`:
- `Leads` = `SUMIFS` of `test_clients` where `arm_role` is `test` or `both`, for that `mne`+`test_desc`
  — pools across every stratum and cohort_month automatically.
- `trt_start_dt`/`trt_end_dt` = `MINIFS`/`MAXIFS` over `INPUT` for that `mne`+`test_desc`.
- Stratified stats: `lift` (raw proportion) `= SUM(wd over matching PAIRED rows) / SUM(w over matching
  PAIRED rows)`; `se = SQRT(SUM(w^2*var)) / SUM(w)`; `z = lift / se`;
  `p_value = 2*(1-NORM.S.DIST(ABS(z), TRUE))`; `Significance = IF(p_value<0.05,"Y","N")`.
  For a single-stratum test (e.g. PCL, `stratum='overall'` only) this algebraically reduces to the
  plain two-proportion z-test — no special-casing needed.
- Supporting/audit columns (`p1`, `p0`, `se`, `z`, `p_value`) sit to the LEFT of a blank separator
  column, which sits to the left of the REPORT-mapped columns (`Leads`, `Lift` in pp, `Significance`) —
  so the audit trail is visible but visually separated from what actually feeds the partner sheet.
- `Lift` is displayed as percentage points (`lift * 100`, format `0.00"pp"`).

## REPORT sheet — the partner's 12-column layout

Formula-linked from `TESTS`, ready to copy-paste into the partner workbook:

| Partner column | Source |
|---|---|
| MNE | `TESTS.mne` |
| DESC | `TESTS.DESC` (manual) |
| Type | `TESTS.Type` (manual) |
| Test Desc | `TESTS.test_desc` |
| Treatment Start Date | `TESTS.trt_start_dt` |
| Treatment End Date | `TESTS.trt_end_dt` |
| Success | `TESTS.success_name` |
| Leads/Unique Clients | `TESTS.Leads` |
| Lift | `TESTS.Lift_pp` |
| P-value/Significance | `TESTS.p_value` and `TESTS.Significance` combined into one text cell |
| Reference Document | `TESTS.reference_document` (manual) |
| Notes | `TESTS.notes` (manual) |

`DESC`, `Type`, `Reference Document`, and `Notes` are entered by hand in `TESTS` — they're business
framing, not something derivable from the SQL output.

## Worked-example check

`TESTS`/`PAIRED`/`INPUT` all carry a hardcoded `EXAMPLE` test contrast
(`n1=1000, x1=60, n0=1000, x0=40`) so the formula chain is checkable on open without live data:
`p1=0.06, p0=0.04, lift=2.00pp, se≈0.009747, z≈2.052, p≈0.0402, Significance=Y`. See the build script's
run log / the agent report for the Python-side verification of this same math.

## Open decisions inherited from `campaigns/sales_modal/`

Both still apply and are NOT resolved by this pipeline — see `campaigns/sales_modal/README.md`:
1. **Period-ASC gating** (PCQ only) — general canon gates success numerators to
   `TRIM(asc_on_app_source)='Period-ASC'`; Andre's 2026-06 instruction for the MS descriptive read was
   to leave it ungated. `pcq_ms_block.sql` ships BOTH (`*_asc` and `*_raw`) — pick one via `success_pick`
   when pasting into INPUT.
2. **Population split** (PCQ only) — `test_group_latest` (assignment/ITT) vs. `ms_targeted` (delivery,
   post-assignment, self-selected). `pcq_ms_block.sql` uses assignment (`test_group_latest`) because
   only ITT is valid for a lift-flavored read — this is fixed for this pipeline, not left open.

## For teammates adding a new block

1. Write your block's SQL in whatever engine/logic fits your campaign. Output exactly the 11 contract
   columns (or, if test/control can't be split at build time, one row per raw arm code — see PCQ).
2. Counts only. Unique clients. `test_clients`/`test_successes` = treated arm, never inverted.
3. Keep `cohort_month` even if you plan to pool immediately — it's a repo-wide hard rule.
4. Give your `test_desc` string ONE fixed, exact value across every row of your test — it's the pooling
   key in `PAIRED`/`TESTS`.
5. Paste your output into `INPUT`, tag `arm_role` (`test`/`control`/`both`) and `success_pick`, add a row
   to `PAIRED` (one per stratum) and `TESTS` (one per test contrast) by copying the formula pattern from
   the existing PCL/PCQ rows and swapping the `mne`/`test_desc`/`stratum` filter values.
