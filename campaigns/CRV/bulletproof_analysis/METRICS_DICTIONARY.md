# CRV ↔ PCL — Metrics Dictionary

One row per metric. Purpose: keep every "count" distinct and labelled by its
**grain**, so the gain side, the loss side, and the value multipliers never get
conflated. If a number isn't in this table, don't put it in the net.

## Grains (the three units that matter)
- **lead** = one (account × PCL-mobile deployment). Same client = multiple leads.
- **client** = one distinct account (deduped).
- **plan** = one installment plan (one row in `cards_crv_install_details`).

---

## PCL gain side

| Metric | Source | Grain | Means | Valid for |
|---|---|---|---|---|
| `pcl_responders` | Q04 / Q06 | lead | PCL leads that converted | PCL recovery rate & gross $ (× $675/conv) |
| PCL gap 1.08pp | Q04 | lead | Control − Action PCL rate | the causal cannibalization claim |

## CRV loss side

| Metric | Source | Grain | Means | Valid for |
|---|---|---|---|---|
| `crv_responders` (flag) | Q06 | lead-flag | PCL-lead windows with ≥1 CRV co-conversion (binary, capped at 1) | the cannibalization **rate** only — **NOT** a $ basis |
| `n_exposed_clients` | **Q13** | client | distinct accts with any CRV wave overlapping PCL | denominator for client-rate |
| `n_converting_clients` | **Q13** | client | distinct CRV converters in overlap | CRV loss valued **per client** |
| `n_install_plans` | **Q13** | plan | distinct installment plans of those converters | CRV loss valued **per plan** |

## CRV value multipliers (characterization, not counts to multiply blindly)

| Metric | Source | Grain | Note |
|---|---|---|---|
| `mean_principal_per_acct` (~$6.7k) | Q05 | client | total installment $ per converter, cumulative |
| `mean_txn_principal` (~$980) | Q05 | plan | per-plan principal |
| plans/converter (~6.85) | Q05 derived | — | = principal_per_acct ÷ txn_principal |
| `n_transactions` / `txns_per_acct` | Q05 | — | ⛔ POISONED by 1.74× fanout — do not quote. Use Q13 `n_install_plans` instead |

---

## How the net is built (grain-matched)

1. Pick the grain of William's CRV NIBT: **per plan** or **per converting client**.
2. Incremental CRV loss = (Action rate − Control rate) × base, computed **at that
   same grain** using Q13:
   - per **plan**:  use `n_install_plans` ÷ `n_exposed_clients` per arm → Δ × base
   - per **client**: use `n_converting_clients` ÷ `n_exposed_clients` per arm → Δ × base
3. **Never** multiply Q06's binary `crv_responders` by a per-plan value, and
   **never** multiply incremental conversions by the total ~6.85 plans/client
   (that mixes an incremental numerator with a total-book multiplier → overstates loss).
4. CRV $ loss − PCL $ gain (gross) = net. All $ math in Excel.
