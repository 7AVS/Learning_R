# Prompt for the VS Code agent that builds the CSV-side analysis notebook.
# Not executable logic - this file exists so the prompt travels to the work environment,
# where only .py and .sql are synced. Print it, or copy the string below.

PROMPT = r"""
You are building an analysis notebook. Do this work yourself with your own tools — do NOT spawn agents.

## What exists

1. `spotlight.py` — a cell-based PySpark script that pulled the data and wrote the CSVs. **Read it first.** It documents the source tables, the unsub definition, the join path, and every derivation. Do not re-derive anything it already establishes; do not modify it.
2. The CSVs it produced, in `<<CSV_DIR>>`.

## What to build

ONE new Jupyter notebook that reads those CSVs with **pandas only** — no Spark, no database connection. Everything needed is already in the files.

Its job is to answer a specific brief. Nothing else.

## The brief (this is the entire scope)

From the campaign lead, due for a quarterly "Power Pack" deck. Two spotlights:

**Spotlight 1**
- **1a** — Unsubs by cards campaign: absolute number and rate. Plus each campaign's contact cadence (is it weekly, bi-weekly, monthly?).
- **1b** — Unsubs by the number of campaigns a client was contacted by, and by contact frequency.
- **1c** — Do certain campaigns drive unsubs on their own, or do unsubs spike when specific campaigns run together?

**Spotlight 2**
- Unsubs by product depth — bucket clients by number of products held, 1 through 4. Among the people who unsubscribe, how deep is their relationship with the bank?

Out of scope: value-of-an-unsub, before/after studies, consent/preference-centre analysis. Do not drift into them.

## The CSVs

Read the actual headers before writing code — do not trust this summary over the files. Expected shape:

**`cube1_profiling`** — the profiling cube
- Dims: `mne, is_cards, is_regulatory, age_band, tenure_band, prod_cat_cnt`
- Measures: `stayers, leavers, clients_total, mean_tenure_stayers, mean_tenure_leavers, median_prof_stayers, median_prof_leavers`

**`cube2_frequency`** — the contact-frequency cube, one row per client-segment cell
- Dims: `n_branded_campaigns_bucket, n_emails_3m_bucket, n_emails_6m_bucket, n_promo_emails_6m_bucket, n_regulatory_emails_6m_bucket, prod_cat_cnt`
- Measures: `stayers, leavers, clients_total`

**`q_trend`** — per-campaign entry-cohort trend, keyed on `mne` and `cohort_month`

**`q_emails_all_summary`** — one row: total and median emails, stayers vs leavers

**`ucp_match_by_mne`** — per-campaign UCP match rate, a data-quality diagnostic

## Rules that will make or break the output

1. **`is_cards` is a tag, not a filter.** The pull was bank-wide, every mnemonic in the bank. `is_cards = 1` marks the 12 cards campaigns. Cards is the lens for every finding — but the *denominator* for contact load must stay bank-wide, because a client hit by a cards campaign is simultaneously hit by other campaigns and total contact load is what's being measured. Never silently drop `is_cards = 0` rows from a frequency calculation.

2. **The cubes are wide.** `stayers` and `leavers` are columns, not values of a bucket dimension. Unsub rate = `leavers / clients_total`. Never sum `clients_total` across rows and divide by a separately-summed numerator without checking you aggregated both consistently.

3. **`prod_cat_cnt` is 0–4, stored as a string, and can be the literal `"no_ucp_match"`.** That is a real category — roughly 9–13% of clients, higher among stayers than leavers. **Report it as its own row. Never drop it, never coerce it to 0.** Dropping it biases every product-depth cut, which is the whole of Spotlight 2.

4. **Counts only in, rates computed in the notebook.** Always show numerator and denominator next to any rate you compute. Never show a rate alone.

5. **Every output must be self-describing.** Print the grain and row count above each table. A reader who sees only the printed output should know what they're looking at.

6. **Suppress small cells.** Any rate computed on fewer than ~100 clients in the denominator is noise. Flag it or drop it — do not rank campaigns on a 12-client base.

## What the notebook should produce

Small, labelled tables — the kind that can be read on screen and pasted into a deck. Not a data dump.

- **1a** — one row per cards campaign: `clients_mailed, sends, unsub_clients, unsub_rate, sends_per_client, median_days_between_sends`. Sorted by rate. Add the bank-wide non-cards campaigns as context rows so cards can be compared against the rest of the bank.
- **1a cadence** — translate `median_days_between_sends` into a plain-language label (~7 weekly, ~14 bi-weekly, ~30 monthly). **Check `clients_used_for_gap` against `clients_mailed` first** — if most of a campaign's audience got exactly one email, the cadence figure describes only a repeat-contacted minority and must be labelled as such.
- **1b** — unsub rate by `n_branded_campaigns_bucket`, and separately by `n_emails_6m_bucket`. This is the dose-response question: does contact load predict leaving? Show the marginals as their own small tables before showing any cross-tab.
- **1c** — the cubes do not carry campaign pairs. Say so plainly rather than inventing it. What *can* be answered: whether clients exposed to more campaigns leave at higher rates, which is `n_branded_campaigns_bucket` from 1b. State the limitation explicitly in the notebook.
- **Spotlight 2** — unsub rate by `prod_cat_cnt` (0, 1, 2, 3, 4, no_ucp_match). Then the same cut for cards clients only. Then `median_prof_leavers` vs `median_prof_stayers` by depth — if leavers are worth more than stayers, that is the headline and it needs to be unmissable.
- **A caveats cell** at the end: what the data cannot answer, with a one-line reason each.

## Open questions to carry forward, not resolve

State these in a markdown cell in the notebook. Do not try to answer them from the CSVs.

- It is **not established** whether an unsubscribe closes only that campaign's list or all marketing email from the bank. If it is global, then "unsubs by campaign" means "which campaign's email was the last straw", not "which campaign causes unsubs" — and the campaign that mails the most will top the table mechanically. Word every 1a finding so it survives either answer.
- The regulatory-campaign list is hardcoded, not joined from a governed source. It goes stale when a campaign is reclassified.
- `median_days_between_sends` is an approximation: `(last_send − first_send) / (n_sends − 1)` per client, then the median across clients. It assumes even spacing, so a burst of emails followed by silence reads the same as a steady cadence.

## Deliverable

One `.ipynb`. Cells self-contained and re-runnable. Minimal comments. No HTML, no deck, no charts unless a chart genuinely beats a table.

When done, report: which brief items you could answer, which you could not, and what data would be needed to close each gap.
"""

if __name__ == "__main__":
    print(PROMPT)
