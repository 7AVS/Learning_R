# Handoff prompt for the VS Code agent.
# Not executable logic - this file exists so the prompt travels to the work environment, where
# only .py and .sql sync. Print it, or copy the string below.
#
# Regenerated 2026-07-31 after the pipeline was rebuilt. The previous version described CSV cubes
# that no longer exist.

PROMPT = r"""
You are analysing an already-extracted dataset. Do this work yourself with your own tools - do NOT
spawn agents. Everything you need is in local parquet; there is no database to query.

# WHAT YOU HAVE

Four parquet datasets in `spotlight_parquet/`, extracted 2026-07-31 from RBC's email vendor
feedback tables. Read them with duckdb, not pandas - it queries off disk with no RAM ceiling.

    import duckdb
    duckdb.sql("SELECT * FROM 'spotlight_parquet/clientagg_v6/bite_*/*.parquet' LIMIT 5").df()

Note the `bite_*/` - data sits one level inside each bite folder. `_ROWCOUNT` files are resume
markers, a few bytes each; ignore them.

**`clientagg_v6`** - one row per client, BANK-WIDE, ~12.3M rows. The main table.
`clnt_no, n_campaigns_all, n_campaigns_branded, n_campaigns_cards, n_campaigns_regulatory,
n_emails_all, n_emails_3m, n_emails_6m, n_emails_promo_6m, n_emails_regulatory_6m,
n_emails_cards, n_emails_cards_6m, unsub_flag, unsub_dt, first_send_dt, last_send_dt`

**`cards_pair_v6`** - one row per client x cards campaign. CARDS MNEs ONLY.
`clnt_no, mne, n_sends, unsub_flag, unsub_dt, first_send_dt, last_send_dt`

**`mne_agg_v6`** - campaign x month aggregates, bank-wide, all ~431 mnemonics.
`mne, cohort_month_num, clients_mailed, sends, unsub_clients`
**These are PER-BITE PARTIAL aggregates. You must SUM across bites to get true totals.** Clients
are partitioned disjointly by `MOD(ABS(clnt_no), 10)`, so summing `clients_mailed` across bites is
exact, not an approximation. Reading one bite gives you a tenth of reality.

**`ucp_enriched`** - one row per client, ~12.3M rows.
`clnt_no, age_band, tenure_band, prod_cat_cnt, tenure_years, prof_tot_annual`
Join to the others on `clnt_no`.

Window: sends and unsubs between **2025-08-01 and 2026-08-01**.

CARDS_MNES (12): PCQ PCL PCD AUH CLI CRV VBA VBU CRO CEC VIF MET
REGULATORY_MNES (22): AFD BPU BUK CFR EOE FNE FSA FSO FXR GAF HFC HPN IOO NST OTC PUK ROP TWI
VMF VOA ZDC ZHX

# THE BRIEF

**Spotlight 1**
- 1a - Unsubs by cards campaign: absolute number and rate. Plus each campaign's contact cadence
  (weekly / bi-weekly / monthly?).
- 1b - Unsubs by the number of campaigns a client was contacted by, and by contact frequency.
- 1c - Do certain campaigns drive unsubs alone, or do unsubs spike when campaigns run together?

**Spotlight 2**
- Unsubs by product depth - bucket by number of products held, 1 through 4. Among unsubscribers,
  how deep is their relationship with the bank?

**Workstream 2** (later, needs data not in these files)
- Unsub ratio by LOB; top ~10 campaigns; impact on cards profit split by spend tier (H/M/L) and
  revolver vs transactor. Spend and revolver/transactor are NOT in this extract - they need a
  separate pull from `D3CV12A.DLY_FULL_PORTFOLIO` and `D3CV12A.CR_CRD_RPTS_ACCT`.

# WHAT WE ALREADY PROVED - do not re-derive, do not contradict

- **The unsubscribe is PER-LIST, not a global opt-out.** Of unsubscribers touching 2+ campaigns,
  ~97% carry a distinct timestamp per campaign; global fan-out is under 1%. 69% touch exactly one
  campaign. So per-campaign attribution is real and you may say a campaign "drove" an unsub.
- **Population anchors:** 308,104 distinct unsubscribers and 753,608 unsub events over the 12
  months. Cross-validates an independently catalogued 319,733 to within 3.6%. If a number you
  compute implies something wildly different, you have a bug, not a finding.
- **Domain anchor:** unsub rates run 0.2-0.6% per deployment, under ~5,000 clients even at peak.
  Sanity-check every rate against this before reporting it.
- **Send-to-unsub lag decays hard** - per day: 23,118 same-day, then 8,233, 4,052, 3,243, 2,620,
  2,298, and 476/day beyond 30 days. A 30-day attribution window captures 71.5% of unsub events.
- **Scope:** campaign ids that are not 10 chars with 7 leading digits (`DEFAULT`, `CABVRSN1` and
  27 others) were excluded - about 6% of send volume. Say so if you report campaign counts.

# TRAPS - each of these silently produces a wrong answer

1. **`mne_agg_v6` is partial per bite. SUM ACROSS BITES.** Forgetting this understates everything
   by 10x and the numbers still look plausible.
2. **`prod_cat_cnt` can be the string `"no_ucp_match"`** - roughly 12% of clients, and it is NOT
   random: leavers match UCP at 90.9%, stayers at 87.9%. Report it as its own row. Dropping it
   biases every product-depth and profitability cut, which is the whole of Spotlight 2.
3. **`cards_pair_v6` is cards-only.** Any denominator for total contact load must come from
   `clientagg_v6`, which is bank-wide. A client hit by a cards campaign is simultaneously hit by
   others, and total load is what is being measured.
4. **Counts only in, rates computed by you.** Always show numerator and denominator beside any
   rate. Never present a rate alone.
5. **Suppress small cells.** Any rate on fewer than ~100 clients in the denominator is noise. Do
   not rank campaigns on a 12-client base.
6. **`prof_tot_annual` is a UCP field that is bank-wide and formally undefined** - current-year vs
   lifetime was never resolved. Label it a proxy wherever it appears. There is no cards-specific
   profit column in this extract.

# WHAT TO PRODUCE

Small, labelled tables that can be read on screen and pasted into a deck. Not a data dump.
Print the grain and row count above each one.

- **1a** - one row per cards campaign: `clients_mailed, sends, unsub_clients, unsub_rate,
  sends_per_client`. Sorted by rate. Add bank-wide non-cards campaigns as context rows so cards
  can be compared against the rest of the bank.
- **1a cadence** - `n_sends` and the span between `first_send_dt` and `last_send_dt` per client x
  campaign give an average gap; take the median across clients per campaign. Translate to plain
  language (~7 weekly, ~14 bi-weekly, ~30 monthly). **Check how many clients got only ONE send
  first** - if most did, the cadence figure describes a repeat-contacted minority and must be
  labelled that way.
- **1b** - unsub rate by `n_campaigns_branded` bucket, and separately by `n_emails_6m` bucket.
  This is the dose-response question. Show the 1-D marginals before any cross-tab.
- **1c** - `cards_pair_v6` supports this directly: for each pair of cards campaigns, clients
  exposed to both vs either alone, and their unsub rates. Compute lift as
  `rate_both / max(rate_a_alone, rate_b_alone)`. Suppress pairs under ~100 clients.
- **Spotlight 2** - unsub rate by `prod_cat_cnt` (0,1,2,3,4,no_ucp_match), then the same for
  cards-exposed clients only, then `prof_tot_annual` for leavers vs stayers by depth. **If leavers
  are worth more than stayers, that is the headline** and it should be unmissable.
- **A caveats cell** at the end: what the data cannot answer, one line each.

# HOW TO WORK

Write duckdb SQL, not pandas chains. One question, one query, one small table. SQL is readable and
checkable; a wrong WHERE clause is visible, a wrong pandas chain is not.

Never invent a column. If something you need is not in the schemas above, say so and stop - do not
substitute a proxy without flagging it.

When done, report which brief items you answered, which you could not, and what data would close
each gap.
"""

if __name__ == "__main__":
    print(PROMPT)
