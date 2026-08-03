# matched_check_prompt.py - v2: the mean-reversion check, CSV-ONLY.
# Runs on b_before_after_cube.csv (already delivered). No parquet, no HDFS,
# no new data. Supersedes the previous parquet-based version entirely.

PROMPT = """
# MATCHED-TIER CHECK - does the growth-stall claim survive mean reversion?
# ONE query on b_before_after_cube.csv. Nothing else. No HTML.

The objection: "you picked high spenders, high spenders drift down - that's
reversion, not an unsub effect." The answer lives in the cube you already
have: tier_then is a THEN-spend band (terciles), tier_now is where the same
clients ended up. Compare leavers vs stayers WITHIN the same starting tier.

1. DESCRIBE the file first; use the actual column names.
2. For tier_then = 'High': for stayers and for cards-unsubscriber groups
   (LEAVERS_ALL; use the group column as it exists), the distribution of
   tier_now (stayed High / fell to Mid / fell to Low / untiered) as % of
   that group's High-then clients, with ns. Exclude 'untiered' tier_then
   rows; report their n in a footnote.
3. Repeat for tier_then = 'Mid'.
4. One table: tier_then x group x tier_now share (+ns). Skip any cell
   under 300 clients and say so.

READ IT THIS WAY (print with the result):
- Leavers fall out of their starting tier MORE than stayers who started in
  the same tier -> reversion refuted; the stall story stands, matched.
- Same fall-out rates -> the aggregate gap was reversion/composition; the
  deck keeps the honest version (first-contact discipline, suppression
  fix, blast guardrail) with no spend-stall claim.

Descriptive wording only. Counts + % with n. Touch nothing else.
"""

if __name__ == "__main__":
    print(PROMPT)
