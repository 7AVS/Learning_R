# Email draft: VBU model test proposal

**To:** {campaign owner / CIDM contact}
**Subject:** VBU propensity model: holdout test proposal

---

Hi {name},

We'd like to run a clean holdout test on the VBU model-based offers (AIB_25K_NR and
AIB_25K_R_55). Short version below, one-pager attached.

What we've seen in the June and July waves: communicated clients convert at 1.79% (NR) and
2.41% (R_55), and 9 of 10 upgrades come from the model's top 4 score bands. Not-communicated
clients: 0 upgrades out of 2,146. So the offer looks like it drives everything, but we can't
certify that yet. The current not-communicated group isn't a verified random control (its
share runs ~8% in the top decile vs ~4% in decile 4 for NR, every wave), so today's data
can't give us a causal number for the model.

The proposal: next wave, split the model-based population 70/30 at client level. 70% get the
offer as usual, 30% get no VBU touch. Run it for 2 waves and read each ~80 days after deploy.
Cost is roughly 100-160 upgrades not made per wave. In return we get the causal value of the
model-targeted communication and a calibration read by score band, which also tells us if the
score cutoff sits in the right place.

Two questions before we lock the design:

1. Is the current ~5% not-communicated group a deliberate random holdout, and if so how is it
   drawn? The score-linked share above is what prompted the question.
2. Can the assignment mechanism run a fresh client-level random 70/30 within the model-based
   offers, with the held-out 30% receiving nothing at all (not moved to another offer or
   path)?

Happy to walk through the design. Full write-up is on the Confluence page.

Thanks,
Andre
