"""Column-algebra checker for unsub_value_museum.py.  RUN BEFORE EVERY PUSH:  python colcheck.py

Replays the notebook's analysis cells against a mock Spark that tracks ONLY column names.

WHY IT EXISTS. On 2026-07-29 a join put two columns named `mne` on one frame - senders_wide's
"campaign that MAILED the client" and banded's "campaign the client LEFT THROUGH". groupBy("mne")
could not resolve it. That was found after a 40-minute pull, which is the wrong place to find it:
nothing about the bug needed data. It was visible in the column names alone.

CATCHES  ambiguous references after a join, missing columns, bad join keys, unionByName mismatch.
MISSES   wrong values, bad SQL, performance, anything that needs a real row.

VERIFIED BY NEGATIVE CONTROL. Re-inject the mne bug and this must exit 1. A checker that cannot
fail is worse than none, because it reads like proof. If you change the mock, re-run the control:
    python -c "import re,subprocess,sys; \
      s=open('unsub_value_museum.py',encoding='utf-8').read(); \
      b=s.replace('.drop(\"mne\", \"program\"),',','); \
      open('_bugged.py','w',encoding='utf-8').write(b); \
      c=re.sub(r'^PATH = .*$','PATH = \"_bugged.py\"',open('colcheck.py',encoding='utf-8').read(),1,re.M); \
      open('_chk.py','w',encoding='utf-8').write(c); \
      sys.exit(0 if subprocess.run([sys.executable,'_chk.py']).returncode else 1)"
"""
import os
import re
import sys

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "unsub_value_museum.py")
ERRORS = []


class Col:
    def __init__(self, name, df=None):
        self.name, self.df = name, df
        if df is not None and name in df.ambiguous:
            ERRORS.append("AMBIGUOUS reference to '%s' (frame has it twice after a join)" % name)
        if df is not None and name not in df.cols and name not in df.ambiguous:
            ERRORS.append("MISSING column '%s' (frame has: %s)" % (name, sorted(df.cols)))

    def alias(self, n):
        return Col(n)

    def cast(self, *a):
        return self

    def __getattr__(self, item):
        # isin / isNotNull / isNull / desc / asc_nulls_last / otherwise ... all return a Col
        return lambda *a, **k: self

    def _op(self, other):
        return Col(self.name)

    __eq__ = __ne__ = __lt__ = __gt__ = __le__ = __ge__ = _op
    __and__ = __or__ = __add__ = __sub__ = __mul__ = __truediv__ = __rtruediv__ = _op
    __radd__ = __rmul__ = __rsub__ = _op
    __invert__ = lambda self: self
    __hash__ = object.__hash__


class Agg:
    def __init__(self, df, keys):
        self.df, self.keys = df, keys

    def agg(self, *exprs):
        out = [k.name if isinstance(k, Col) else k for k in self.keys]
        out += [e.name for e in exprs]
        return DF(out, self.df.name + ".agg")

    def count(self):
        return DF([k.name if isinstance(k, Col) else k for k in self.keys] + ["count"], "cnt")

    def pivot(self, c, vals=None):
        if isinstance(c, str):
            Col(c, self.df)
        return self


class DF:
    def __init__(self, cols, name="df", ambiguous=None):
        self.cols = list(dict.fromkeys(cols))
        self.name = name
        self.ambiguous = set(ambiguous or [])

    # --- shape-preserving no-ops -------------------------------------------------
    def _same(self, *a, **k):
        return self

    filter = where = orderBy = sort = limit = cache = persist = repartition = _same
    coalesce = _same
    distinct = dropDuplicates = _same
    checkpoint = hint = _same

    @property
    def columns(self):
        return list(self.cols)

    def count(self):
        return 1

    def collect(self):
        class R(dict):
            def __getitem__(self, k):
                return 1
            def get(self, k, d=None):
                return 1
        return [R(), R()]

    def toPandas(self):
        return None

    @property
    def write(self):
        return self

    @property
    def read(self):
        return self

    def mode(self, *a):
        return self

    def option(self, *a):
        return self

    def parquet(self, *a):
        return DF(["_"], "readback")

    def csv(self, *a):
        return DF(["_"], "readback")

    # --- column algebra ----------------------------------------------------------
    def select(self, *cs):
        out = []
        for c in cs:
            if isinstance(c, str):
                if c == "*":
                    out += self.cols
                    continue
                Col(c, self)
                out.append(c)
            elif isinstance(c, Col):
                out.append(c.name)
            elif isinstance(c, (list, tuple)):
                for x in c:
                    out.append(x.name if isinstance(x, Col) else x)
        return DF(out, self.name + ".select")

    def withColumn(self, n, _c):
        return DF(self.cols + [n], self.name, self.ambiguous)

    def withColumnRenamed(self, a, b):
        return DF([b if c == a else c for c in self.cols], self.name, self.ambiguous)

    def drop(self, *cs):
        gone = set(cs)
        return DF([c for c in self.cols if c not in gone], self.name,
                  self.ambiguous - gone)

    def groupBy(self, *keys):
        flat = []
        for k in keys:
            flat += list(k) if isinstance(k, (list, tuple)) else [k]
        for k in flat:
            if isinstance(k, str):
                Col(k, self)
        return Agg(self, flat)

    def join(self, other, on=None, how="inner"):
        keys = [on] if isinstance(on, str) else (list(on) if on else [])
        for k in keys:
            if k not in self.cols:
                ERRORS.append("JOIN key '%s' not on left (%s): %s" % (k, self.name, sorted(self.cols)))
            if k not in other.cols:
                ERRORS.append("JOIN key '%s' not on right (%s): %s" % (k, other.name, sorted(other.cols)))
        dupes = (set(self.cols) & set(other.cols)) - set(keys)
        return DF(self.cols + [c for c in other.cols if c not in keys],
                  self.name + "+" + other.name,
                  self.ambiguous | other.ambiguous | dupes)

    def unionByName(self, other, allowMissingColumns=False):
        if not allowMissingColumns and set(self.cols) != set(other.cols):
            ERRORS.append("unionByName MISMATCH\n     left  only: %s\n     right only: %s"
                          % (sorted(set(self.cols) - set(other.cols)),
                             sorted(set(other.cols) - set(self.cols))))
        return DF(self.cols, self.name + "|union")

    def approxQuantile(self, c, probs, rel):
        Col(c, self)
        return [0.0] * len(probs)

    def __getattr__(self, item):
        return lambda *a, **k: self


class Fns:
    @staticmethod
    def col(n):
        return Col(n)

    @staticmethod
    def lit(v):
        return Col("lit")

    @staticmethod
    def expr(e):
        return Col("expr")

    def __getattr__(self, name):
        def f(*a, **k):
            for x in a:
                if isinstance(x, str) and name in ("sum", "max", "min", "avg", "count",
                                                   "countDistinct", "round", "first"):
                    pass
            return Col(name)
        return f


def run():
    src = open(PATH, encoding="utf-8").read()
    cells = re.split(r"^# %% ", src, flags=re.M)
    by_id = {}
    for c in cells[1:]:
        cid = c.split("]")[0].lstrip("[")
        by_id[cid] = c[c.index("\n") + 1:]

    F = Fns()

    # ---- real starting schemas, taken from the file's own definitions ------------
    UCP = ["AGE", "TENURE_RBC_YEARS", "T_TOT_CNT", "I_TOT_CNT", "B_TOT_CNT", "C_TOT_CNT",
           "PROF_TOT_ANNUAL"]
    BANDS = ["prod_cnt", "prod_band", "tibc_mix", "tenure_band", "age_band", "prof_quintile",
             "high_potential", "band_v3"]
    CONTACT = ["n_send_events", "n_opens", "n_clicks", "opened", "clicked", "contact_band",
               "engagement", "breadth_upper", "breadth_lower", "breadth_band"]
    BANDED = (["CLNT_NO", "bucket", "mne", "program", "TREATMENT_ID", "unsub_tm", "ucp_matched"]
              + UCP + BANDS + CONTACT)

    g = {
        "F": F, "spark": DF([], "spark"), "Window": type("W", (), {"partitionBy": staticmethod(lambda *a: None)}),
        "pd": type("pd", (), {"DataFrame": staticmethod(lambda *a, **k: None)}),
        "T": lambda *a, **k: None, "print": lambda *a, **k: None,
        "display": lambda *a: None, "Markdown": lambda s: s,
        "banded": DF(BANDED, "banded"),
        "matched": DF(BANDED, "matched"),
        # seeded frame removed - built by the cells
        "senders_wide_raw": DF(["CLNT_NO", "mne", "program"], "senders_wide_raw"),
        "senders_cards_raw": DF(["CLNT_NO", "mne", "program"], "senders_cards_raw"),
        "senders_mne": DF(["mne", "senders", "program"], "senders_mne"),
        "_leavers_by_mne": DF(["mne", "program", "unsubs"], "_leavers_by_mne"),
        "cadence_mne": DF(["mne", "n_deployments", "send_days", "first_send_dt", "last_send_dt"],
                          "cadence_mne"),
        # seeded frame removed - built by the cells
        "add_mne_program": lambda df, tid_col=None, mne_col=None: df.withColumn("mne", None).withColumn("program", None),
        "stage": lambda name, build, requires=None: build(),
        "landed": lambda n: True,
        "HAVE_WIDE": True, "HAVE_CADENCE": True, "HAVE_CONTACT": True, "HAVE_PRIOR_DETAIL": True,
        "BREADTH_COL": "n_mnes", "BREADTH_IS_CAMPAIGNS": True, "BREADTH_LABEL": "campaigns",
        "SAMPLE_MOD": 10, "CARDS_MNES": frozenset({"PCQ", "PCL", "PCD", "AUH", "CLI", "MVP", "CRV"}),
        "BASE": "/x/", "WIN_START": "2026-03-01", "WIN_END": "2026-06-01",
        "PROF_CUTS": [0, 1, 2, 3], "HP_AGE": 35, "HP_TENURE": 5, "HP_PRODS": 2,
        "PROVEN": True, "REGULATORY_MNES": frozenset({"FXR","OTC","VMF","VOA"}), "UCP_ANCHOR": "2026-02-28", "PULL_CADENCE": True, "SAMPLE_MOD2": 10, "RUN_CLEANUP": False, "BAND_VERSION": 3, "BAND_STAMP": "band_v3", "_n_lv": 1, "_n_st": 1,
        "_WIN_DAYS": 92, "_bucket_counts": __import__("pandas").DataFrame({"bucket": ["leaver", "stayer", "already_out"], "count": [1, 2, 3]}), "_n_mailed": 1, "_n_matched": 1,
        # seeded frame removed - built by the cells
        # seeded frame removed - built by the cells
        "sum": sum, "len": len, "set": set, "sorted": sorted, "str": str, "int": int,
        "list": list, "any": any, "all": all, "range": range, "dict": dict, "enumerate": enumerate,
        "MAX_MNE_PER_CLIENT": 25, "MIN_PAIR_LEAVERS": 30, "PULL_WIDE": True,
    }

    for cid in ["17", "18", "19", "20", "20b", "20d", "20e", "20f", "20g", "20h", "20i", "20c", "21", "21b", "22", "23"]:
        if cid not in by_id:
            print("!! cell [%s] not found" % cid)
            continue
        before = len(ERRORS)
        try:
            exec(compile(by_id[cid], "<cell %s>" % cid, "exec"), g)
        except Exception as e:
            ERRORS.append("cell [%s] RAISED %s: %s" % (cid, type(e).__name__, e))
        n = len(ERRORS) - before
        print("cell [%-4s] %s" % (cid, "OK" if n == 0 else "%d problem(s)" % n))

    print("")
    if ERRORS:
        for e in ERRORS:
            print("  X " + e)
        sys.exit(1)
    print("COLUMN ALGEBRA CLEAN - all analysis cells")


run()
