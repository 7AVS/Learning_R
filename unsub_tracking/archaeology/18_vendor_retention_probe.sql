-- 18: Vendor feedback retention probe — how far back does coverage go?
-- ENGINE: Teradata-direct.


-- Block 1 — MASTER, quarterly grain (load_tm = retention proxy, not a send date)
SELECT
    EXTRACT(YEAR FROM m.load_tm) * 10
      + ((EXTRACT(MONTH FROM m.load_tm) - 1) / 3 + 1) AS yyyyq,
    COUNT(*)                    AS n_rows,
    COUNT(DISTINCT m.CLNT_NO)   AS n_distinct_clients,
    MIN(m.load_tm)              AS min_load_tm,
    MAX(m.load_tm)              AS max_load_tm
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
GROUP BY 1
ORDER BY 1;


-- Block 2 — EVENT, quarterly grain (disposition_dt_tm = true event timestamp)
SELECT
    EXTRACT(YEAR FROM e.disposition_dt_tm) * 10
      + ((EXTRACT(MONTH FROM e.disposition_dt_tm) - 1) / 3 + 1) AS yyyyq,
    COUNT(*)                              AS n_rows,
    COUNT(DISTINCT e.consumer_id_hashed)  AS n_distinct_clients,
    MIN(e.disposition_dt_tm)              AS min_disposition_dt,
    MAX(e.disposition_dt_tm)              AS max_disposition_dt
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
GROUP BY 1
ORDER BY 1;
