-- 40 — schema probes: hunting SFMC send-level data (jid) in the warehouse
-- Teradata-direct, plain SQL. Run statements one at a time, top to bottom.
-- [1]-[3] = dictionary sweeps (instant). [4] = metadata per candidate pattern.
-- [5] = per-finalist freshness. SAMPLE any hit before believing its name.


/* ============================================================================
[1] Columns that smell like a send-job id, anywhere in the warehouse
============================================================================ */
SELECT DatabaseName, TableName, ColumnName
FROM DBC.ColumnsV
WHERE ColumnName LIKE ANY ('%JOB%ID%', '%JID%', '%JOB_NO%', '%SEND%ID%', '%DEPLOY%ID%',
                           '%SUBSCRIBER%')
ORDER BY DatabaseName, TableName, ColumnName;


/* ============================================================================
[2] Tables that smell like SFMC / ESP / send-log / opt-out feeds
    (RESP family excluded so ESP does not match RESPONSE)
============================================================================ */
SELECT DatabaseName, TableName, TableKind          -- T = table, V = view
FROM DBC.TablesV
WHERE ( TableName LIKE ANY ('%SFMC%', '%EXACT%', '%EMC%', '%SENDLOG%', '%SEND_LOG%',
                            '%OPTOUT%', '%OPT_OUT%', '%UNSUB%', '%CASL%', '%MKT_CLOUD%')
        OR (TableName LIKE '%ESP%' AND TableName NOT LIKE '%RESP%') )
ORDER BY DatabaseName, TableName;


/* ============================================================================
[3] Everything living where the known SFMC->EDW pipe lands
    (VENDOR_FEEDBACK is in DTZV01 - what else is?)
============================================================================ */
SELECT DatabaseName, TableName, TableKind
FROM DBC.TablesV
WHERE DatabaseName IN ('DTZV01', 'DTZTAU')
ORDER BY DatabaseName, TableName;


/* ============================================================================
[4] Freshness / metadata per candidate - EDIT the LIKE pattern, rerun per family
    Caveats: LastAlterTimeStamp = last DDL change, NOT last data load.
             LastAccessTimeStamp/AccessCount = readers; NULL unless logging is on.
             size_gb ~ 0 = empty shell; views (TableKind V) have no size at all.
============================================================================ */
SELECT t.DatabaseName, t.TableName, t.TableKind,
       t.CreateTimeStamp, t.CreatorName,
       t.LastAlterTimeStamp, t.LastAlterName,
       t.LastAccessTimeStamp, t.AccessCount,
       CAST(s.CurrentPerm AS FLOAT) / 1024/1024/1024 AS size_gb
FROM DBC.TablesV t
LEFT JOIN (SELECT DatabaseName, TableName, SUM(CurrentPerm) AS CurrentPerm
           FROM DBC.TableSizeV GROUP BY 1, 2) s
       ON s.DatabaseName = t.DatabaseName AND s.TableName = t.TableName
WHERE t.TableName LIKE '%UNSUB%'                   -- <- EDIT pattern here
ORDER BY t.LastAlterTimeStamp DESC;


/* ============================================================================
[5] Per-finalist freshness - EDIT table/column names, run the three statements
    Dictionary cannot say when DATA last landed; the table's own clock can.
============================================================================ */
SELECT * FROM DTZV01.VENDOR_FEEDBACK_EVENT SAMPLE 5;                    -- <- EDIT table

SELECT MAX(disposition_dt_tm) AS latest_data, COUNT(*) AS n_rows        -- <- EDIT column
FROM DTZV01.VENDOR_FEEDBACK_EVENT;                                      -- <- EDIT table

SELECT StatsName, LastCollectTimeStamp
FROM DBC.StatsV
WHERE DatabaseName = 'DTZV01' AND TableName = 'VENDOR_FEEDBACK_EVENT'   -- <- EDIT both
ORDER BY LastCollectTimeStamp DESC;
