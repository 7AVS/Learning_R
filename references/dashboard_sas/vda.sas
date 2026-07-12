/* SOURCE: dashboard team SAS, transcribed from photos 2026-05-01 */
/* Original file: VCN_VDA_Acquisition.sas (dashboard team) — VDA section extracted */
/*
  FLAGS IN THIS FILE:
  - FLAG 1 (Two history tables): VDA_success01 queries DG6V01.TACTIC_EVNT_IP_AR_HIST;
    VDA_success02 queries DG6V01.TACTIC_EVNT_INFO_HIST — different table names.
    Confirm this is intentional (two distinct history tables for different success definitions).
  - FLAG 2 (SUCCESS1_IND naming): Dashboard team uses SUCCESS1_IND (no underscore between
    SUCCESS and 1) in VDA_success01, vs. SUCCESS2_IND in VDA_success02. Preserved verbatim.
  - FLAG 3 (DDWV01.PT_OF_SALE_TXN): First appearance of this table in the cards codebase.
    Confirm table exists and is accessible under DDWV01 schema.
*/

%include '/users/qjk9wbt/Password.txt';
option obs=max compress=yes reuse=yes mprint symbolgen;
options dlcreatedir;

%LET ME_DT = '2024-06-30';
/*%LET TACTIC = '2024190VDA';*/
/*%LET TACTIC = '2023325VDA','2022332VDA' ;*/
/*%LET TACTIC = '2023325VDA','2022332VDA','2024190VDA','2023143VDA','2023234VDA' ;*/

%let NME = 'VDA';

proc sql;
connect to teradata (mode=teradata user=&user password=&password);
create table VDA_TACTIC as
select * from connection to teradata (

SELECT DISTINCT
        CLNT_NO  /*client number */
        ,tactic_id
        ,tst_grp_cd
        ,rpt_grp_cd
        ,TREATMT_MN
        ,TACTIC_CELL_CD

FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
/*    where tactic_id in (&tactic.)*/
        where substr(tactic_id,8,3) = &nme.
        order by 1
    );
quit;

proc freq data=VDA_TACTIC;
tables
    tactic_id*tst_grp_cd
    tactic_id*TACTIC_CELL_CD
    tactic_id*rpt_grp_cd*TREATMT_MN
    / list missing;
run;


proc sql;
connect to teradata (mode=teradata user=&user password=&password);
create table VDA_success01 as
select * from connection to teradata (
SELECT DISTINCT

    A.CLNT_NO
    ,A.TACTIC_ID
    ,A.TST_GRP_CD
    ,A.rpt_grp_cd
    ,A.TREATMT_MN
    ,1 AS SUCCESS1_IND /* FLAG 2: SUCCESS1_IND — dashboard team convention, no underscore between SUCCESS and 1 */

    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A /* FLAG 1: _IP_AR_HIST here vs. _INFO_HIST in success02 — confirm intentional */
    INNER JOIN DDWV01.VISA_DR_CRD_DLY  AS B
    ON A.CLNT_NO = B.CLNT_NO

    AND substr(a.tactic_id,8,3) = &nme.
/*  AND A.TREATMT_STRT_DT >= DATE '2023-01-01'*/
    AND B.STS_CD IN ('06', '08')
    AND B.ACTV_DT BETWEEN A.TREATMT_STRT_DT AND A.TREATMT_END_DT
    AND B.SNAP_DT =  &me_dt.
ORDER BY 1,2
);
quit;

proc freq data=VDA_success01;
tables
    tactic_id
    tactic_id*tst_grp_cd
    / list missing;
run;


proc sql;
connect to teradata (mode=teradata user=&user password=&password);
create table VDA_success02 as
select * from connection to teradata (

SELECT DISTINCT

    tactic.clnt_no
    ,tactic.tactic_id
    ,tactic.tst_grp_cd
    ,tactic.rpt_grp_cd
    ,tactic.TREATMT_MN
    ,tactic.TREATMT_STRT_DT
    ,1 AS SUCCESS2_IND

    FROM    DDWV01.PT_OF_SALE_TXN AS C, /* FLAG 3: DDWV01.PT_OF_SALE_TXN — confirm exists */
            DG6V01.TACTIC_EVNT_INFO_HIST  as TACTIC /* FLAG 1: _INFO_HIST here vs. _IP_AR_HIST in success01 — confirm intentional */

    WHERE   C.TXN_DT BETWEEN tactic.TREATMT_STRT_DT AND tactic.TREATMT_END_DT
        AND C.SRVC_CD in (36)  /*Visa debit */
        AND C.amt1 > 0  /* non zero transaction */
        and c.txn_tp IN ('10','13')  /* purchase */
        AND C.MSG_TP = '0210'
        and substr(tactic.tactic_id,8,3) = &nme.
        and tactic.clnt_no = substr(C.clnt_crd_no,7,9)

ORDER BY 1
);
quit;

proc freq data=VDA_success02;
tables
    tactic_id*tst_grp_cd
    / list missing;
run;


PROC SQL;
CREATE TABLE VDA_SUMM AS

SELECT A.*
    ,COALESCE(b.SUCCESS1_IND,0) as SUCCESS1_IND
    ,COALESCE(c.SUCCESS2_IND,0) as SUCCESS2_IND

FROM WORK.VDA_TACTIC A

    LEFT JOIN WORK.VDA_SUCCESS01 B
    ON A.CLNT_NO = B.CLNT_NO
    AND A.TACTIC_ID = B.TACTIC_ID

    LEFT JOIN WORK.VDA_SUCCESS02 C
    ON A.CLNT_NO = C.CLNT_NO
    AND A.TACTIC_ID = C.TACTIC_ID;

QUIT;

proc freq data=VDA_SUMM;
tables
    tactic_id*tst_grp_cd
    tactic_id*tst_grp_cd*SUCCESS1_IND
    tactic_id*tst_grp_cd*SUCCESS2_IND
    / list missing;
run;
