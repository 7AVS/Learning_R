/* SOURCE: dashboard team SAS, transcribed from photos 2026-05-01 */
/* Original file: VCN_VDA_Acquisition.sas (dashboard team) — VCN section extracted */
/*
  FLAGS IN THIS FILE:
  - FLAG 1 (TREATMT_MN field): Field name is TREATMT_MN — confirm whether dashboard team
    intended TREATMT_DT_MN or another date-qualified variant. Used in both TACTIC and
    VCN_success01 SELECT lists but no date filter is applied on it.
  - FLAG 2 (ME_DT date format): %LET ME_DT passed as a quoted string ('2024-06-30').
    Used bare (&me_dt.) in AND B.SNAP_DT = &me_dt. — SAS/Teradata pass-through will
    receive the quotes as part of the resolved value. Confirm this is intentional and
    matches how Teradata expects the date literal.
*/

%include '/users/qjk9wbt/Password.txt';
option obs=max compress=yes reuse=yes mprint symbolgen;
options dlcreatedir;

%LET ME_DT = '2024-06-30';
%LET TACTIC = '2024001VCN';

proc sql;
connect to teradata (mode=teradata user=&user password=&password);
create table TACTIC as
select * from connection to teradata (

SELECT DISTINCT
        CLNT_NO  /*client number */
        ,tactic_id
        ,tst_grp_cd
        ,rpt_grp_cd
        ,TREATMT_MN /* FLAG 1: confirm vs. TREATMT_DT_MN or similar */

FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    where tactic_id = &tactic.
    order by 1
);
quit;


proc sql;
connect to teradata (mode=teradata user=&user password=&password);
create table VCN_success01 as
select * from connection to teradata (
SELECT DISTINCT

    A.CLNT_NO
    ,A.TACTIC_ID
    ,A.TST_GRP_CD
    ,A.rpt_grp_cd
    ,A.TREATMT_MN /* FLAG 1: confirm vs. TREATMT_DT_MN or similar */
    ,1 AS SUCCESS_IND

    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A
    INNER JOIN DDWV01.VISA_DR_CRD_DLY  AS B
    ON A.CLNT_NO = B.CLNT_NO

    AND A.tactic_id = &tactic.
/* AND A.TREATMT_STRT_DT >= DATE '2023-01-01'*/
    AND B.STS_CD IN ('06', '08')
    AND B.ACTV_DT BETWEEN A.TREATMT_STRT_DT AND A.TREATMT_END_DT
    AND B.SNAP_DT =  &me_dt. /* FLAG 2: ME_DT passed as quoted string — confirm Teradata date literal handling */
ORDER BY 1,2
);
quit;

proc freq data=VCN_success01;
tables
tst_grp_cd
/ list missing;
run;
