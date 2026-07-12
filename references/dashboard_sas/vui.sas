/* SOURCE: dashboard team SAS, transcribed from photos 2026-05-01 */
/*
** FLAGS requiring verification:
**   1. Line 26 in source: TRIM(TACTIC.TACTIC_DECISION_VAR_INFO.INDEX(TACTIC.TACTIC_DECISN_VAR_INFO,'SEG: '),4,11))
**      — function structure unusual; could be SUBSTR(INDEX(...))
**   2. HRP_GRP_CD vs RPT_GRP_CD — input column name looks like typo for RPT_GRP_CD
**   3. Line 113 / 115: AND A.TACTIC_ID AND B.TACTIC_ID — almost certainly should be AND A.TACTIC_ID = B.TACTIC_ID (missing =)
**   4. Multiple lines have leading-comma style preserved as in source
*/

***** 20241018: THIS CODE STARTS OFF FROM THE WORKING CSR_success_VUI CODE *****;
***** 20241018: REVISIONS AND ENHANCEMENTS MADE *****;

%include '~/password.sas';
%macro connectsql;
    connect to teradata (mode=teradata user=&uid password=&pwd);
%mend connectsql;
option obs=max compress=yes reuse=yes mprint symbolgen;
libname VUI '~/sas/cla/transit05234/p4/mwpx/Payments/VUI';
%let BEHAVE_STRT = '2024-08-19';  /* this is the campaign start date */
%let BEHAVE_END  = '2024-10-17';  /* MOST RECENT DATE AVAILABLE */

***** START: PULL THE CLIENTS IN THE CAMPAIGN *****;
proc sql;
%connectsql;
create table TACTIC as
    select * from connection to teradata (
        select
            TACTIC.clnt_no,
            TACTIC.TACTIC_ID,
            TACTIC.TST_GRP_CD AS TST_GRP_CD,
            TACTIC.HRP_GRP_CD AS RPT_GRP_CD,  /* FLAG: HRP_GRP_CD spelling — likely typo for RPT_GRP_CD */
            TACTIC.TREATMT_STRT_DT AS TREATMT_STRT_DT,
            TACTIC.TREATMT_END_DT AS TREATMT_END_DT,
            TRIM(TACTIC.TACTIC_DECISION_VAR_INFO.INDEX(TACTIC.TACTIC_DECISN_VAR_INFO,'SEG: '),4,11)) AS SEGMENT,  /* FLAG: TRIM/INDEX function structure unusual — verify with source */
            max(CASE WHEN evt_sts_reas_reltn_hist.evnt_sts_cd > 0 THEN 1 ELSE 0 END) as ACTION,
            min(evt_sts_reas_reltn_hist.evnt_STRT_DT) as EVNT_STRT_DT0,
            max(evt_sts_reas_reltn_hist.evnt_sts_cd) as evnt_sts_cd

        from DG6V01.TACTIC_EVNT_IP_AR_HIST as TACTIC

            LEFT JOIN dg6v01.evnt_sts_reas_reltn_hist as evnt_sts_reas_reltn_hist
            ON evnt_sts_reas_reltn_hist.tactic_id = TACTIC.tactic_id
            ON evnt_sts_reas_reltn_hist.clnt_no = TACTIC.clnt_no  /* FLAG: source shows ON, should likely be AND */
/*  and     evnt_sts_reas_reltn_hist.evnt_id_typ_cd = TACTIC.tactic_evnt_id_typ_cd*/
/*  and     evnt_sts_reas_reltn_hist.evnt_cd = TACTIC.tactic_evnt_src_cd*/
            and evnt_sts_reas_reltn_hist.strtgy_src_cd = TACTIC.strtgy_src_cd
            and evnt_sts_reas_reltn_hist.chnl_sys_id not in ('5') /* 'S' is internet*/
            and evnt_sts_reas_reltn_hist.evnt_STRT_DT BETWEEN date &BEHAVE_STRT. AND DATE &BEHAVE_END.
            and evnt_sts_reas_reltn_hist.evnt_sts_cd >100  /*'actioned only'*/

        WHERE substr(TACTIC.tactic_id,8,3) IN('VUI')
        AND TACTIC.TREATMT_STRT_DT BETWEEN DATE &BEHAVE_STRT. AND DATE &BEHAVE_END.

        group by 1,2,3,4,5,6,7,8
        order by 1,2
    );
quit;

***** END: PULL THE CLIENTS IN THE CAMPAIGN *****;

***** START: PULL PRIMARY SUCCESS - VUI USAGE WITHIN CAMPAIGN PERIOD *****;
proc sql;
%connectsql;
create table VUI_USAGE as
    select * from connection to teradata (

SELECT DISTINCT

    tactic.clnt_no,
    ,tactic.tactic_id,
    ,tactic.TREATMT_STRT_DT,
    ,1 AS SUCCESS_IND

    FROM   DDWV01.PT_OF_SALE_TXN AS C,
            DG6V01.TACTIC_EVNT_INFO_HIST  as TACTIC

    WHERE   C.TXN_DT BETWEEN tactic.TREATMT_STRT_DT AND (tactic.TREATMT_STRT_DT+90)  /*purchase within 90 days */
        AND C.SRVC_CD IN (36)  /*Visa debit*/
        /* AND C.AMT1 > 0  /* non zero transaction */  */
        AND C.txn_tp IN (10,13)  /* purchase */
        and substr(TACTIC.tactic_id,8,3) IN('VUI')
        AND TACTIC.TREATMT_STRT_DT BETWEEN DATE &BEHAVE_STRT. AND DATE &BEHAVE_END.
        and tactic.clnt_no = substr(C.clnt_crd_no,7,9)

ORDER BY 1
);
quit;

PROC SQL;
create table VUI_USAGE_SUCCESS AS (
SELECT
    CLNT_NO,
    TACTIC_ID,
    SUCCESS_IND,
    SUM(SUCCESS_IND) AS SUCCESS_CNT
FROM VUI_USAGE
GROUP BY 1,2,3);
QUIT;

***** END: PULL PRIMARY SUCCESS - VUI USAGE WITHIN CAMPAIGN PERIOD *****;

***** START: JOIN ALL THE TABLES TOGETHER *****;
PROC SQL;
CREATE TABLE VUI_CAMPAIGN_DATA AS (
SELECT
    A.clnt_no,
    A.TACTIC_ID,
    A.TST_GRP_CD AS TST_GRP_CD,
    A.RPT_GRP_CD AS RPT_GRP_CD,
    A.TREATMT_STRT_DT AS TREATMT_STRT_DT,
    A.TREATMT_END_DT AS TREATMT_END_DT,
    A.CHANNEL,
    A.SEGMENT,
    A.ACTION,
    A.EVNT_STRT_DT0,
    A.evnt_sts_cd,
    COALESCE(B.SUCCESS_IND,0) AS SUCCESS_IND,
    COALESCE(B.SUCCESS_CNT,0) AS SUCCESS_CNT
FROM TACTIC AS A

    LEFT JOIN VUI_USAGE_SUCCESS AS B
    ON A.CLNT_NO = B.CLNT_NO
    AND A.TACTIC_ID AND B.TACTIC_ID  /* FLAG: missing `=` — should be A.TACTIC_ID = B.TACTIC_ID */
);
QUIT;

proc freq data=VUI_CAMPAIGN_DATA;
tables
    tst_grp_cd
    / list missing;
run;

***** END: JOIN ALL THE TABLES TOGETHER *****;

***** START: ROLL UP THE DATA FOR ANALYTICS PURPOSE *****;
PROC SQL;
CREATE TABLE VUI_CAMPAIGN_DATA_RU AS (
SELECT
    TACTIC_ID,
    TST_GRP_CD AS TST_GRP_CD,
    RPT_GRP_CD AS RPT_GRP_CD,
    TREATMT_STRT_DT AS TREATMT_STRT_DT,
    TREATMT_END_DT AS TREATMT_END_DT,
    CHANNEL,
    SEGMENT,
    ACTION,
    SUCCESS_IND,
    SUCCESS_CNT,
    COUNT(CLNT_NO) AS CLNT_CNT
FROM VUI_CAMPAIGN_DATA
GROUP BY 1,2,3,4,5,6,7,8,9,10
);
QUIT;

***** END: ROLL UP THE DATA FOR ANALYTICS PURPOSE *****;
