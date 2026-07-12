/* SOURCE: dashboard team SAS, transcribed from photos 2026-05-01
**
** FLAGS IN THIS FILE:
**   FLAG-01  Line 296 of source: `B.AMTS = ?` — filter value entirely unreadable in photo
**   FLAG-02  Line 298: leading word read as `wnd` — likely `and`; also field name `VISA_OR_CRD_NO` uncertain
**   FLAG-03  Line 384: `substr(TACTIC.TACTIC_ID,8,12)` — second arg almost certainly should be `3` not `12`,
**            but source shows `12`. Preserved verbatim in both occurrences.
**   FLAG-04  `&&EHAVE_STRT` / `&&EHAVE_END_DT` macro variable names — truncated/blurry, likely
**            `&&BEHAVE_STRT` / `&&BEHAVE_END`
**   FLAG-05  B.SRVC_CD = 38 in Success01 vs 36 in Success02 — inconsistency between blocks, confirm
**   FLAG-06  C.TOKEN_WALLET_IND = 'V' in Success01 vs 'Y' in vut.sas — inconsistency, confirm
**   FLAG-07  Two `CREATE TABLE Success02` blocks in source — preserved both; second one overrides first
**   FLAG-08  Catalog inconsistency: Success01 uses DG6V01.CLNT_CRD_POS_LOG vs DDWV05 / DB6MAPS in vut.sas
**   FLAG-09  DI_DECMAN.TOKEN_LIST in Success01 vs DL_DECMAN.TOKEN_LIST in vut.sas — library prefix differs
**   FLAG-10  B.POS_ENTR_MODE_CD_NON_EMV in Success01 — comparison operator and value missing in source
**   FLAG-11  proc freq after TACTIC: `tables` statement has no variables listed — source unclear
*/

%include "~/sas/security/pswd.sas";
/*libname teratlb teradata user=&user. pass=&pswd. database=&utd.. */
/* Function to Connect to Teradata */
%macro sqlconnect;
    connect to teradata (mode=teradata user=&user. password=&pass.);
%mend sqlconnect;


/* TOTAL VAW */
PROC SQL;
%sqlconnect;
    create table TACTIC as
    select * from connection to teradata (

        SELECT DISTINCT
            CLNT_NO  /*client number */
            ,TACTIC_ID
            ,tst_grp_cd
            ,rpt_grp_cd
            ,TREATMT_MN

        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
            where SUBSTR(TACTIC_ID,8,3) = 'VAW'
            order by 1
    );
quit;

proc freq data=TACTIC;
    title 'Distribution of Test Groups in VAW Campaign';
    tables
        / list missing;  /* FLAG-11: no variables listed in tables statement — source unclear */
run;
title;


/* Captures any wallet Tokenization activity */
PROC SQL;
%sqlconnect;
    create table Success01 as
    select * from connection to teradata (

        SELECT DISTINCT

            A.CLNT_NO
            ,A.TACTIC_ID
            ,A.TST_GRP_CD
            ,A.RPT_GRP_CD
            ,A.TREATMT_MN
            ,min(txn_dt) as min_dt
            ,1 AS SUCCESS_IND

        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A

            INNER JOIN DG6V01.CLNT_CRD_POS_LOG AS B  /* FLAG-08: catalog inconsistent across files — DG6V01 vs DDWV05 vs DB6MAPS */
                ON A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO,7,9)

            INNER JOIN DI_DECMAN.TOKEN_LIST AS C  /* FLAG-09: DI_DECMAN vs DL_DECMAN — earlier files use DL */
                on B.TOKEN_REQSTR_ID = C.TOKEN_ID

        WHERE B.TXN_DT BETWEEN A.TREATMT_STRT_DT-30 AND A.TREATMT_END_DT
            and  B.AMTS                            = ?  /* FLAG-01: filter value unreadable in source photo */
            and  SUBSTR(B.CLNT_CRD_NO,1,5)         = '45190'
            and  wnd  SUBSTR(B.VISA_OR_CRD_NO,1,5) = '45190'  /* FLAG-02: leading word reads `wnd` — likely `and`; VISA_OR_CRD_NO field name uncertain */
            and  SUBSTR(B.TOKEN_REQSTR_ID,1,3)     > '0'
            and  B.POS_ENTR_MODE_CD_NON_EMV  /* FLAG-10: comparison operator and value missing in source */
            and  B.SRVC_CD                         = 38  /* FLAG-05: 38 here vs 36 elsewhere — confirm */
            and  C.TOKEN_WALLET_IND                = 'V'  /* FLAG-06: 'V' here vs 'Y' elsewhere — confirm */

        /*  and tokn_vvd_pan is not null  */

            AND  SUBSTR(A.TACTIC_ID,8,3) = 'VAW'

        group by 1,2,3,4,5
        ORDER BY 1,2
    );
quit;

proc freq data=Success01;
    title 'All Wallet Provisioning Activity (Combined Methods)';
    tables
        min_dt
        ,tst_grp_cd
        ,tst_grp_cd*min_dt
    / list missing;
run;
title;

/* SRVC_COND_CD reference values from source comments:
00 - Normal card present transaction
01 - Cardholder not present (CNP)
02 - Unattended acceptance terminal (card present)
03 - Merchant initiatives of transaction or card (card present)
05 - Cardholder present but card number is on file (e.g. Shell easypay key tags)
09 - Mail/Phone Order (MTO - considered CNP for operating certificate)
10 - Merchant suspicious of transaction or card (card present)
14 - Request for account number verification without authorization
51 - e-Commerce (considered CNP operating without certificate)
52 - AVS and authorization request
53 - e-Commerce (considered CNP operating certificate)
71 - card present, mag stripe can't be read (key-entered)
*/


/* Diagnostic: distribution of SRVC_COND_CD x txn_tp */
PROC SQL;
%sqlconnect;
CREATE TABLE Success02 AS  /* FLAG-07: this table is overridden by a second CREATE TABLE Success02 below */
SELECT * FROM CONNECTION TO TERADATA(
    SELECT DISTINCT

        SRVC_COND_CD,
        txn_tp,
        count(distinct clnt_no) as count_clnt

    FROM DG6V01.PT_OF_SALE_TXN AS C,
         DG6V01.TACTIC_EVNT_INFO_HIST  as TACTIC

    WHERE  C.TXN_DT BETWEEN tactic.TREATMT_STRT_DT AND (tactic.TREATMT_STRT_DT +90)  /*purchase within 90 days*/
        AND C.SRVC_CD in (36)  /*Visa debit*/
     /* AND C.amts > 0 */   /* non zero transaction */
        AND c.txn_tp IN (10,11)
        AND substr(TACTIC.TACTIC_ID,8,12) IN ('VAW')  /* FLAG-03: 8,12 in source — likely should be 8,3 */
        and tactic.clnt_no = substr(C.clnt_crd_no,7,9)

    ORDER BY SRVC_COND_CD,txn_tp
    group by SRVC_COND_CD,txn_tp
);
quit;


/* Success02 OVERRIDDEN — same name, different definition (per dashboard source) */  /* FLAG-07 */
PROC SQL;
%sqlconnect;
CREATE TABLE Success02 AS
SELECT * FROM CONNECTION TO TERADATA(
    SELECT DISTINCT

        tactic.clnt_no
        ,tactic.TACTIC_ID
        ,tactic.TREATMT_STRT_DT
        ,1 AS SUCCESS_IND2

    FROM DG6V01.PT_OF_SALE_TXN AS C,
         DG6V01.TACTIC_EVNT_INFO_HIST  as TACTIC

    WHERE  C.TXN_DT BETWEEN tactic.TREATMT_STRT_DT AND (tactic.TREATMT_STRT_DT +90)  /*purchase within 90 days*/
        AND C.SRVC_CD in (36)  /*Visa debit*/
     /* AND C.amts > 0 */   /* non zero transaction */
     /* AND c.txn_tp IN (10,11) */  /* purchase */
        AND substr(TACTIC.TACTIC_ID,8,12) IN ('VAW')  /* FLAG-03: same as above */
        AND TACTIC.TREATMT_STRT_DT BETWEEN DATE &&EHAVE_STRT AND DATE &&EHAVE_END_DT  /* FLAG-04: macro var names truncated — likely &&BEHAVE_STRT / &&BEHAVE_END */
        and tactic.clnt_no = substr(C.clnt_crd_no,7,9)

    ORDER BY 1
);
quit;


/* Final: tactic flags joining both successes */
proc sql;
    create table tactic_flags as

    select a.*
        ,COALESCE(b.SUCCESS_IND,0)  as any_wallet
        ,COALESCE(c.SUCCESS_IND2,0) as Usage

    from work.tactic as a

        left join work.success01 as b
            on a.clnt_no = b.clnt_no

        left join work.success02 as c
            on a.clnt_no = c.clnt_no
    ;
quit;

proc freq data=tactic_flags;
    tables
        tst_grp_cd*Usage
        /*PROV_meth*Usage*/
        any_wallet*Usage
    / list missing;
run;
title;
