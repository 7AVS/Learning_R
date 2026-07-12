/* SOURCE: dashboard team SAS, transcribed from photos 2026-05-01
**
** FLAGS IN THIS FILE:
**   FLAG-01  Line 8 of source: `%options dlcreatable;*/` — option name uncertain, could be `dlcreatedir`
**   FLAG-02  Line 22: `CLT_NO` — could be `CLNT_NO`, blurry in source photo
**   FLAG-03  Line 44: `Successia` table name — could be `Success1a`
**   FLAG-04  Catalog conflict: source images show both `DB6MAPS.CLNT_CRD_POS_LOG` AND `DDWV05.CLNT_CRD_POS_LOG`
**            TWO DIFFERENT readings from different photos. Used DDWV05 since that appears in the cleaner image.
**   FLAG-05  Line 71: `B.POS_ENTR_MODE_CD_NON_EMV = 000` (no quotes) vs `= '000'` — appears both ways across images
**   FLAG-06  Line 116: source shows `6.rpt_grp_cd` — almost certainly typo for `A.rpt_grp_cd`, preserved as flag
**   FLAG-07  Line 134 of Success2a: `SUBSTR(B.CLNT_CRD_NO,3,5) = '45190'` vs the original block's
**            `SUBSTR(B.CLNT_CRD_NO,1,9)` — different position arguments, source unclear
**   FLAG-08  `/*38033*/` comment — unclear, possibly client count, ticket, or version marker
**   FLAG-09  `min_min_dt` in proc freq — alias from min(min_dt), could not confirm actual alias name
**   FLAG-10  SUCCESS1_IND in COALESCE target vs SUCCESSI_IND in Successib — name mismatch in source
*/

/* VUT successes
    1. who did it for the first time in campaign period
    2. who did it during campaign irrespective of first or not
    3. n/a
    Frequency of provisions prior to campaign vs during campaign */

%include '~/sas/security/pswd.sas';
%options dlcreatable;*/  /* FLAG-01: option name uncertain — likely dlcreatedir */
option obs=max compress=yes reuse=yes mprint symbolgen;   */

%let func_strt = '2024-07-20';  /* date this new functionality was launched */
%let camp_strt = '2024-08-19';  /* campaign start date */
%let camp_end  = '2024-09-18';  /* hard coded end date — 30 days from campaign start */

%let TACTIC = '2024232VUT';

proc sql;
    connect to teradata (mode=teradata &password);
    create table TACTIC as
    select * from connection to teradata (

        SELECT DISTINCT
            CLT_NO  /* FLAG-02: could be CLNT_NO — blurry in source photo */
            ,Tactic_Id
            ,tst_grp_cd
            ,rpt_grp_cd
            ,TREATMT_MN

        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        where tactic_id = &tactic.
        order by 1
    );
quit;

proc freq data=TACTIC;
tables
    tst_grp_cd
    / list missing;
run;

/* Primary success: clients who provisioned VVD to mobile wallet for the first time during campaign */

proc sql;
    connect to teradata (mode=teradata &password);
    create table Successia as  /* FLAG-03: name could be Success1a */
    select * from connection to teradata (
        SELECT DISTINCT

            A.CLNT_NO
            ,A.TACTIC_ID
            ,A.TST_GRP_CD
            ,A.rpt_grp_cd
            ,A.TREATMT_MN
            ,a.TREATMT_strt_DT
            ,a.TREATMT_END_DT +30 as new_end_dt
            ,min(txn_dt) as min_dt

        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A

            INNER JOIN DDWV05.CLNT_CRD_POS_LOG AS B  /* FLAG-04: catalog could be DB6MAPS — conflicting source images */
            ON A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO,7,9)

            INNER JOIN DL_DECMAN.TOKEN_LIST  as C
            on b.TOKN_REQSTR_ID = C.TOKEN_ID

        WHERE B.TXN_DT BETWEEN date &func_strt. AND date &camp_end.
            and  B.AMT1                            = 0
            and  SUBSTR(B.CLNT_CRD_NO,1,9)         = '45190'
            and  SUBSTR(B.VISA_DR_CRD_NO,1,5)      = '45199'
            and  SUBSTR(B.TOKN_REQSTR_ID,1,1)      > '0'
            and  B.POS_ENTR_MODE_CD_NON_EMV        = 000  /* FLAG-05: '000' vs 000 inconsistent across source images */
            and  B.SRVC_CD                         = 36
            AND  C.TOKEN_WALLET_IND                = 'Y'

            AND  A.tactic_id = &tactic.

        group by 1,2,3,4,5,6,7
        ORDER BY 1,2
    );
quit;

proc sql;
    create table Successib as
    select *,
        case when min_dt BETWEEN TREATMT_strt_DT AND TREATMT_strt_DT+30
            then 1 else 0 end as SUCCESSI_IND

    from Successia;
quit;

proc freq data=Successib;
    tables
        successi_ind
        min_dt
        /*max_dt*/
        tst_grp_cd
        tst_grp_cd*min_dt
        tst_grp_cd*SUCCESSI_IND
    / list missing;
run;


/* Secondary success: clients who provisioned VVD to mobile wallet at all (irrespective of timing) */

proc sql;
    connect to teradata (mode=teradata &password);
    create table Success2a as
    select * from connection to teradata (
        SELECT DISTINCT

            A.CLNT_NO
            ,A.TACTIC_ID
            ,A.TST_GRP_CD
            ,6.rpt_grp_cd  /* FLAG-06: source reads `6.rpt_grp_cd` — almost certainly typo for A.rpt_grp_cd */
            ,A.TREATMT_MN
            ,a.TREATMT_strt_DT
            ,a.TREATMT_END_DT
            ,b.TOKN_REQSTR_ID
            ,min(b.txn_dt) as min_dt

        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A

            INNER JOIN DDWV05.CLNT_CRD_POS_LOG AS B  /* FLAG-04: same catalog conflict as Successia */
            ON A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO,7,9)

            INNER JOIN DL_DECMAN.TOKEN_LIST  as C
            on b.TOKN_REQSTR_ID = C.TOKEN_ID

        WHERE B.TXN_DT BETWEEN date &func_strt. AND date &camp_end.
            and  B.AMT1                            = 0
            and  SUBSTR(B.CLNT_CRD_NO,3,5)         = '45190'  /* FLAG-07: position args different from Successia — source unclear */
            and  SUBSTR(B.VISA_DR_CRD_NO,1,5)      = '45199'
            and  SUBSTR(B.TOKN_REQSTR_ID,1,1)      > '0'
            and  B.POS_ENTR_MODE_CD_NON_EMV        = '000'  /* FLAG-05: see Successia */
            and  B.SRVC_CD                         = 36
            AND  C.TOKEN_WALLET_IND                = 'Y'

            AND  A.tactic_id = &tactic.

        group by 1,2,3,4,5,6,7,8
        ORDER BY 1,2
    );
quit;

proc freq data=success2a;
    tables TST_GRP_CD / list missing;
run;

proc sql;
    create table Success2d as
    select distinct
        CLNT_NO
        ,TACTIC_ID
        ,TST_GRP_CD
        ,rpt_grp_cd
        ,treatmt_strt_dt
        ,min(min_dt) as min_dt FORMAT DATE9.
        ,1 as success2_ind

    from Success2a

    where min_dt BETWEEN TREATMT_strt_DT AND TREATMT_strt_DT+30
    group by 1,2,3,4,5;
quit;

/*38033*/  /* FLAG-08: unclear — possibly client count, ticket, or version marker */

proc freq data=success2d;
    tables success2_ind
        min_min_dt  /* FLAG-09: alias from min(min_dt) — confirm actual alias name */
        /*max_dt*/
        tst_grp_cd
        tst_grp_cd*min_min_dt
        tst_grp_cd*SUCCESS2_IND
    / list missing;
run;


/* Final: tactic flags joining both successes */

proc sql;
    create table tactic_flags2d as

    select a.*
        ,COALESCE(b.SUCCESS1_IND,0) as SUCCESS1_IND  /* FLAG-10: source uses SUCCESSI_IND in successib — name mismatch with COALESCE target */
        ,COALESCE(c.SUCCESS2_IND,0) as SUCCESS2_IND

    from work.tactic as a

        left join work.successib as b
            on a.clnt_no = b.clnt_no

        left join work.success2d as c
            on a.clnt_no = c.clnt_no
    ;
quit;

proc freq data=tactic_flags2d;
    tables
        /*max_dt*/
        tst_grp_cd
        tst_grp_cd*SUCCESS1_IND
        tst_grp_cd*SUCCESS2_IND
    / list missing;
run;
