/* SOURCE: dashboard team SAS, transcribed from photos 2026-05-01 */
/*
** FLAGS requiring verification:
**   1. dl_mr_test.TRIGGER_TACTIC_SEGMENT_PREP — confirm if this is a test schema or production equivalent
**   2. VRB_RESP_END_DT — confirm field name spelling
**   3. File continues past line 67 (cuts off in source photos) — section ends after _p_success_&mnemonic. volatile table is created
*/

/*** Create volatile table unique mnemonic and clnt_no ***/
%Put Creating volatile table unq_mne_clnt at %sysfunc(time(),time10.3);
execute(create volatile table unq_mne_clnt as
    (select MNEMONIC,
            TACTIC_ID,
            TACTIC_ID_SUB1,
            TACTIC_ID_SUB2,
            CLNT_NO,
            AR_ID,
            TREATMT_STRT_DT,
            VRB_RESP_END_DT  /* FLAG: confirm field name */
    from dl_mr_test.TRIGGER_TACTIC_SEGMENT_PREP  /* FLAG: test schema? */
        where mnemonic = UPPER(%str(%')&mnemonic.%str(%'))
        qualify row_number() over (partition by MNEMONIC,CLNT_NO,TACTIC_ID
        order by TREATMT_STRT_DT) = 1
    )
    With Data
    Primary Index(CLNT_NO)
    On Commit preserve Rows) by teradata;

%if &SQLXRC. ne 0 or &syserr ne 0 %then
    %do;
        %let err_msg=Error: SQL Error creating volatile table unq_mne_clnt !;
        %let sqlerr=Y;
    %end;

%put collecting stats on unq_mne_clnt at %sysfunc(time(),time10.3);
execute(collect stats on unq_mne_clnt index(CLNT_NO)) by teradata;


/**** Primary Success: Virtual Debit Card Activation ****/

execute(CREATE VOLATILE TABLE EXTRACT_DATE AS
( SELECT MAX(SNAP_DT) AS EXTRACT_DATE
FROM DDWV01.VISA_DR_CRD_DLY
    WHERE SNAP_DT > CURRENT_DATE -5)
    With Data
    Primary Index(EXTRACT_DATE)
    On Commit preserve Rows) by teradata;

execute(CREATE VOLATILE TABLE P_SUCCESS_TEMP AS
(select unq_mne_clnt.MNEMONIC,
        unq_mne_clnt.TACTIC_ID,
        TACTIC_ID_SUB1,
        null as TACTIC_ID_SUB2,
        unq_mne_clnt.CLNT_NO,
        unq_mne_clnt.AR_ID,
        1 as SUCCESS_IND_1,
        1 as SUCCESS_VAR_1,
        MIN(ACTV_DT) AS SUCCESS_DT_1

from DDWV01.VISA_DR_CRD_DLY  AS VISA_DR_CRD_DLY
JOIN unq_mne_clnt
ON VISA_DR_CRD_DLY.AR_ID=unq_mne_clnt.AR_ID
where SNAP_DT >= (SELECT EXTRACT_DATE FROM EXTRACT_DATE)
and ACTV_DT BETWEEN unq_mne_clnt.TREATMT_STRT_DT AND unq_mne_clnt.TREATMT_STRT_DT+90
and STS_CD in ('06')
and CRD_CNTRL_CD in ('0')
and VISA_DR_CRD_BRND_CD in ('01')
GROUP BY 1,2,3,4,5,6,7,8
)With Data
Primary Index(CLNT_NO)
On Commit preserve Rows) by teradata;


%Put Creating volatile _p_success_&mnemonic. at %sysfunc(time(),time10.3);
execute(create volatile table _p_success_&mnemonic. as (
    SELECT MNEMONIC,
            TACTIC_ID,
            TACTIC_ID_SUB1,
            TACTIC_ID_SUB2,
            CLNT_NO,
            SUCCESS_IND_1,
            SUCCESS_VAR_1,
            SUCCESS_DT_1
    FROM P_SUCCESS_TEMP

    ) With Data
    Primary Index(CLNT_NO)
    On Commit preserve Rows) by teradata;
%if &SQLXRC. ne 0 or &syserr ne 0 %then
    %do;
        %let err_msg=Error: SQL Error creating volatile table _p_success_&mnemonic. !;
        %let sqlerr=Y;
    %end;

%put collecting stats on _p_success_&mnemonic. at %sysfunc(time(),time10.3);
execute(collect stats on _p_success_&mnemonic. index(CLNT_NO)) by teradata;
