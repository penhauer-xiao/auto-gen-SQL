

set trims on;
set wrap off ;
set linesize 4000;
set echo off;
set termout off;
set feedback off;
set pagesize 0;

spool PREX_AAA.csv




select PAM_CKHCTQZX_NUM ||'|'|| PAM_CKHCTQZX_TYPE_NUM||'|'|| PAM_TERM_TYPE_NUM||'|'|| PAM_TNDNCY_NUM||'|'||PAM_CKHCTQZX_CLCTN_INDC||'|'|| PAM_CKHCTQZX_SHRT_DESC ||'|'||PAM_CKHCTQZX_EXPR_INDC ||'|'||PAM_CKHCTQZX_DESC ||'|'||PAM_FRST_CKHCTQZX_RATE_NUM ||'|'||PAM_SCND_CKHCTQZX_RATE_NUM ||'|'||TO_CHAR(PAM_CKHCTQZX_EFCTV_DATE,'YYYY/MM/DD')||'|'||PAM_DRVTV_INDC ||'|'||PAM_CKHCTQZX_STATUS_INDC from PREX_AAA order by PAM_CKHCTQZX_NUM;




spool off;