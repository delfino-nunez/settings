define _editor=notepad

set termout off
set null <NULL>
set numwidth 10
set lines 300
set pages 300
set timing on
set trimspool on

col db_name new_value dbname
select sys_context('USERENV','CDB_NAME') ||':'||
sys_context('USERENV','INSTANCE_NAME') ||':'||
sys_context('USERENV','CON_NAME') db_name FROM DUAL;

alter session set NLS_DATE_FORMAT = 'DD-MON-YYYY HH24:MI:SS';

set termout on

REM SET sqlprompt '&_user:&_connect_identifier > '
SET sqlprompt '&_user:&dbname. > '

