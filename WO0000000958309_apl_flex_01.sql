-- en la etiqueta <esquema> se coloca el esquema donde ejecutar el script --> (FLEX, FLEX_CUSTOMIZACION, FLEX_SOMOS, OPENSIRIUS, OPENSIRIUSWM)
ALTER SESSION SET CURRENT_SCHEMA = FLEX;  --- ESQUEMA
SET appinfo ON
SET echo OFF
SET serveroutput ON
SET timing OFF
SET verify OFF
SET heading OFF
SET feedback ON

COLUMN file_script new_val file_script;
COLUMN instance_name new_val instance_name;
COLUMN fecha_exec new_val fecha_exec;
COLUMN spool_file new_val spool_file;
COLUMN usuario_exec new_val usuario_exec;
COLUMN usuario_os new_val usuario_os;
COLUMN esquema new_val esquema;

DEFINE OC='WO0000000958309'

SELECT SUBSTR(SYS_CONTEXT('USERENV','MODULE'),INSTR(SYS_CONTEXT('USERENV','MODULE'),'@')+2) file_script,
       SYS_CONTEXT('USERENV', 'DB_NAME') instance_name,
       TO_CHAR(SYSDATE, 'DD-MM-YYYY HH24:MI:SS') fecha_exec,
	   SYS_CONTEXT('USERENV','CURRENT_SCHEMA') esquema,
       USER usuario_exec,
       SYS_CONTEXT('USERENV', 'OS_USER') usuario_os,
       '&OC'||'_'||'apl_'||USER||'_'||SUBSTR(SYS_CONTEXT('USERENV', 'DB_NAME'),1,4)||
       '_'||TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS')  spool_file  FROM DUAL;

--SPOOL &spool_file..log  --> Ya no se va usar el spool para ningún archivo, 
--                            porque la ejecución del proceso va queda en la consola del release
PROMPT
PROMPT =========================================
PROMPT  ****   Información de Ejecución    ****
PROMPT =========================================
PROMPT Archivo ejecutado: &file_script
PROMPT Instancia        : &instance_name
PROMPT Fecha ejecución  : &fecha_exec
PROMPT Usuario DB       : &usuario_exec
PROMPT Usuario O.S      : &usuario_os
PROMPT Esquema          : &esquema
PROMPT WO               : &OC
PROMPT =========================================
PROMPT
PROMPT **** Aplica de objetos ****

SET define OFF

PROMPT INICIA PROCESO ....
-- aqui van los script que se van a ejecutar 
prompt "Aplicando /WO0000000958309_00.sql"  
@./WO0000000958309_00.sql

--SHOW ERRORS--> la instrucción SHOW ERRORS debe colocarse cuando se este comoilando paquete, funcion o procedure. no aplica para oc cactualizacion de datos 

PROMPT **** Termina aplica de objetos ****

SPOOL OFF
SET SERVEROUTPUT OFF