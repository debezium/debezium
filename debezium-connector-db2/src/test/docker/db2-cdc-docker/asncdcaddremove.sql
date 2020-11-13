
-- 
-- Define ASNCDC.REMOVETABLE() and ASNCDC.ADDTABLE()
-- ASNCDC.ADDTABLE() puts a table in CDC mode, making the ASNCapture server collect changes for the table
-- ASNCDC.REMOVETABLE() makes the ASNCapture server stop collecting changes for that table
--

--#SET TERMINATOR @
CREATE OR REPLACE PROCEDURE ASNCDC.REMOVETABLE(
in  tableschema VARCHAR(128),
in  tablename VARCHAR(128)
)
LANGUAGE SQL
P1:
BEGIN

DECLARE stmtSQL VARCHAR(2048);

DECLARE SQLCODE INT; 
DECLARE SQLSTATE CHAR(5);  
DECLARE RC_SQLCODE INT DEFAULT 0; 
DECLARE RC_SQLSTATE CHAR(5) DEFAULT '00000';

DECLARE CONTINUE HANDLER FOR SQLEXCEPTION, SQLWARNING, NOT FOUND  VALUES (SQLCODE, SQLSTATE) INTO RC_SQLCODE, RC_SQLSTATE; 

-- delete ASN.IBMSNAP_PRUNCTL entries / source
SET stmtSQL = 'DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_OWNER=''' || tableschema || ''' AND SOURCE_TABLE=''' || tablename ||  '''';
        EXECUTE IMMEDIATE stmtSQL;

-- delete ASN.IBMSNAP_Register entries / source
SET stmtSQL = 'DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER=''' || tableschema || ''' AND SOURCE_TABLE=''' || tablename ||  '''';
        EXECUTE IMMEDIATE stmtSQL;

-- drop CD Table / source
SET stmtSQL = 'DROP TABLE ASNCDC."CDC_' ||
                tableschema || '_' || tablename || '"';
        EXECUTE IMMEDIATE stmtSQL;

--  delete ASN.IBMSNAP_SUBS_COLS entries /target
SET stmtSQL = 'DELETE FROM ASNCDC.IBMSNAP_SUBS_COLS WHERE TARGET_OWNER=''' || tableschema || ''' AND TARGET_TABLE=''' || tablename ||  '''';
        EXECUTE IMMEDIATE stmtSQL;

-- delete ASN.IBMSNAP_SUSBS_MEMBER entries /target
SET stmtSQL = 'DELETE FROM ASNCDC.IBMSNAP_SUBS_MEMBR WHERE TARGET_OWNER=''' || tableschema || ''' AND TARGET_TABLE=''' || tablename ||  '''';
        EXECUTE IMMEDIATE stmtSQL;

-- delete ASN.IBMQREP_COLVERSION
SET stmtSQL = 'DELETE FROM ASNCDC.IBMQREP_COLVERSION col WHERE EXISTS (SELECT * FROM ASNCDC.IBMQREP_TABVERSION tab WHERE SOURCE_OWNER=''' || tableschema || ''' AND SOURCE_NAME=''' || tablename ||  '''AND col.TABLEID1 = tab.TABLEID1 AND col.TABLEID2 = tab.TABLEID2';
        EXECUTE IMMEDIATE stmtSQL;

-- delete ASN.IBMQREP_TABVERSION
SET stmtSQL = 'DELETE FROM ASNCDC.IBMQREP_TABVERSION WHERE SOURCE_OWNER=''' || tableschema || ''' AND SOURCE_NAME=''' || tablename ||  '''';
        EXECUTE IMMEDIATE stmtSQL;

SET  stmtSQL = 'ALTER TABLE "' || tableschema || '"."' || tablename ||  '" DATA CAPTURE NONE';
EXECUTE IMMEDIATE stmtSQL; 

END P1@
--#SET TERMINATOR ;

--#SET TERMINATOR @
CREATE OR REPLACE PROCEDURE ASNCDC.ADDTABLE(
in  tableschema VARCHAR(128),
in  tablename VARCHAR(128)
)
LANGUAGE SQL
P1:
BEGIN

DECLARE SQLSTATE CHAR(5);

DECLARE stmtSQL VARCHAR(2048);

SET  stmtSQL = 'ALTER TABLE "' || tableschema || '"."' || tablename || '" DATA CAPTURE CHANGES';
EXECUTE IMMEDIATE stmtSQL; 

SET  stmtSQL = 'CREATE TABLE ASNCDC."CDC_' ||
                tableschema || '_' || tablename || '"' || 
                ' AS ( SELECT ' || 
                '   CAST('''' AS VARCHAR ( 16 )  FOR BIT DATA) AS  IBMSNAP_COMMITSEQ, ' || 
                '   CAST('''' AS VARCHAR ( 16 )  FOR BIT DATA) AS  IBMSNAP_INTENTSEQ, ' ||
                '   CAST ('''' AS CHAR(1)) ' || 
                ' AS IBMSNAP_OPERATION,   t.* FROM "' || tableschema || '"."' || tablename || '" as t )  WITH NO  DATA ORGANIZE BY ROW ';
EXECUTE IMMEDIATE stmtSQL; 

SET stmtSQL = 'ALTER TABLE ASNCDC."CDC_' ||
                tableschema || '_' || tablename || '"' ||
                ' ALTER COLUMN IBMSNAP_COMMITSEQ SET NOT NULL';
EXECUTE IMMEDIATE stmtSQL; 

SET stmtSQL = 'ALTER TABLE ASNCDC."CDC_' ||
                tableschema || '_' || tablename || '"' || 
              ' ALTER COLUMN IBMSNAP_INTENTSEQ SET NOT NULL';
EXECUTE IMMEDIATE stmtSQL; 

SET stmtSQL = 'ALTER TABLE ASNCDC."CDC_' ||
                tableschema || '_' || tablename || '"' ||
              ' ALTER COLUMN IBMSNAP_OPERATION SET NOT NULL';
EXECUTE IMMEDIATE stmtSQL;

SET stmtSQL = 'CREATE  UNIQUE  INDEX ASNCDC."IXCDC_' ||
                  tableschema || '_' || tablename || '"' ||
                ' ON ASNCDC."CDC_' ||
                  tableschema || '_' || tablename || '"' ||
                ' ( IBMSNAP_COMMITSEQ ASC, IBMSNAP_INTENTSEQ ASC ) PCTFREE 0 MINPCTUSED 0';
EXECUTE IMMEDIATE stmtSQL;

SET stmtSQL = 'ALTER TABLE ASNCDC."CDC_' ||
                tableschema || '_' || tablename || '"' || 
              '  VOLATILE CARDINALITY';
EXECUTE IMMEDIATE stmtSQL;

SET stmtSQL =   'INSERT INTO ASNCDC.IBMSNAP_REGISTER (SOURCE_OWNER, SOURCE_TABLE, ' || 
                'SOURCE_VIEW_QUAL, GLOBAL_RECORD, SOURCE_STRUCTURE, SOURCE_CONDENSED, ' || 
                'SOURCE_COMPLETE, CD_OWNER, CD_TABLE, PHYS_CHANGE_OWNER,  ' || 
                'PHYS_CHANGE_TABLE, CD_OLD_SYNCHPOINT, CD_NEW_SYNCHPOINT,  ' || 
                'DISABLE_REFRESH, CCD_OWNER, CCD_TABLE, CCD_OLD_SYNCHPOINT,  ' || 
                'SYNCHPOINT, SYNCHTIME, CCD_CONDENSED, CCD_COMPLETE, ARCH_LEVEL,  ' || 
                'DESCRIPTION, BEFORE_IMG_PREFIX, CONFLICT_LEVEL,  ' || 
                'CHG_UPD_TO_DEL_INS, CHGONLY, RECAPTURE, OPTION_FLAGS, ' || 
                'STOP_ON_ERROR, STATE, STATE_INFO ) VALUES( ' || 
                '''' || tableschema || ''', ' || 
                '''' || tablename || ''', ' ||  
                '0, ' || 
                '''N'', ' || 
                '1, ' || 
                '''Y'', ' || 
                '''Y'', ' || 
                '''ASNCDC'', ' || 
                '''CDC_' ||  tableschema ||  '_' || tablename || ''', ' || 
                '''ASNCDC'', ' || 
                '''CDC_' ||  tableschema ||  '_' || tablename || ''', ' || 
                'null, ' || 
                'null, ' || 
                '0, ' || 
                'null, ' || 
                'null, ' || 
                'null, ' || 
                'null, ' || 
                'null, ' || 
                'null, ' || 
                'null, ' || 
                '''0801'', ' || 
                'null, ' || 
                'null, ' || 
                '''0'', ' || 
                '''Y'', ' || 
                '''N'', ' || 
                '''Y'', ' || 
                '''NNNN'', ' || 
                '''Y'', ' || 
                '''A'',' || 
                'null ) ';
EXECUTE IMMEDIATE stmtSQL;

SET stmtSQL =   'INSERT INTO ASNCDC.IBMSNAP_PRUNCNTL ( ' || 
                'TARGET_SERVER,  ' || 
                'TARGET_OWNER,  ' || 
                'TARGET_TABLE,  ' || 
                'SYNCHTIME,  ' || 
                'SYNCHPOINT,  ' || 
                'SOURCE_OWNER,  ' || 
                'SOURCE_TABLE,  ' || 
                'SOURCE_VIEW_QUAL,  ' || 
                'APPLY_QUAL,  ' || 
                'SET_NAME,  ' || 
                'CNTL_SERVER ,  ' || 
                'TARGET_STRUCTURE ,  ' || 
                'CNTL_ALIAS ,  ' || 
                'PHYS_CHANGE_OWNER ,  ' || 
                'PHYS_CHANGE_TABLE ,  ' || 
                'MAP_ID  ' || 
                ') VALUES ( ' || 
                '''KAFKA'', ' || 
                '''' || tableschema || ''', ' || 
                '''' || tablename || ''', ' ||
                'NULL, ' || 
                'NULL, ' || 
                '''' || tableschema || ''', ' || 
                '''' || tablename || ''', ' ||
                '0, ' || 
                '''KAFKAQUAL'', ' || 
                '''SET001'', ' || 
                ' (Select CURRENT SERVER from sysibm.sysdummy1 ), ' || 
                '8, ' || 
                ' (Select CURRENT SERVER from sysibm.sysdummy1 ), ' || 
                '''ASNCDC'', ' || 
                '''"CDC_' ||  tableschema ||  '_' || tablename || '"'', ' ||
                ' ( SELECT CASE WHEN max(CAST(MAP_ID AS INT)) IS NULL THEN CAST(1 AS VARCHAR(10)) ELSE CAST(CAST(max(MAP_ID) AS INT) + 1 AS VARCHAR(10))  END AS MYINT from  ASNCDC.IBMSNAP_PRUNCNTL ) ' || 
                '    )';
EXECUTE IMMEDIATE stmtSQL;

END P1@
--#SET TERMINATOR ;
