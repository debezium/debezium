-- Alter table partitioning
ALTER TABLE sales add partition p6 values less than (1996);
ALTER TABLE products add partition;
ALTER TABLE products add partition p5 tablespace u5;
ALTER TABLE customers add partition central_india values ('BHOPAL','NAGPUR');
ALTER TABLE products coalesce partition;
ALTER TABLE sales drop partition p5;
ALTER TABLE sales DROP PARTITION p5 UPDATE GLOBAL INDEXES;
ALTER TABLE sales merge partition p2 and p3 into partition p23;
ALTER TABLE customers MODIFY PARTITION south_india ADD VALUES ('KOCHI', 'MANGALORE');
ALTER TABLE customers MODIFY PARTITION south_india DROP VALUES ('KOCHI','MANGALORE');
ALTER TABLE sales split partition p5 into (Partition p6 values less than (1996), Partition p7 values less than (MAXVALUE));
ALTER TABLE sales truncate partition p5;
ALTER TABLE "SYSTEM". LOGMNR_ATTRCOL$ exchange partition P1023 with table LOGMNRT_1023_ATTRCOL$ including  indexes without validation;
ALTER TABLE S.PRODUCTS DROP PARTITION FOR 101;
ALTER TABLE S.PRODUCTS DROP PARTITION p0 UPDATE GLOBAL INDEXES;
ALTER TABLE S.PRODUCTS DROP PARTITION p0 INVALIDATE GLOBAL INDEXES;
ALTER TABLE S.PRODUCTS DROP PARTITION p0 UPDATE INDEXES;
ALTER TABLE S.PRODUCTS DROP PARTITION p0 UPDATE GLOBAL INDEXES NOPARALLEL;
ALTER TABLE S.PRODUCTS DROP PARTITION p0 INVALIDATE GLOBAL INDEXES NOPARALLEL;
ALTER TABLE S.PRODUCTS DROP PARTITION p0 UPDATE INDEXES NOPARALLEL;
ALTER TABLE TEST001 MODIFY NAME DEFAULT NULL;
-- Alter table add unique index
alter table dbz1211 add constraint name unique (id,data) using index tablespace ts;
alter table dbz1211_child add constraint name unique (id) using index tablespace ts;
ALTER TABLE "ORACDC"."DEPARTMENT" ADD CONSTRAINT "DEPARTMENT_PK" PRIMARY KEY ("REQUEST_ID", "EMP_NO") USING INDEX "ORACDC"."DEPARTMENT_PK"  ENABLE NOVALIDATE;
alter table AbsTDDesc add constraint F_AbsTDDesc_AbstractTr_1vl7ewo foreign key (deviceId, whLocId) references AbstractTransportDevice;
alter table AbsTDDesc add constraint F_AbsTDDesc_AbstractTr_1vl7ewo foreign key (deviceId, whLocId) references AbstractTransportDevice on delete set null;
alter table AbsTDDesc add constraint F_AbsTDDesc_AbstractTr_1vl7ewo foreign key (deviceId, whLocId) references AbstractTransportDevice on delete cascade;
-- Alter table add primary key using index
ALTER TABLE "IDENTITYDB"."CHANGE_NUMBERS" ADD CONSTRAINT "IDX_CHANGENUMBERS_PK" PRIMARY KEY ("CHANGE_NO", "EXPIRY_TIME") USING INDEX "IDENTITYDB"."IDX_CHANGENUMBERS_PK"  ENABLE NOVALIDATE;
ALTER TABLE "MYSCHEMA"."MY_PLANT" DROP PRIMARY KEY DROP INDEX;
-- Alter table truncate subpartition
alter table tdo_001 truncate subpartition inbound_full_pwork update indexes;
alter table tcd_abc_int truncate partition (p1);
-- Alter table add/modify/drop columns
ALTER TABLE SCOTT.T_DBZ_TEST1 ADD T_VARCHAR2 VARCHAR2(20);
ALTER TABLE SCOTT.T_DBZ_TEST1 MODIFY T_VARCHAR2 VARCHAR2(20);
ALTER TABLE SCOTT.T_DBZ_TEST1 DROP COLUMN T_VARCHAR2;
ALTER TABLE debezium.test add location sdo_geometry;
ALTER TABLE SYSTEM.LOGMNR_KOPM$ MODIFY PARTITION P137 REBUILD UNUSABLE LOCAL INDEXES;
ALTER TABLE TABLE_DATA_CHANGE_LOG MODIFY PARTITION PART_LOAN_CARDS_DEMAND_DEPOSITION ADD SUBPARTITION SUBPART_LOAN_CARDS_DEMAND_DEPOSITION_20220301 VALUES LESS THAN (30);
ALTER TABLE B.EXCHANGE_LOG DROP PARTITION FOR (TIMESTAMP '2022-01-28 00:00:00') UPDATE GLOBAL INDEXES ;
ALTER TABLE TABLE_A
ADD TABLE_B_ID NUMBER(5)
ADD FOREIGN KEY (TABLE_B_ID) REFERENCES TABLE_B (ID);
alter table mesg_perf_stat TRUNCATE PARTITION SYS_P1221396 DROP STORAGE CASCADE UPDATE INDEXES;
ALTER TABLE PK_CNT_GIORNI_TIPI_INC SPLIT PARTITION PMAX AT (TO_DATE('1410202215','DDMMYYYYHH24'))
     INTO (PARTITION P2022101414, PARTITION PMAX);
-- Virtual column support
ALTER TABLE VIDEO ADD (sql_code_injection_check NUMBER GENERATED ALWAYS AS (sdd_avoid_sql_injection(SKRIPT)) VIRTUAL);
-- Shrink Space
ALTER TABLE "ODS_XMES_QY"."ORDER_ITEM" shrink space CHECK;
ALTER TABLE boff_item_jn SET INTERVAL ( numtodsinterval ( 1, 'DAY' ) );
