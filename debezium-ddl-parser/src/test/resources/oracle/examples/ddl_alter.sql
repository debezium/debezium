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
-- Alter table add unique index
alter table dbz1211 add constraint name unique (id,data) using index tablespace ts;
alter table dbz1211_child add constraint name unique (id) using index tablespace ts;
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
ALTER TABLE SYSTEM.LOGMNR_KOPM$ MODIFY PARTITION P137 REBUILD UNUSABLE LOCAL INDEXES;