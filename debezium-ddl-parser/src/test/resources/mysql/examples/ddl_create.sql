#begin
GET DIAGNOSTICS @p1 = NUMBER, @p2 = ROW_COUNT;
GET DIAGNOSTICS CONDITION 1 @p1 = MYSQL_ERRNO;
GET DIAGNOSTICS CONDITION 1 @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
GET DIAGNOSTICS CONDITION 1 @p3 = RETURNED_SQLSTATE, @p4 = MESSAGE_TEXT;
GET DIAGNOSTICS CONDITION 1 @p5 = SCHEMA_NAME, @p6 = TABLE_NAME;
GET DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO;
GET DIAGNOSTICS @cno = NUMBER;
GET DIAGNOSTICS CONDITION @cno @errno = MYSQL_ERRNO;
GET CURRENT DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT;
GET STACKED DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT;
GET CURRENT DIAGNOSTICS errcount = NUMBER;
-- Create User
CREATE USER 'test_crm_debezium'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9' PASSWORD EXPIRE NEVER COMMENT '-';
CREATE USER 'jim'@'localhost' ATTRIBUTE '{"fname": "James", "lname": "Scott", "phone": "123-456-7890"}';
CREATE USER 'jim' @'localhost' ATTRIBUTE '{"fname": "James", "lname": "Scott", "phone": "123-456-7890"}';
-- Create Table
create table new_t  (like t1);
create table log_table(row varchar(512));
create table log_table(row character(512));
create table ships(name varchar(255), class_id int, id int);
create table ships_guns(guns_id int, ship_id int);
create table guns(id int, power decimal(7,2), callibr decimal(10,3));
create table ship_class(id int, class_name varchar(100), tonange decimal(10,2), max_length decimal(10,2), start_build year, end_build year(4), max_guns_size int);
create table `some table $$`(id int auto_increment key, class varchar(10), data binary) engine=MYISAM;
create table quengine(id int auto_increment key, class varchar(10), data binary) engine='InnoDB';
create table quengine(id int auto_increment key, class varchar(10), data binary) engine="Memory";
create table quengine(id int auto_increment key, class varchar(10), data binary) engine=`CSV`;
create table quengine(id int auto_increment key, class varchar(10), data binary COMMENT 'CSV') engine=MyISAM;
create table quengine(id int auto_increment key, class varchar(10), data binary) engine=Aria;
create table `parent_table`(id int primary key, column1 varchar(30), index parent_table_i1(column1(20)), check(char_length(column1)>10)) engine InnoDB;
create table child_table(id int unsigned auto_increment primary key, id_parent int references parent_table(id) match full on update cascade on delete set null) engine=InnoDB;
create table `another some table $$` like `some table $$`;
create table `actor` (`last_update` timestamp default CURRENT_TIMESTAMP, `birthday` datetime default CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP);
create table boolean_table(c1 bool, c2 boolean default true);
create table default_table(c1 int default 42, c2 int default -42, c3 varchar(256) DEFAULT _utf8mb3'xxx');
create table ts_table(
  ts1 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ts2 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIME,
  ts3 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP,
  ts4 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(),
  ts5 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIME(),
  ts6 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP(),
  ts7 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE NOW(),
  ts8 TIMESTAMP(6) NOT NULL,
  ts9 TIMESTAMP(6) NOT NULL DEFAULT NOW(6) ON UPDATE NOW(6),
  ts10 TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  ts11 TIMESTAMP NOT NULL DEFAULT '2038-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP
);
create table dt_table(
  dt1 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  dt2 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIME,
  dt3 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP,
  dt4 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(),
  dt5 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIME(),
  dt6 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP(),
  dt7 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE NOW(),
  dt10 DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  dt11 DATETIME DEFAULT '2038-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP
);
create table with_check (c1 integer not null,c2 varchar(22),constraint c1 check (c2 in ('a', 'b', 'c')));
CREATE TABLE genvalue1 (id binary(16) NOT NULL, val char(32) GENERATED ALWAYS AS (hex(id)) STORED, PRIMARY KEY (id));
CREATE TABLE genvalue2 (id binary(16) NOT NULL, val char(32) AS (hex(id)) STORED, PRIMARY KEY (id));
CREATE TABLE genvalue3 (id binary(16) NOT NULL, val char(32) GENERATED ALWAYS AS (hex(id)) VIRTUAL, PRIMARY KEY (id));
CREATE TABLE cast_charset (col BINARY(16) GENERATED ALWAYS AS (CAST('xx' as CHAR(16) CHARACTER SET BINARY)) VIRTUAL);
CREATE TABLE cast_charset (col BINARY(16) GENERATED ALWAYS AS (CAST('xx' as CHAR(16) CHARSET BINARY)) VIRTUAL);
CREATE TABLE check_table_kw (id int primary key, upgrade varchar(256), quick varchar(256), fast varchar(256), medium varchar(256), extended varchar(256), changed varchar(256));
CREATE TABLE sercol1 (id SERIAL, val INT);
CREATE TABLE sercol2 (id SERIAL PRIMARY KEY, val INT);
CREATE TABLE sercol3 (id SERIAL NULL, val INT);
CREATE TABLE sercol4 (id SERIAL NOT NULL, val INT);
CREATE TABLE serval1 (id SMALLINT SERIAL DEFAULT VALUE, val INT);
CREATE TABLE serval2 (id SMALLINT SERIAL DEFAULT VALUE PRIMARY KEY, val INT);
CREATE TABLE serval3 (id SMALLINT(3) NULL SERIAL DEFAULT VALUE, val INT);
CREATE TABLE serval4 (id SMALLINT(5) UNSIGNED SERIAL DEFAULT VALUE NOT NULL, val INT);
CREATE TABLE serial (serial INT);
CREATE TABLE float_table (f1 FLOAT, f2 FLOAT(10), f3 FLOAT(7,4));
CREATE TABLE USER (INTERNAL BOOLEAN DEFAULT FALSE);
create table table_with_character_set_eq (id int, data varchar(50)) character set = default;
create table table_with_character_set (id int, data varchar(50)) character set default;
create table table_with_visible_index (id int, data varchar(50), UNIQUE INDEX `data_UNIQUE` (`data` ASC) INVISIBLE VISIBLE);
create table table_with_index (id int, data varchar(50), UNIQUE INDEX `data_UNIQUE` (`data` ASC));
create table table_with_keyword_as_column_name (geometry int, national int);
create table transactional_table(name varchar(255), class_id int, id int) transactional=1;
create table transactional(name varchar(255), class_id int, id int);
create table add_test(col1 varchar(255), col2 int, col3 int);
create table blob_test(id int, col1 blob(45));
create table žluťoučký (kůň int);
CREATE TABLE staff (PRIMARY KEY (staff_num), staff_num INT(5) NOT NULL, first_name VARCHAR(100) NOT NULL, pens_in_drawer INT(2) NOT NULL, CONSTRAINT pens_in_drawer_range CHECK(pens_in_drawer BETWEEN 1 AND 99));
create table column_names_as_aggr_funcs(min varchar(100), max varchar(100), sum varchar(100), count varchar(100));
CREATE TABLE char_table (c1 CHAR VARYING(10), c2 CHARACTER VARYING(10), c3 NCHAR VARYING(10));
create table rack_shelf_bin ( id int unsigned not null auto_increment unique primary key, bin_volume decimal(20, 4) default (bin_len * bin_width * bin_height));
CREATE TABLE `tblSRCHjob_desc` (`description_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, `description` mediumtext NOT NULL, PRIMARY KEY (`description_id`)) ENGINE=TokuDB AUTO_INCREMENT=4095997820 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=TOKUDB_QUICKLZ;
create table invisible_column_test(id int, col1 int INVISIBLE);
create table visible_column_test(id int, col1 int VISIBLE);
create table table_with_buckets(id int(11) auto_increment NOT NULL COMMENT 'ID', buckets int(11) NOT NULL COMMENT '分桶数');
CREATE TABLE foo (c1 decimal(19), c2 decimal(19.5), c3 decimal(0.0), c4 decimal(0.2), c5 decimal(19,2));
create table statement(id int);
CREATE TABLE table_items (id INT, purchased DATE)
    PARTITION BY RANGE( YEAR(purchased) )
        SUBPARTITION BY HASH( TO_DAYS(purchased) )
        SUBPARTITIONS 2 (
        PARTITION p0 VALUES LESS THAN (1990),
        PARTITION p1 VALUES LESS THAN (2000),
        PARTITION p2 VALUES LESS THAN MAXVALUE
    );

CREATE TABLE T1 (
ID INT NOT NULL,
NAME VARCHAR(255),
UNIQUE KEY(ID)
) PARTITION BY KEY() PARTITIONS 2;

CREATE TABLE table_items_with_subpartitions (id INT, purchased DATE)
    PARTITION BY RANGE( YEAR(purchased) )
        SUBPARTITION BY HASH( TO_DAYS(purchased) ) (
        PARTITION p0 VALUES LESS THAN (1990) (
            SUBPARTITION s0,
            SUBPARTITION s1
        ),
        PARTITION p1 VALUES LESS THAN (2000) (
            SUBPARTITION s2,
            SUBPARTITION s3
        ),
        PARTITION p2 VALUES LESS THAN MAXVALUE (
            SUBPARTITION s4,
            SUBPARTITION s5
        )
    );

CREATE TABLE positions_rollover (
    id bigint(20) NOT NULL AUTO_INCREMENT,
    time datetime NOT NULL,
    partition_index int(10) unsigned NOT NULL DEFAULT 0,
    PRIMARY KEY (id,partition_index),
    KEY time (time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
PARTITION BY LIST (partition_index) (
    PARTITION positions_rollover_partition VALUES IN (0) ENGINE = InnoDB,
    PARTITION default_positions_rollover_partition DEFAULT ENGINE = InnoDB
);

CREATE TABLE `tab_with_json_value` (
   `col0` JSON NOT NULL,
   `col1` VARCHAR(36) COLLATE utf8mb4_bin GENERATED ALWAYS AS (
      JSON_VALUE(`col0`, _utf8mb4'$._field1' RETURNING CHAR(36) CHARACTER SET latin1)
   ) STORED NOT NULL,
   `col2` VARCHAR(36) COLLATE utf8mb4_bin GENERATED ALWAYS AS (
      JSON_VALUE(`col0`, _utf8mb4'$._field1' ERROR ON EMPTY)
   ) STORED NOT NULL,
   `col3` VARCHAR(36) COLLATE utf8mb4_bin GENERATED ALWAYS AS (
      JSON_VALUE(`col0`, _utf8mb4'$._field1' DEFAULT 'xx' ON ERROR)
   ) STORED NOT NULL,
   `col4` JSON NOT NULL,
   PRIMARY KEY (`col1`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = COMPRESSED;

CREATE TABLE CustomerTable (
    CustomerID varchar(5),
    CompanyName varchar(40),
    ContactName varchar(30),
    Address varchar(60),
    Phone varchar(24)
 ) ENGINE = CONNECT TABLE_TYPE = ODBC;

CREATE TABLE CustomerTable (
    table_type varchar(5)
);

CREATE TABLE tbl (
    col1 LONGTEXT,
    data JSON,
    INDEX idx1 ((SUBSTRING(col1, 1, 10))),
    INDEX idx2 ((CAST(JSON_EXTRACT(data, _utf8mb4'$') AS UNSIGNED ARRAY))),
    INDEX ((CAST(data->>'$.name' AS CHAR(30))))
);

CREATE TABLE `orders_json_2` (
  `id` int NOT NULL AUTO_INCREMENT,
  `reward` json DEFAULT NULL,
  `additional_info` json DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_order_codes` ((cast(json_extract(`additional_info`,_utf8mb4'$.order_codes') as char(17) array)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE keywords (
    eur VARCHAR(100),
    iso VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
    usa VARCHAR(100),
    jis VARCHAR(100),
    internal INT,
    instant BIT
);

CREATE TABLE T1 (C NATIONAL CHAR);
CREATE TABLE T1 (C GEOMETRY SRID 0);
CREATE TABLE T1 (C POINT SRID 0);
CREATE TABLE T1 (C LINESTRING SRID 0);
CREATE TABLE T1 (C POLYGON SRID 0);
CREATE TABLE T1 (C MULTIPOINT SRID 0);
CREATE TABLE T1 (C MULTILINESTRING SRID 0);
CREATE TABLE T1 (C MULTIPOLYGON SRID 0);
CREATE TABLE T1 (C GEOMETRYCOLLECTION SRID 0);
CREATE TABLE T1 (ID BIGINT, S VARCHAR(100), I INT, CONSTRAINT ABC CHECK (ID < 5) ENFORCED);
CREATE TABLE T1 (ID BIGINT REFERENCES TT (TT_ID) ON DELETE SET DEFAULT);
CREATE TABLE T1 (ID BIGINT REFERENCES TT (TT_ID) ON UPDATE SET DEFAULT);

create table if not exists tbl_signed_unsigned(
  `id` bigint(20) ZEROFILL signed UNSIGNED signed ZEROFILL unsigned ZEROFILL NOT NULL AUTO_INCREMENT COMMENT 'ID',
  c1 int signed unsigned,
  c2 decimal(10, 2) SIGNED UNSIGNED ZEROFILL,
  c3 float SIGNED ZEROFILL,
  c4 double precision(18, 4) UNSIGNED SIGNED ZEROFILL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `daily_intelligences`(
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '',
`partner_code` varchar(32) DEFAULT NULL COMMENT '',
`text` LONGTEXT DEFAULT NULL COMMENT '',
`monitor_time` TIMESTAMP DEFAULT NULL COMMENT '',
`gmt_modify` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '',
`gmt_create` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '',
PRIMARY KEY (`id`)
) ENGINE=innodb DEFAULT CHAR SET=utf8 COMMENT '';

CREATE TABLE IF NOT EXISTS `contract_center`.`ent_folder_letter_relationship` (
`id` BIGINT(19) UNSIGNED NOT NULL COMMENT '唯一标识',
`layer` TINYINT(4) UNSIGNED DEFAULT _UTF8MB4'0' COMMENT '文档所属层级，0-主关联文档， 1-次关联文档',
`deleted` TINYINT(1) NOT NULL DEFAULT _UTF8MB4'0' COMMENT '0-有效记录, 1-删除',
`data_create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '创建时间',
`data_update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP() COMMENT '更新时间',
PRIMARY KEY(`id`)) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4;

CREATE TABLE `auth_realm_clients` (
`pk_realm` int unsigned NOT NULL DEFAULT '0',
`fk_realm` int unsigned DEFAULT NULL,
`client_id` varchar(150) NOT NULL,
`client_secret` blob NOT NULL,
PRIMARY KEY (`pk_realm`),
KEY `auth_realms_auth_realm_clients` (`fk_realm`)
) START TRANSACTION ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table `site_checker_b_sonet_group_favorites` (
USER_ID int(11) not null,
GROUP_ID int(11) not null,
DATE_ADD datetime DEFAULT NULL,
primary key (USER_ID, GROUP_ID)
);

CREATE TABLE `table_default_fn`(`quote_id` varchar(32) NOT NULL,`created_at` bigint(20) NOT NULL);
CREATE TABLE `test_table\\`(id INT(11) NOT NULL, PRIMARY KEY (`id`)) ENGINE = INNODB;
CREATE TABLE `\\test_table`(id INT(11) NOT NULL, PRIMARY KEY (`id`)) ENGINE = INNODB;
CREATE TABLE `\\test\\_table\\`(id INT(11) NOT NULL, PRIMARY KEY (`id`)) ENGINE = INNODB;

CREATE TABLE TableWithVector (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, vec1 VECTOR, vec2 VECTOR);

CREATE TABLE PARTICIPATE_ACTIVITIES (
    ID BIGINT NOT NULL AUTO_INCREMENT,
    USER_ID BIGINT NOT NULL,
    PRIMARY KEY (ID) USING BTREE)
ENGINE=INNODB AUTO_INCREMENT=1979503 DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI SECONDARY_ENGINE=RAPID;

CREATE TABLE `TABLE1` (
`COL1` INT(10) UNSIGNED NOT NULL,
`COL2` VARCHAR(32) NOT NULL,
`COL3` ENUM (`VAR1`,`VAR2`, `VAR3`) NOT NULL,
PRIMARY KEY (`COL1`, `COL2`, `COL3`),
CLUSTERING KEY `CLKEY1` (`COL3`, `COL2`))
ENGINE=TOKUDB DEFAULT CHARSET=CP1251;

CREATE TABLE T1 (NAME VARCHAR(36), SEQUENCE_TABLE BIGINT NOT NULL);

#end
#begin
-- Rename table
-- http://dev.mysql.com/doc/refman/5.6/en/rename-table.html
RENAME TABLE old_table TO tmp_table, new_table TO old_table, tmp_table TO new_table;
RENAME TABLE table_b TO table_a;
RENAME TABLE current_db.tbl_name TO other_db.tbl_name;
rename table debezium_all_types_old to debezium_all_types, test_json_object_old to test_json_object;
#end
#begin
-- Truncate table
truncate table t1;
truncate parent_table;
truncate `#`;
truncate `#!^%$`;
truncate table tbl_without_pk;
#end
#begin
-- Create database
create database somedb;
create schema if not exists myschema;
create schema `select` default character set = utf8;
create database if not exists `current_date` character set cp1251;
create database super default character set utf8 collate = utf8_bin character set utf8 collate utf8_bin;
create database super_cs default charset utf8 collate = utf8_bin character set utf8 collate utf8_bin;
create database db_with_character_set_eq character set = default;
create database db_with_character_set character set default;
create database `ymsun_test1` charset gb18030 collate gb18030_bin;
create database `test` charset binary collate binary;
#end
#begin
-- Create event 1
-- delimiter //
create definer = current_user event if not exists someevent on schedule at current_timestamp + interval 30 minute
on completion preserve do begin insert into test.t1 values (33), (111);select * from test.t1; end; -- //
#end
#begin
-- Create event 2
create definer = 'ivan'@'%' event testevent1 on schedule every 1 hour ends '2016-11-05 23:59:00'
do begin select * from test.t2; end; -- //
#end
#begin
-- Create event 3
create definer = current_user() event testevent2 on schedule at '2016-11-03 23:59:00'
do begin update test.t2 set 1c = 1c + 1; end; -- //
-- delimiter ;
#end
#begin
-- Create index
create index index1 on t1(col1) comment 'test index' comment 'some test' using btree;
create unique index index2 using btree on t2(1c desc, `_` asc);
create index index3 using hash on antlr_tokens(token(30) asc);
create index index4 on antlr_tokens(token(30) asc) lock default;
create index index5 on antlr_tokens(token(30) asc) algorithm default;
create index index6 on antlr_tokens(token(30) asc) algorithm default lock default;
create index index7 on antlr_tokens(token(30) asc) lock default algorithm default;
create index index8 on t1(col1) comment 'test index' using btree;
create index myindex on t1(col1) comment 'test index' comment 'some test' using btree;
CREATE INDEX `idx_custom_field_30c4f4a7c529ccf0825b2fac732bebfd843ed764` ON `deals` ((cast(json_unquote(json_extract(`custom_fields`,_utf8mb4'$."30c4f4a7c529ccf0825b2fac732bebfd843ed764".value')) as double)));
CREATE INDEX `idx_custom_field_d3bb7ad91ba729aaa20df0af037cb7ed8ce3ffc8` ON `deals` ((cast(json_unquote(json_extract(`custom_fields`,_utf8mb4'$."d3bb7ad91ba729aaa20df0af037cb7ed8ce3ffc8".value')) as float)));
#end
#begin
-- Create logfile group
-- http://dev.mysql.com/doc/refman/5.6/en/create-logfile-group.html
CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' INITIAL_SIZE = 10M ENGINE = InnoDB;
-- CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' INITIAL_SIZE = 10M;
CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' INITIAL_SIZE = 10000000 ENGINE = NDB;
#end
#begin
-- Create server
-- http://dev.mysql.com/doc/refman/5.6/en/create-server.html
CREATE SERVER s
FOREIGN DATA WRAPPER mysql
OPTIONS (USER 'Remote', HOST '192.168.1.106', DATABASE 'test');
#end
#begin
-- Create tablespace
create tablespace tblsp1 add datafile 'tblsp_work1' use logfile group lg_1 initial_size = 4G engine MYISAM;
create tablespace tblsp2 add datafile 'tblsp_proj1' use logfile group lg_6 autoextend_size = 4294 max_size = 2294967296 engine NDB;
#end
#begin
-- Create trigger 1
-- delimiter //
create trigger trg_my1 before delete on test.t1 for each row begin insert into log_table values ("delete row from test.t1"); insert into t4 values (old.col1, old.col1 + 5, old.col1 + 7); end; -- //-- delimiter ;
#end
#begin
-- Create trigger 2
create definer = current_user() trigger trg_my2 after insert on test.t2 for each row insert into log_table values (concat("inserted into table test.t2 values: (1c, _) = (", cast(NEW.col1 as char(100)), ", ", convert(new.`_`, char(100)), ")"));
#end
#begin
-- Create trigger 3
-- delimiter //
CREATE TRIGGER mask_private_data BEFORE INSERT ON users FOR EACH ROW BEGIN SET NEW.phone = CONCAT('555', NEW.id); END; -- //-- delimiter ;
#end
#begin
-- Create trigger 4
-- CAST to JSON
CREATE DEFINER=`ctlplane`@`%` TRIGGER `write_key_add` AFTER INSERT ON `sources` FOR EACH ROW
BEGIN
DECLARE i, n INT DEFAULT 0;
SET n = JSON_LENGTH(CAST(CONVERT(NEW.write_keys USING utf8mb4) AS JSON));
SET campaign_id = NEW.write_keys->>'$.campaign_id';
WHILE i < n DO
INSERT INTO source_id_write_key_mapping (source_id, write_key)
VALUES (NEW.id, JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(NEW.write_keys USING utf8mb4) AS JSON), CONCAT('$[', i, ']'))))
ON DUPLICATE KEY UPDATE
       source_id  = NEW.ID,
       write_key  = JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(NEW.write_keys USING utf8mb4) AS JSON), CONCAT('$[', i, ']')));
SET i = i + 1;
END WHILE;
END
#end
#begin
-- Create trigger 5
CREATE TRIGGER `rtl_trigger_before_update`
BEFORE UPDATE
ON all_student_educator FOR EACH ROW
BEGIN
    IF NEW.student_words_read_total is not null AND NEW.student_words_read_total >= 3 AND NEW.badge_3_words_read_flag = 0 THEN
        SET
        NEW.badge_flag = 1,
        NEW.badge_student_total = NEW.badge_student_total + 1,
        NEW.badge_datetime = now();
        INSERT IGNORE INTO user_platform_badge (platform_badge_id, user_id) VALUES (3, NEW.student_id);
    END IF;
END
#end
#begin
-- Create trigger 6
-- delimiter //
create trigger trg_my1 before delete on test.t1 for each row begin insert into log_table values ("delete row from test.t1"); insert into t4 values (old.col1, old.col1 + 5, old.col1 + 7); end; -- //-- delimiter ;
#end
#begin
-- Create trigger 7
-- delimiter //
CREATE TRIGGER IF NOT EXISTS `my_trigger` BEFORE INSERT ON `my_table` FOR EACH ROW BEGIN SET NEW.my_col = CONCAT(NEW.mycol, NEW.id); END; -- //-- delimiter ;
#end
#begin
-- Create trigger 8
CREATE DEFINER='bcadmin'@'%' TRIGGER `host_id_constraint` BEFORE INSERT ON `host` FOR EACH ROW begin
  declare next_id int;
  declare max_id int;
  declare error_msg varchar(100);

  set max_id = power(2, 9)-1;
  set next_id = null;

  select min(st.value) into next_id
  from SEQUENCE_TABLE(max_id+1) st
  left join host h on h.id=st.value
  where st.value > 0
  and h.id is null;

  if isnull(next_id)
  then
    set error_msg = concat('no free ids in range [1, ', cast(max_id as char), ']');
    signal sqlstate '45000' set MESSAGE_TEXT = error_msg;
  end if;

  if next_id > max_id
  then
    set erroR_msg = concat('next id too high to insert: next_id=', cast(next_id as char), ' max_id=', cast(max_id as char));
    signal sqlstate '45000' set MESSAGE_TEXT = error_msg;
  end if;

  set NEW.id = next_id;
END
#end
-- Create view
create or replace view my_view1 as select 1 union select 2 limit 0,5;
create algorithm = merge view my_view2(col1, col2) as select * from t2 with check option;
create or replace definer = 'ivan'@'%' view my_view3 as select count(*) from t3;
create or replace definer = current_user sql security invoker view my_view4(c1, 1c, _, c1_2)
	as select * from  (t1 as tt1, t2 as tt2) inner join t1 on t1.col1 = tt1.col1;
create view v_some_table as (with a as (select * from some_table) select * from a);

#end
#begin
-- Create function
-- delimiter //
CREATE FUNCTION `fivenumbers`() RETURNS varchar(5) CHARSET utf8 COLLATE utf8_unicode_ci DETERMINISTIC
BEGIN
	RETURN '12345';
END; -- //-- delimiter ;
#end
#begin
-- Create function
CREATE FUNCTION `uuidToBinary`(_uuid BINARY(36)) RETURNS binary(16)
    DETERMINISTIC
    SQL SECURITY INVOKER
RETURN
  UNHEX(CONCAT(
            SUBSTR(_uuid, 15, 4),
            SUBSTR(_uuid, 10, 4),
            SUBSTR(_uuid,  1, 8),
            SUBSTR(_uuid, 20, 4),
            SUBSTR(_uuid, 25) ))
#end
#begin
-- Use UTC_TIMESTAMP without parenthesis
CREATE FUNCTION IF NOT EXISTS myfunc(a INT) RETURNS INT
BEGIN
    DECLARE result INT;
    SET result = UTC_TIMESTAMP;
    RETURN result;
END;
#end
#begin
-- https://dev.mysql.com/doc/refman/8.0/en/json-validation-functions.html#json-validation-functions-constraints
CREATE TABLE geo (
    coordinate JSON,
    CHECK(
        JSON_SCHEMA_VALID(
           '{
               "type":"object",
               "properties":{
                 "latitude":{"type":"number", "minimum":-90, "maximum":90},
                 "longitude":{"type":"number", "minimum":-180, "maximum":180}
               },
               "required": ["latitude", "longitude"]
           }',
           coordinate
        )
    )
);
#end
#begin
CREATE TABLE `tab1` (
  f4 FLOAT4,
  f8 FLOAT8,
  i1 INT1,
  i2 INT2,
  i3 INT3,
  i4 INT4,
  i8 INT8,
  lvb LONG VARBINARY,
  lvc LONG VARCHAR,
  lvcfull LONG BINARY CHARSET utf8 COLLATE utf8_bin,
  l LONG,
  mi MIDDLEINT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE trace_info (
id bigint(20) NOT NULL AUTO_INCREMENT,
trace_code varchar(20) NOT NULL DEFAULT '' COMMENT '溯源码',
whole_order_no varchar(20) NOT NULL DEFAULT '' COMMENT '购物订单号',
order_no varchar(20) NOT NULL DEFAULT '' COMMENT '购物子订单号',
express_no varchar(30) NOT NULL DEFAULT '' COMMENT '快递单号',
shipped_time datetime DEFAULT NULL COMMENT '发货时间',
received_time datetime DEFAULT NULL COMMENT '收货时间',
sku_code varchar(50) DEFAULT NULL COMMENT '商品sku',
sku_name varchar(255) DEFAULT '' COMMENT '商品名称',
is_refund tinyint(4) DEFAULT 0 COMMENT '是否退货，0，否，1是',
is_exchange tinyint(4) DEFAULT 0 COMMENT '是否换货，0，否，1是',
sap_code varchar(50) NOT NULL DEFAULT CURDATE() COMMENT '商品物料号',
create_time datetime DEFAULT CURTIME() COMMENT '创建时间',
update_time datetime DEFAULT NULL COMMENT '更新时间',
PRIMARY KEY (id),
UNIQUE KEY idx_unique_key (trace_code,order_no,sku_code),
KEY idx_order_no (order_no)
) ENGINE=InnoDB AUTO_INCREMENT=123 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
#end
-- Comments
-- SELECT V_PAYABLE_AMT, DIS_ADJUST_TOTAL_PAYABLE;
--	SELECT V_PAYABLE_AMT, DIS_ADJUST_TOTAL_PAYABLE;
#begin
-- Create procedure
-- The default value for local variables in a DECLARE statement should be an expression
-- src: https://dev.mysql.com/doc/refman/5.7/en/declare-local-variable.html
-- delimiter //
CREATE PROCEDURE procedure1()
BEGIN
  DECLARE var1 INT unsigned default 1;
  DECLARE var2 TIMESTAMP default CURRENT_TIMESTAMP;
  DECLARE var3 INT unsigned default 2 + var1;
END -- //-- delimiter ;
#end
#begin
-- delimiter //
CREATE PROCEDURE doiterate(p1 INT)
-- label which can be parsed as a beginning of IPv6 address
aaa:BEGIN
  label1:LOOP
    SET p1 = p1 + 1;
    IF p1 < 10 THEN ITERATE label1; END IF;
    LEAVE label1;
  END LOOP label1;
END -- //-- delimiter ;
#end
#begin
CREATE DEFINER=`system_user`@`%` PROCEDURE `update_order`(IN orderID bigint(11))
BEGIN  insert into order_config(order_id, attribute, value, performer)
       SELECT orderID, 'first_attr', 'true', 'AppConfig'
       WHERE NOT EXISTS (select 1 from inventory.order_config t1 where t1.order_id = orderID and t1.attribute = 'first_attr') OR
             EXISTS (select 1 from inventory.order_config p2 where p2.order_id = orderID and p2.attribute = 'first_attr' and p2.performer = 'AppConfig')
       ON DUPLICATE KEY UPDATE value = 'true',
                            performer = 'AppConfig'; -- Enable second_attr for order
END
#end
#begin
-- Create procedure
-- delimiter //
CREATE PROCEDURE makesignal(p1 INT)
BEGIN
  DECLARE error_text VARCHAR(255);
  IF (error_text != 'OK') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_text;
  END IF;
END -- //-- delimiter ;
#end
#begin
CREATE DEFINER=`bettingservice`@`stage-us-nj-app%` PROCEDURE `AggregatePlayerFactDaily`()
BEGIN
    DECLARE CID_min BIGINT;
    DECLARE CID_max BIGINT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SHOW ERRORS;
ROLLBACK;
END;

SELECT LastID+1 INTO CID_min FROM AggregateStatus
WHERE TableName = 'Combination_Transaction_Player_Fact';
SELECT Id INTO CID_max FROM Combination_Transaction ORDER BY Id DESC LIMIT 1;

START TRANSACTION;
UPDATE AggregateStatus SET LastId = CID_max, LastUpdated = CURRENT_TIMESTAMP
WHERE TableName = 'Combination_Transaction_Player_Fact';

INSERT INTO Combination_Transaction_Player_Fact
SELECT
    NULL `Id`,
    CT.Player_UID,
    CT.Tx_Type `Type`,
    DATE(BT.Timestamp) `Date`,
    SUM(CT.Real_Amount) `Real_Amount`,
    SUM(CT.Bonus_Amount) `Bonus_Amount`,
    BT.Currency_UID,
    COUNT(CT.Id) Tx_Count,
    SUM(IF(CT.Real_Amount>0,1,0)) `Real_Count`,
    SUM(IF(CT.Bonus_Amount>0,1,0)) `Bonus_Count`
FROM Combination_Transaction CT
    LEFT JOIN Betting_Transaction BT ON CT.Betting_Tx_ID = BT.ID
WHERE CT.Id BETWEEN CID_min
  AND CID_max
GROUP BY CT.Player_UID, CT.Tx_Type, DATE(BT.Timestamp)
ON DUPLICATE KEY UPDATE
                     Currency_UID = VALUES(Currency_UID),
                     Tx_Count     = Tx_Count + VALUES(Tx_Count),
                     Real_Amount  = Real_Amount + VALUES(Real_Amount),
                     Bonus_Amount = Bonus_Amount + VALUES(Bonus_Amount),
                     Real_Count   = Real_Count + VALUES(Real_Count),
                     Bonus_Count  = Bonus_Count + VALUES(Bonus_Count);
COMMIT;
END
#end
-- Create procedure
-- delimiter //
CREATE PROCEDURE makesignal(p1 INT)
BEGIN
  DECLARE error_text VARCHAR(255);
  IF (error_text != 'OK') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_text;
  END IF;
END -- //-- delimiter ;
#end
#begin
-- delimiter //
CREATE PROCEDURE set_unique_check()
BEGIN
    SET unique_checks=off;
    SET unique_checks=on;
END; -- //-- delimiter ;
#end
#begin
CREATE DEFINER=`prod_migrations`@`%` PROCEDURE `upsert_virtual_item`(IN name VARCHAR(45), IN type TINYINT UNSIGNED)
BEGIN
    SET @merchantId := (SELECT merchant_id FROM merchant LIMIT 1);
    IF @merchantId > 0 THEN
        SET @rows := (SELECT COUNT(*) FROM item WHERE item_type = type);
        IF @rows > 0 THEN
UPDATE item SET
                merchant_id = @merchantId,
                cz_title = name,
                price = 0,
                orderer = 2,
                takeaway = 0,
                currency_id = (
                    SELECT currency_currency_id
                    FROM merchant
                    WHERE merchant_id = @merchantId
                ),
                tax_vat_id = (
                    SELECT tax_vat.tax_vat_id
                    FROM tax_vat
                             JOIN merchant
                                  ON merchant.place_country_id = tax_vat.country_id
                                      AND merchant.merchant_id = @merchantId
                    WHERE tax_vat.default = 1
                ),
                item_measure_id = 1,
                kitchen_print = 0,
                deleted = 0,
                virtual = 1
WHERE item_type = type;
ELSE
            INSERT INTO item SET
                merchant_id = @merchantId,
                cz_title = name,
                price = 0,
                orderer = 2,
                takeaway = 0,
                currency_id = (
                    SELECT currency_currency_id
                    FROM merchant
                    WHERE merchant_id = @merchantId
                ),
                tax_vat_id = (
                    SELECT tax_vat.tax_vat_id
                    FROM tax_vat
                    JOIN merchant
                        ON merchant.place_country_id = tax_vat.country_id
                        AND merchant.merchant_id = @merchantId
                    WHERE tax_vat.default = 1
                ),
                item_measure_id = 1,
                kitchen_print = 0,
                deleted = 0,
                virtual = 1,
                item_type = type
            ;
END IF;
END IF;
END
#end
#begin
-- delimiter //
CREATE DEFINER=`reportwriter`@`%` PROCEDURE `sp_ds_DAL_TX_Impoundment`(IN pDateFrom datetime, IN pDateTo datetime)
BEGIN

    SET @goliveDate = '2023-05-02 02:00:00';
    set @pRegion = 'DAL-TX';
-- set @pDateFrom = '2023-02-01 00:00:00';
-- set @pDateTo = '2023-03-10 00:00:00';
    set @pDateFrom = pDateFrom;
    set @pDateTo = pDateTo;

    set @contractAmount = 21.03;

with
Temp1 as
(
    select l.code                                                            as lotCode
         , fi.Id                                                             AS FeeItemID
         , fi.unitBillingPrice                                               as billingPrice
         , eq.equipmentClass
         , a.customerCode
         , v.impoundStatus
         , tc.companyCode                                                    AS impoundCompany
         , b.companyCode                                                     AS towOperator
         , v.id                                                              AS vehicleId
         , re.reasoncode
         , v.towReferenceNumber

         , fn_CalculateTimeZoneOffset(regionCode, v.clearedDate, 'DISPLAY')  AS towDate
         , fn_CalculateTimeZoneOffset(regionCode, v.releaseDate, 'DISPLAY')  AS releaseDate
         , fn_CalculateTimeZoneOffset(regionCode, fi.createdDate, 'DISPLAY') AS feeDate

         , f.code
         , fi.totalBillingPricePretax

    from ims_vehicle v
             join ref_region r
                  on v.regionId = r.regionId

             INNER JOIN ims_fee_event fe ON v.id = fe.vehicleId
             INNER JOIN ims_fee_item fi ON fe.id = fi.feeEventId
             INNER JOIN ims_fee f ON fi.feeId = f.id
             INNER JOIN ims_fee_category fc ON f.feeCategoryEnumCode = fc.enumcode

             INNER JOIN ref_customer a ON v.accountId = a.customerId
             INNER JOIN ref_reason re ON v.reasonId = re.reasonId
             INNER JOIN ref_tow_company tc ON v.currentImpoundOperatorId = tc.towCompanyId

             JOIN ref_tow_company b ON v.towOperatorId = b.towCompanyId
             left join ref_lot l on v.currentLotId = l.id
             join ref_equipment eq
                  on v.equipmentId = eq.id

    where r.regionCode = @pRegion
      and v.releaseDate >= @pDateFrom
      and v.releaseDate < @pDateTo
      and v.clearedDate >= @goliveDate
      and b.companyCode != 'ART-DAL-TX'
      and v.impoundStatus = 'RELEASED'
)

    select lotCode
         , Temp1.vehicleId         as "Vehicle ID"
         , towReferenceNumber      as "Tow Reference Number"
         , equipmentClass          as "Class"
         , impoundStatus           as "Status"
         , customerCode            as "Customer"
         , impoundCompany          as "Impound Company"
         , towOperator             as "Tow Operator"
         , towDate                 as "Tow Date"
         , releaseDate             as "Release Date"
         , billingPrice            as "Auto Pound Authorized Fee"

         , billingPrice - @contractAmount   as "rev threshold"

-- ,DATEDIFF(s.timeTo, s.timeFrom) as "Storage Days"
         , null                    as "Storage Days"
         , null                    as timeFrom
         , null                    as timeTo
         , billingPrice            as "Authorized Impoundment Fee"

         , (billingPrice - @contractAmount)/2 + @contractAmount as "rev share amount"
    from Temp1

    where code in ('ImpoundmentFee')
      and lotCode like '%PEAKA%';

END; -- //-- delimiter ;
#end
#begin
-- Create Role
create role 'RL_COMPLIANCE_NSA';
create role if not exists 'RL_COMPLIANCE_NSA';
CREATE ROLE 'admin', 'developer';
CREATE ROLE 'webapp'@'localhost';
#end
#begin
CREATE VIEW view_with_cte1 AS
WITH cte1 AS
(
    SELECT column_1 AS a, column_2 AS b
    FROM table1
)
SELECT a, b FROM cte1;
#end
#begin
CREATE VIEW view_with_cte2 AS
WITH cte1 (col1, col2) AS
(
  SELECT 1, 2
  UNION ALL
  SELECT 3, 4
),
cte2 (col1, col2) AS
(
  SELECT 5, 6
  UNION ALL
  SELECT 7, 8
)
SELECT col1, col2 FROM cte;
#end
#begin
CREATE VIEW view_with_cte3 AS
WITH cte (col1, col2) AS
(
  SELECT 1, 2
  UNION ALL
  SELECT 3, 4
)
SELECT col1, col2 FROM cte;
#end
#begin
CREATE VIEW view_with_cte4 AS
WITH RECURSIVE cte (n) AS
(
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM cte WHERE n < 5
)
SELECT * FROM cte;
#end
#begin
CREATE VIEW `invoice_payments_stats` AS
SELECT
    `i`.`id` AS `id`
FROM (`invoices` `i` JOIN lateral (SELECT MAX(`ip`.`date`) AS `latest_payment` FROM `invoice_payments` `ip`) `ps`);
#end
#begin
lock tables t1 read;
lock table t1 read local;
#end

#begin
CREATE OR REPLACE VIEW view_name AS
WITH my_values(val1, val2) AS (
    VALUES (1, 'One'),
           (2, 'Two')
)
SELECT v.val1, v.val2 FROM my_values v;
#end

#begin
CREATE DEFINER=`gpuser`@`%` PROCEDURE `test_parse_array` (IN val INT)
BEGIN
DECLARE array VARCHAR(50);

SELECT 1;

END
#end

#begin
CREATE DEFINER=`peuser`@`%` PROCEDURE `test_utf`()
BEGIN
    SET @Ν_greece := 1, @N_latin := 'test';
SELECT
    @Ν_greece
     ,@N_latin;
END
#end

#begin
CREATE PROCEDURE test_union()
BEGIN
    (SELECT id FROM test_auto_inc)
    UNION ALL
    SELECT id FROM test_auto_inc;
END
#end

#begin
CREATE PROCEDURE test_union()
BEGIN
    (SELECT id FROM test_auto_inc)
    UNION ALL
    SELECT id FROM test_auto_inc
    UNION ALL
    SELECT id FROM test_auto_inc ORDER BY id;
END
#end

#begin
CREATE PROCEDURE test_union()
BEGIN
    (SELECT id FROM test_auto_inc)
    UNION ALL
    (SELECT id FROM test_auto_inc)
    UNION ALL
    SELECT id FROM test_auto_inc ORDER BY id;
END
#end

#begin
CREATE DEFINER=`PEUSER`@`%` PROCEDURE `SANDBOX`.`TEST_UNION`( )
BEGIN
SELECT ID ,SUM(COL_1) AS SUM_COL_1
FROM (
    (SELECT ID ,COL_1 FROM TEST_AUTO_INC
    UNION ALL
    SELECT ID ,COL_1 FROM TEST_AUTO_INC TAI)
    UNION ALL
    (SELECT ID ,COL_1 FROM TEST_AUTO_INC TAI)
)SS
GROUP BY 1
ORDER BY 1
;
END
#end

#begin
CREATE PROCEDURE TEST_UPDATE()
BEGIN
    UPDATE TEST_AUTO_INC AI
        JOIN TEST_JOIN_LIMIT JL ON JL.ID = AI.ID
    SET AI.COL_1 = NULL
    LIMIT 500;
END
#end

#begin
CREATE DEFINER=`bcadmin`@`%` PROCEDURE `sp_next_available_otc_instance_strategy_id`()
BEGIN
  SELECT MIN(st.value) AS strategy_id
  FROM SEQUENCE_TABLE(100000) ST
  JOIN btc_quant.stratId_ranges r ON r.stratId0 <= st.value AND st.value < r.stratId1
  LEFT JOIN otc_instance i ON i.strategy_id=st.value
  WHERE r.category = 'otc' AND now() BETWEEN validFromDate AND COALESCE(validToDate, now())
  AND i.strategy_id IS NULL;
END
#end