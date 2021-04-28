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
-- Create Table
create table new_t  (like t1);
create table log_table(row varchar(512));
create table log_table(row character(512));
create table ships(name varchar(255), class_id int, id int);
create table ships_guns(guns_id int, ship_id int);
create table guns(id int, power decimal(7,2), callibr decimal(10,3));
create table ship_class(id int, class_name varchar(100), tonange decimal(10,2), max_length decimal(10,2), start_build year, end_build year(4), max_guns_size int);
create table `some table $$`(id int auto_increment key, class varchar(10), data binary) engine=MYISAM;
create table `parent_table`(id int primary key, column1 varchar(30), index parent_table_i1(column1(20)), check(char_length(column1)>10)) engine InnoDB;
create table child_table(id int unsigned auto_increment primary key, id_parent int references parent_table(id) match full on update cascade on delete set null) engine=InnoDB;
create table `another some table $$` like `some table $$`;
create table `actor` (`last_update` timestamp default CURRENT_TIMESTAMP, `birthday` datetime default CURRENT_TIMESTAMP ON UPDATE LOCALTIMESTAMP);
create table boolean_table(c1 bool, c2 boolean default true);
create table table_with_character_set_eq (id int, data varchar(50)) character set = default;
create table table_with_character_set (id int, data varchar(50)) character set default;
create table table_with_visible_index (id int, data varchar(50), UNIQUE INDEX `data_UNIQUE` (`data` ASC) VISIBLE);
create table table_with_index (id int, data varchar(50), UNIQUE INDEX `data_UNIQUE` (`data` ASC));
create table transactional_table(name varchar(255), class_id int, id int) transactional=1;
create table transactional(name varchar(255), class_id int, id int);
create table add_test(col1 varchar(255), col2 int, col3 int);
create table blob_test(id int, col1 blob(45));
CREATE TABLE `user_account` ( `id1` bigint(20) unsigned NOT NULL DEFAULT nextval(`useraccount`.`user_account_id_seq`));
create table žluťoučký (kůň int);
CREATE TABLE staff (PRIMARY KEY (staff_num), staff_num INT(5) NOT NULL, first_name VARCHAR(100) NOT NULL, pens_in_drawer INT(2) NOT NULL, CONSTRAINT pens_in_drawer_range CHECK(pens_in_drawer BETWEEN 1 AND 99));
create table column_names_as_aggr_funcs(min varchar(100), max varchar(100), sum varchar(100), count varchar(100));
CREATE TABLE char_table (c1 CHAR VARYING(10), c2 CHARACTER VARYING(10), c3 NCHAR VARYING(10));
CREATE TABLE generated_persistent(id int NOT NULL AUTO_INCREMENT, ip_hash char(64) AS (SHA2(CONCAT(`token`, COALESCE(`ip`, "")), 256)) PERSISTENT, persistent int, PRIMARY KEY (`id`), UNIQUE KEY `token_and_ip_hash` (`ip_hash`)) ENGINE=InnoDB;
create table rack_shelf_bin ( id int unsigned not null auto_increment unique primary key, bin_volume decimal(20, 4) default (bin_len * bin_width * bin_height));
CREATE TABLE `tblSRCHjob_desc` (`description_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, `description` mediumtext NOT NULL, PRIMARY KEY (`description_id`)) ENGINE=TokuDB AUTO_INCREMENT=4095997820 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=TOKUDB_QUICKLZ;

CREATE TABLE table_items (id INT, purchased DATE)
    PARTITION BY RANGE( YEAR(purchased) )
        SUBPARTITION BY HASH( TO_DAYS(purchased) )
        SUBPARTITIONS 2 (
        PARTITION p0 VALUES LESS THAN (1990),
        PARTITION p1 VALUES LESS THAN (2000),
        PARTITION p2 VALUES LESS THAN MAXVALUE
    );

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
#end
#begin
-- Rename table
-- http://dev.mysql.com/doc/refman/5.6/en/rename-table.html
RENAME TABLE old_table TO tmp_table, new_table TO old_table, tmp_table TO new_table;
RENAME TABLE table_b TO table_a;
RENAME TABLE current_db.tbl_name TO other_db.tbl_name;
#end
#begin
-- Truncate table
truncate table t1;
truncate parent_table;
truncate `#`;
truncate `#!^%$`;
#end
#begin
-- Create database
create database somedb;
create schema if not exists myschema;
create schema `select` default character set = utf8;
create database if not exists `current_date` character set cp1251;
create database super default character set utf8 collate = utf8_bin character set utf8 collate utf8_bin;
create database db_with_character_set_eq character set = DEFAULT;
create database db_with_character_set character set default;
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
create index ix_add_test_col1 on add_test(col1) comment 'test index' using btree;
#end
#begin
create index myindex on t1(col1) comment 'test index' comment 'some test' using btree;
create or replace index myindex on t1(col1) comment 'test index' comment 'some test' using btree;
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
-- Create view
create or replace view my_view1 as select 1 union select 2 limit 0,5;
create algorithm = merge view my_view2(col1, col2) as select * from t2 with check option;
create or replace definer = 'ivan'@'%' view my_view3 as select count(*) from t3;
create or replace definer = current_user sql security invoker view my_view4(c1, 1c, _, c1_2) 
	as select * from  (t1 as tt1, t2 as tt2) inner join t1 on t1.col1 = tt1.col1;

#end
#begin
-- Create function
-- delimiter //
CREATE FUNCTION `func1`() RETURNS varchar(5) CHARSET utf8 COLLATE utf8_unicode_ci
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
CREATE FUNCTION myfunc(a INT) RETURNS INT
BEGIN
    DECLARE result INT;
    SET result = UTC_TIMESTAMP;
    RETURN result;
END;
#end
#begin
-- From MariaDB 10.4.3, the JSON_VALID function is automatically used as a CHECK constraint for the JSON data type alias in order to ensure that a valid json document is inserted.
-- src: https://mariadb.com/kb/en/json_valid/
CREATE TABLE `global_priv` (
    `Host` CHAR(60) COLLATE utf8_bin NOT NULL DEFAULT '',
    `User` CHAR(80) COLLATE utf8_bin NOT NULL DEFAULT '',
    `Privilege` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '{}' CHECK (json_valid(`Privilege`)),
    PRIMARY KEY (`Host`,`User`)
) ENGINE=Aria DEFAULT CHARSET=utf8 COLLATE=utf8_bin PAGE_CHECKSUM=1 TRANSACTIONAL=1 COMMENT='Users and global privileges';
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
-- Create procedure
-- delimiter //
CREATE PROCEDURE doiterate(p1 INT)
label2:BEGIN
  label1:LOOP
    SET p1 = p1 + 1;
    IF p1 < 10 THEN ITERATE label1; END IF;
    LEAVE label1;
  END LOOP label1;
END -- //-- delimiter ;
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
