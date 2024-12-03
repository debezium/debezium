-- alter table
alter table ship_class add column ship_spec varchar(150) first, add somecol int after start_build, algorithm=instant;
alter table t3 add column (c2 decimal(10, 2) comment 'comment`' null, c3 enum('abc', 'cba', 'aaa')), add index t3_i1 using btree (c2) comment 'some index';
alter table t3 add column (c4 decimal(10, 2) comment 'comment`' null), add index t3_i2 using btree (c4) comment 'some index';
alter table t1 stats_auto_recalc=default, stats_sample_pages=50.0;
alter table add_test alter index ix_add_test_col1 invisible;
alter table add_test alter index ix_add_test_col1 visible;
alter table add_test add column optional bool default 0 null;
alter table add_test add column empty varchar(255);
alter table add_test add column geometry int;
alter table add_test drop foreign key fk;
ALTER TABLE t1 ADD PARTITION (PARTITION p3 VALUES LESS THAN (2002));

-- alter database
alter database test_1 default encryption = 'Y' read only = 1;

-- alter user
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password history default;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password history 90;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password reuse interval default;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password reuse interval 360 DAY;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password require current;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password require current optional;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password require current default;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock failed_login_attempts 5;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password_lock_time 2;
alter user 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password_lock_time unbounded;
alter user 'user'@'%' identified by 'newpassword' retain current password;
alter user if exists 'user'@'%' identified with 'mysql_native_password' as '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'
    require none password expire default account unlock password history default;

-- get diagnostics
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

-- create table
create table table_with_keyword_as_column_name (geometry int, national int);
create table transactional_table(name varchar(255), class_id int, id int) transactional=1;
create table transactional(name varchar(255), class_id int, id int);
create table add_test(col1 varchar(255), col2 int, col3 int);
create table statement(id int);

CREATE TABLE `TABLE1` (
`COL1` INT(10) UNSIGNED NOT NULL,
`COL2` VARCHAR(32) NOT NULL,
`COL3` ENUM (`VAR1`,`VAR2`, `VAR3`) NOT NULL,
PRIMARY KEY (`COL1`, `COL2`, `COL3`),
CLUSTERING KEY `CLKEY1` (`COL3`, `COL2`))
ENGINE=TOKUDB DEFAULT CHARSET=CP1251;

CREATE TABLE PARTICIPATE_ACTIVITIES (
    ID BIGINT NOT NULL AUTO_INCREMENT,
    USER_ID BIGINT NOT NULL,
    PRIMARY KEY (ID) USING BTREE)
ENGINE=INNODB AUTO_INCREMENT=1979503 DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI SECONDARY_ENGINE=RAPID;

-- create database
create database db_with_character_set_eq character set = DEFAULT;

-- select statement
SELECT mod(3,2);
SELECT SCHEMA();
SELECT
  year, country, product, profit,
  SUM(profit) OVER() AS total_profit,
  SUM(profit) OVER(PARTITION BY country) AS country_profit
FROM sales
  ORDER BY country, year, product, profit;
SELECT
  year, country, product, profit,
  ROW_NUMBER() OVER(PARTITION BY country) AS row_num1,
  ROW_NUMBER() OVER(PARTITION BY country ORDER BY year, product) AS row_num2
FROM sales;

-- https://dev.mysql.com/doc/refman/8.0/en/lateral-derived-tables.html
SELECT
  salesperson.name,
  max_sale.amount,
  max_sale.customer_name
FROM
  salesperson,
  LATERAL
  (SELECT amount, customer_name
    FROM all_sales
    WHERE all_sales.salesperson_id = salesperson.id
    ORDER BY amount DESC LIMIT 1)
  AS max_sale;

-- grant
GRANT AUDIT_ADMIN, BACKUP_ADMIN, BINLOG_ADMIN, BINLOG_ENCRYPTION_ADMIN, CLONE_ADMIN, CONNECTION_ADMIN,
ENCRYPTION_KEY_ADMIN, FIREWALL_ADMIN, FIREWALL_USER, GROUP_REPLICATION_STREAM, INNODB_REDO_LOG_ARCHIVE,
NDB_STORED_USER, PERSIST_RO_VARIABLES_ADMIN, REPLICATION_APPLIER, REPLICATION_SLAVE_ADMIN, RESOURCE_GROUP_ADMIN,
RESOURCE_GROUP_USER, ROLE_ADMIN, SESSION_VARIABLES_ADMIN, SET_USER_ID, SENSITIVE_VARIABLES_OBSERVER, SHOW_ROUTINE, SYSTEM_VARIABLES_ADMIN, AUTHENTICATION_POLICY_ADMIN,
TABLE_ENCRYPTION_ADMIN, VERSION_TOKEN_ADMIN, XA_RECOVER_ADMIN, AUDIT_ABORT_EXEMPT, FIREWALL_EXEMPT, SKIP_QUERY_REWRITE, TELEMETRY_LOG_ADMIN, TP_CONNECTION_ADMIN ON *.* TO `admin`@`localhost`;

-- revoke
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'retool'@

























