#begin
-- Alter Table
alter table ship_class add column ship_spec varchar(150) first, add somecol int after start_build, algorithm=instant;
alter table t3 add column (c2 decimal(10, 2) comment 'comment`' null, c3 enum('abc', 'cba', 'aaa')), add index t3_i1 using btree (c2) comment 'some index';
alter table t3 add column (c4 decimal(10, 2) comment 'comment`' null), add index t3_i2 using btree (c4) comment 'some index';
alter table t2 add constraint t2_pk_constraint primary key (1c), alter column `_` set default 1;
alter table t2 drop constraint t2_pk_constraint;
alter table ship_class change column somecol col_for_del tinyint first;
alter table ship_class drop col_for_del;
alter table t3 drop index t3_i1;
alter table t3 drop index if exists t3_i2;
alter table childtable drop index fk_idParent_parentTable;
alter table t2 drop primary key;
alter table t3 rename to table3column;
alter table childtable add constraint `fk1` foreign key (idParent) references parenttable(id) on delete restrict on update cascade;
alter table table3column default character set = cp1251;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 collate utf8mb4_bin not null;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 binary not null;
alter table `test` change `id` `id` varchar(10) character set utf8mb4 binary null default null;
alter table table1 add primary key (id);
alter table table1 add primary key table_pk (id);
alter table table1 add primary key `table_pk` (id);
alter table table1 add primary key `table_pk` (`id`);
alter table add_test add column if not exists col1 varchar(255);
alter table add_test add column if not exists col4 varchar(255);
alter table add_test add index if not exists ix_add_test_col1 using btree (col1) comment 'test index';
alter table add_test add index if not exists ix_add_test_col4 using btree (col4) comment 'test index';
alter table add_test alter index ix_add_test_col1 invisible;
alter table add_test alter index ix_add_test_col1 visible;
alter table add_test change column if exists col8 col9 tinyint;
alter table add_test change column if exists col3 col5 tinyint;
alter table add_test modify column if exists col9 tinyint;
alter table add_test modify column if exists col5 varchar(255);
alter table add_test drop column if exists col99;
alter table add_test drop column if exists col5;
alter table add_test add column optional bool default 0 null;
#end
#begin
-- Alter database
alter database test default character set = utf8;
alter schema somedb_name upgrade data directory name;
#end
#begin
-- Alter event
alter definer = current_user event someevent on schedule at current_timestamp + interval 30 minute;
alter definer = 'ivan'@'%' event someevent on completion preserve;
alter definer = 'ivan'@'%' event someevent rename to newsomeevent;
alter event newsomeevent enable comment 'some comment';
-- delimiter //
alter definer = current_user event newsomeevent on schedule at current_timestamp + interval 2 hour
rename to someevent disable
do begin update test.t2 set 1c = 1c + 1; end; -- //
-- delimiter ;
#end
#begin
-- Alter function/procedure
alter function f_name comment 'some funct' language sql sql security invoker;
alter function one_more_func contains sql sql security definer;
alter procedure p_name comment 'some funct' language sql sql security invoker;
alter procedure one_more_proc contains sql sql security definer;
#end
#begin
-- Alter logfile group
-- http://dev.mysql.com/doc/refman/5.6/en/alter-logfile-group.html
ALTER LOGFILE GROUP lg_3 ADD UNDOFILE 'undo_10.dat' INITIAL_SIZE=32M ENGINE=NDBCLUSTER;
ALTER LOGFILE GROUP lg_1 ADD UNDOFILE 'undo_10.dat' wait ENGINE=NDB;
#end
#begin
-- Alter server
-- http://dev.mysql.com/doc/refman/5.6/en/alter-server.html
ALTER SERVER s OPTIONS (USER 'sally');
#end
#begin
-- Alter tablespace
alter tablespace tblsp_1 add datafile 'filename' engine = ndb;
alter tablespace tblsp_2 drop datafile 'deletedfilename' wait engine ndb;
#end
#begin
-- Alter view
alter view my_view1 as select 1 union select 2 limit 0,5;
alter algorithm = merge view my_view2(col1, col2) as select * from t2 with check option;
alter definer = 'ivan'@'%' view my_view3 as select count(*) from t3;
alter definer = current_user sql security invoker view my_view4(c1, 1c, _, c1_2) 
	as select * from  (t1 as tt1, t2 as tt2) inner join t1 on t1.col1 = tt1.col1;
#end
#begin
-- Alter user
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
rename user user1@100.200.1.1 to user2@100.200.1.2;
rename user user1@100.200.1.1 to user2@2001:0db8:85a3:0000:0000:8a2e:0370:7334;
#end
ALTER TABLE t1 ADD PARTITION (PARTITION p3 VALUES LESS THAN (2002));
ALTER TABLE t1 ADD PARTITION IF NOT EXISTS (PARTITION p3 VALUES LESS THAN (2002));
