drop table green_table;

create table green_table
(
  green_col_one number(10) not null disable
, green_col_two varchar2(64)
)
tablespace "greenspace"
;

Drop	Table	Green_Table; Create Table green_table
(
  green_col_one number(10) not null disable
, green_col_two varchar2(64)
)
Tablespace USERS
;

insert into green_table (green_col_one, green_col_two)
values (100021, 'green-varchar-21');
insert into green_table (green_col_one, green_col_two)
values (100022, 'green-varchar-22');
insert into green_table (green_col_one, green_col_two)
values (100023, 'green-varchar-23');

select *
from green_table;
