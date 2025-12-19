-- column

comment on column employees.employee_id is 'Primary key of employees table.';

comment on column hr.employees.employee_id is 'Multiline
comment on column.';

comment on column "hr"."employees"."employee_id" is 'Primary key of employees table.';


-- add example: fix when comment on column with schema, the parser is report syntax error
comment on column s.a.c1 is 'comment';

-- table

comment on table employees is 'employees table. Contains 107 rows.';

comment on table hr.employees is 'employees table. Contains 107 rows.';

comment on table "hr"."employees" is 'employees table. Contains 107 rows.';

comment on table "my schema"."my table" is 'Some demo table with space in its name
and a multiline comment.';

COMMENT ON MATERIALIZED VIEW "MONITOR"."SQL_ALERT_LOG_ERRORS" IS
'snapshot table for snapshot MONITOR.SQL_ALERT_LOG_ERRORS';