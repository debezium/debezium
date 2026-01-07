CREATE AUDIT POLICY table_pol
  PRIVILEGES CREATE ANY TABLE, DROP ANY TABLE;

CREATE AUDIT POLICY dml_pol
  ACTIONS DELETE on hr.employees,
INSERT on hr.employees,
UPDATE on hr.employees,
    ALL on hr.departments;
CREATE AUDIT POLICY security_pol
  ACTIONS ADMINISTER KEY MANAGEMENT;

CREATE AUDIT POLICY dir_pol
  ACTIONS READ DIRECTORY, WRITE DIRECTORY, EXECUTE DIRECTORY;

CREATE AUDIT POLICY all_actions_pol
  ACTIONS ALL;

CREATE AUDIT POLICY dp_actions_pol
  ACTIONS COMPONENT = datapump IMPORT;

CREATE AUDIT POLICY java_pol
  ROLES java_admin, java_deploy;

CREATE AUDIT POLICY hr_admin_pol
  PRIVILEGES CREATE ANY TABLE, DROP ANY TABLE
  ACTIONS DELETE on hr.employees,
INSERT on hr.employees,
UPDATE on hr.employees,
    ALL on hr.departments,
    LOCK TABLE
    ROLES audit_admin, audit_viewer;

CREATE AUDIT POLICY order_updates_pol
  ACTIONS UPDATE ON oe.orders
                     WHEN 'SYS_CONTEXT(''USERENV'', ''IDENTIFICATION_TYPE'') = ''EXTERNAL'''
                     EVALUATE PER SESSION;

CREATE AUDIT POLICY emp_updates_pol
  ACTIONS DELETE on hr.employees,
INSERT on hr.employees,
UPDATE on hr.employees
    WHEN 'UID NOT IN (100, 105, 107)'
    EVALUATE PER STATEMENT;

CREATE AUDIT POLICY local_table_pol
  PRIVILEGES CREATE ANY TABLE, DROP ANY TABLE
  CONTAINER = CURRENT;

CREATE AUDIT POLICY common_role1_pol
  ROLES c##role1
  CONTAINER = ALL;
