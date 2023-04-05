REVOKE DROP ANY TABLE
    FROM hr, oe;
REVOKE dw_manager
    FROM sh;
REVOKE CREATE TABLESPACE
    FROM dw_manager;
REVOKE dw_user
    FROM dw_manager;
REVOKE DELETE
    ON orders FROM hr;
REVOKE UPDATE
    ON emp_details_view FROM public;
REVOKE INHERIT PRIVILEGES ON USER sh FROM hr;
REVOKE SELECT
    ON hr.departments_seq FROM oe;
REVOKE REFERENCES
    ON hr.employees
    FROM oe
    CASCADE CONSTRAINTS;
REVOKE READ ON DIRECTORY bfile_dir FROM hr;