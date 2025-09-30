-- forall syntax: https://docs.oracle.com/en/database/oracle/oracle-database/21/lnpls/FORALL-statement.html#GUID-C45B8241-F9DF-4C93-8577-C840A25963DB
-- forall exception handling: https://docs.oracle.com/en/database/oracle/oracle-database/21/lnpls/plsql-optimization-and-tuning.html#GUID-DAF46F06-EF3F-4B1A-A518-5238B80C69FA

CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

PROCEDURE forall_and_exception_after_forall;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case passed on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    PROCEDURE forall_and_exception_after_forall
    IS
        TYPE IdList IS TABLE OF NUMBER;
        ids IdList := IdList(10, 20, 30);

        error_message  VARCHAR2(100);
        bad_stmt_no    PLS_INTEGER;

        dml_errors  EXCEPTION;
        PRAGMA EXCEPTION_INIT(dml_errors, -24381);
    BEGIN
        -- Populate table:
        -- The DDL of TEST_TB is 'CREATE TABLE TEST_TB(id NUMBER(3), value VARCHAR2(10))'
        INSERT INTO TEST_TB (id, value) VALUES (10, 'v_10');
        INSERT INTO TEST_TB (id, value) VALUES (20, 'value_20');
        INSERT INTO TEST_TB (id, value) VALUES (30, 'v_30');
        COMMIT;

        -- forall_statement syntax
        FORALL j IN ids.FIRST..ids.LAST SAVE EXCEPTIONS
            -- Append 5-character string to each value:
            UPDATE TEST_TB SET value = value || '. add'
            WHERE id = ids(j);

        EXCEPTION
          WHEN dml_errors THEN
          -- Handling FORALL Exceptions After FORALL Statement Completes
            FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
              error_message := SQLERRM(-(SQL%BULK_EXCEPTIONS(i).ERROR_CODE));
              DBMS_OUTPUT.PUT_LINE (error_message);

              bad_stmt_no := SQL%BULK_EXCEPTIONS(i).ERROR_INDEX;
              DBMS_OUTPUT.PUT_LINE('Bad statement #: ' || bad_stmt_no);

            END LOOP;

            COMMIT;  -- Commit results of successful updates

            WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Unrecognized error.');
              RAISE;
    END forall_and_exception_after_forall;

END TEST_PKG_LXD;