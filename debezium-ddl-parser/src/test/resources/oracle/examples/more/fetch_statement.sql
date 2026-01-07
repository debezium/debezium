CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

PROCEDURE fetch_statement_test;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case compiles and runs fine on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    PROCEDURE fetch_statement_test
    IS

        TYPE value_type IS RECORD (v PLS_INTEGER);
        TYPE list IS TABLE OF value_type INDEX BY PLS_INTEGER;
        tmp_list list;

        CURSOR fetch_into_cursor IS
        SELECT 2 FROM dual;

        -- The DDL of TEST_TB is 'CREATE TABLE TEST_TB(id NUMBER(3), value VARCHAR2(10))'
        TYPE IDList IS TABLE OF TEST_TB.ID%TYPE;
        TYPE ValueList IS TABLE OF TEST_TB.VALUE%TYPE;

        id_list IDList;
        value_list ValueList;

        CURSOR fetch_bulk_collect_cursor IS
            SELECT ID, VALUE FROM TEST_TB;
    BEGIN

        OPEN fetch_into_cursor;
            FETCH fetch_into_cursor INTO tmp_list(0).v;
        CLOSE fetch_into_cursor;

        -- The expected output value is 2, and the actual value is also 2.
        dbms_output.put_line('tmp_list(0).v=' || tmp_list(0).v);

        OPEN fetch_bulk_collect_cursor;
            -- Fetch 2 fields into id_list and value_list separately, each field has 2 values, the number of values is limited by tmp_list(0).v.
            FETCH fetch_bulk_collect_cursor BULK COLLECT INTO id_list, value_list LIMIT tmp_list(0).v;
        CLOSE fetch_bulk_collect_cursor;

        FOR i IN id_list.FIRST .. id_list.LAST
        LOOP
            -- print 2 records
            dbms_output.put_line('id=' || id_list(i) || ', value=' || value_list(i));
        END LOOP;

    END fetch_statement_test;

END TEST_PKG_LXD;