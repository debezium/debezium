CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

PROCEDURE values_test;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case compiles and runs fine on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    PROCEDURE values_test
    IS

        TYPE value_type IS RECORD (ID PLS_INTEGER, VALUE VARCHAR2(100));
        TYPE list IS TABLE OF value_type INDEX BY PLS_INTEGER;
        tmp_list list;

    BEGIN

        tmp_list(0).ID := 1;
        tmp_list(0).VALUE := 'test_value';

        --The DDL of TEST_TB is 'CREATE TABLE TEST_TB(id NUMBER(3), value VARCHAR2(100))'
        INSERT INTO TEST_TB VALUES tmp_list(0);

    END values_test;

END TEST_PKG_LXD;