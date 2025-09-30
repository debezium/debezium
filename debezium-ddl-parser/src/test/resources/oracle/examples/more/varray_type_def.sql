CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

PROCEDURE varray_def_test;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case passed on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    PROCEDURE varray_def_test
    IS
        -- All of these varray definition are valid
        TYPE type_array1 IS VARRAY(5) OF PLS_INTEGER;
        TYPE type_array2 IS VARYING ARRAY(5) OF PLS_INTEGER;
        TYPE type_array3 IS VARRAY(5) OF PLS_INTEGER;

        array1 type_array1;
        array2 type_array2;
        array3 type_array3;
    BEGIN

        array1 := type_array1(1);
        array2 := type_array2(2);
        array3 := type_array3(3);

        dbms_output.put_line('v1=' || array1(1) || ', v2=' || array2(1) || ', v3=' || array3(1));

    END varray_def_test;

END TEST_PKG_LXD;