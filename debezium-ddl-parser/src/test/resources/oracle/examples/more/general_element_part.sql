CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

TYPE type_array IS ARRAY(5) OF PLS_INTEGER;

FUNCTION return_array_test RETURN type_array;
PROCEDURE get_array_test;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case compiles and runs fine on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    FUNCTION return_array_test RETURN type_array
    IS
        test_array type_array;
    BEGIN
        test_array := type_array(1, 2, 3, 4, 5);
        return test_array;
        
    END return_array_test;

    PROCEDURE get_array_test
    IS
        value PLS_INTEGER;
    BEGIN

        value := return_array_test()(3);
        -- The expected output value is 3, and the actual output value is also 3
        dbms_output.put_line('value=' || value);

    END get_array_test;

END TEST_PKG_LXD;