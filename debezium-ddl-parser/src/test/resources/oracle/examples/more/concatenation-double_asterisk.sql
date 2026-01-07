CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

PROCEDURE double_asterisk_test;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case compiles and runs fine on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    PROCEDURE double_asterisk_test
    IS
        value number(3);
    BEGIN

        value := 2 * 2 ** 3;
        -- The expected output value is 16, and the actual output value is also 16
        dbms_output.put_line('value=' || value);

    END double_asterisk_test;

END TEST_PKG_LXD;