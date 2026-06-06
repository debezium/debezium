CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

PROCEDURE double_mod;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case compiles and runs fine on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    PROCEDURE double_mod
    IS
        value number(3);
    BEGIN

        value := 17 mod 3 ** 2;
        -- The expected output value is 8, and the actual output value is also 8
        dbms_output.put_line('value=' || value);

    END double_mod;

END TEST_PKG_LXD;