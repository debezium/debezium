CREATE OR REPLACE PACKAGE TEST_PKG_LXD
AS
-- Package header

PROCEDURE block_test;

END TEST_PKG_LXD;

CREATE OR REPLACE PACKAGE BODY TEST_PKG_LXD
AS
-- Package body
-- This test case compiles and runs fine on 'Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production'

    PROCEDURE block_test
    IS
    BEGIN

        DECLARE
            v1 PLS_INTEGER := 1;
        BEGIN
            dbms_output.put_line('v1=' || v1);
        END;

        DECLARE
        BEGIN
            dbms_output.put_line('empty declare_spec is ok');
        END;

        BEGIN
            dbms_output.put_line('no DECLARE no declare_spec is ok too');
        END;

        -- the block below is invalid
        -- v2 PLS_INTEGER := 1;
        -- BEGIN
        --     dbms_output.put_line('');
        -- END;

    END block_test;

END TEST_PKG_LXD;