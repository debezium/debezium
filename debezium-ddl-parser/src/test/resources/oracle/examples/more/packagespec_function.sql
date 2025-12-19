-- Adding pipelined and deterministic keyword in one function spec
CREATE OR REPLACE PACKAGE TEST IS 
    FUNCTION TEST_FUNC RETURN NUMBER PIPELINED DETERMINISTIC;
END TEST;
