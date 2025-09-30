DECLARE
    NUX NUMBER := 1;
BEGIN
-- Searched case statement
    CASE
        WHEN 1=1 THEN
            DBMS_OUTPUT.PUT_LINE('Hello');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Something went wrong!');
    END;

-- Simple case statement
    CASE NUX
        WHEN 1 THEN
            DBMS_OUTPUT.PUT_LINE('Hello');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Something went wrong!');
    END;

-- Searched case expression
    NUX := CASE
        WHEN 1 THEN 1
        ELSE 0
    END;

-- Simple case expression
    NUX := CASE NUX
        WHEN 1 THEN 1
        ELSE 0
    END;
END;
/
