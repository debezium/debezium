DECLARE
    NUX NUMBER := 1;
    GRADE VARCHAR2(10) := 'A';
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

-- Simple case statement with multiple selector values and dangling predicates
    CASE NUX
        WHEN < 0, 0.5, > 100 THEN
            DBMS_OUTPUT.PUT_LINE('Error');
        WHEN 40, 41, 42 THEN
            DBMS_OUTPUT.PUT_LINE('Optimal');
        WHEN BETWEEN 10 AND 30 THEN
            DBMS_OUTPUT.PUT_LINE('Good');
        WHEN NOT IN (1, 2, 3) THEN
            DBMS_OUTPUT.PUT_LINE('Other');
        WHEN IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('Unknown');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Something went wrong!');
    END;

-- Simple case statement with LIKE dangling predicates
    CASE GRADE
        WHEN LIKE 'A%' THEN
            DBMS_OUTPUT.PUT_LINE('Excellent');
        WHEN NOT LIKE 'B%' THEN
            DBMS_OUTPUT.PUT_LINE('Not a B');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Other');
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

-- Simple case expression with multiple selector values and dangling predicates
    NUX := CASE NUX
        WHEN < 0, 0.5, > 100 THEN 1
        WHEN 40, 41, 42 THEN 2
        WHEN BETWEEN 10 AND 30 THEN 3
        WHEN NOT IN (1, 2, 3) THEN 4
        WHEN IS NULL THEN 5
        ELSE 0
    END;
END;
/
