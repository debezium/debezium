DECLARE
  TYPE num_list IS TABLE OF NUMBER;
  departments num_list := num_list(45, 78, 91);
BEGIN
  FORALL j IN departments.FIRST..departments.LAST
    DELETE FROM employees WHERE department_id = departments(j);

  FOR i IN departments.FIRST..departments.LAST LOOP
    DBMS_OUTPUT.PUT_LINE ( 'Statement #' || i || ' deleted ' || SQL%BULK_ROWCOUNT(i) || ' rows.' );
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Total rows deleted: ' || SQL%ROWCOUNT);
END;
/
